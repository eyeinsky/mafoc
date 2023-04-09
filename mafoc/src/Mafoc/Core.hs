{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TypeFamilyDependencies #-}
module Mafoc.Core
  ( module Mafoc.Core
  , module Mafoc.Common
  ) where

import Control.Monad.Trans.Class (lift)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Text qualified as TS
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import Data.Word (Word32)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Streaming qualified as S
import Streaming.Prelude qualified as S
import System.FilePath ((</>))

import Cardano.Api qualified as C
-- import Cardano.BM.Data.Trace
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace qualified as Trace
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Mafoc.RollbackRingBuffer qualified as RB
import Marconi.ChainIndex.Indexers.MintBurn ()
import Marconi.ChainIndex.Logging qualified as Marconi
import Prettyprinter (Pretty (pretty), defaultLayoutOptions, layoutPretty)
import Prettyprinter.Render.Text (renderStrict)

import Mafoc.Common (SlotNoBhh, blockChainPoint, blockSlotNo, blockSlotNoBhh, chainPointSlotNo, foldYield,
                     getSecurityParamAndNetworkId, tipDistance)

-- * Interval

data UpTo
  = SlotNo C.SlotNo
  | Infinity
  | CurrentTip
  deriving (Show)

data Interval = Interval
  { from :: C.ChainPoint
  , upTo :: UpTo
  } deriving (Show)

chainPointLaterThanFrom :: C.ChainPoint -> Interval -> Bool
chainPointLaterThanFrom cp (Interval from' _) = from' <= cp

takeUpTo
  :: Trace.Trace IO TS.Text
  -> UpTo
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) IO ()
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) IO ()
takeUpTo trace upTo' source = case upTo' of
  SlotNo slotNo -> do
    lift $ traceDebug trace $ "Will index up to " <> show slotNo
    flip S.takeWhile source $ \case
      CS.RollForward blk _ -> blockSlotNo blk <= slotNo
      CS.RollBackward{}    -> True
  Infinity -> source
  CurrentTip -> S.lift (S.next source) >>= \case
    Left r -> return r
    Right (event, source') -> do
      let tip = getTipPoint event :: C.ChainPoint
      lift $ traceDebug trace $ "Will index up to current tip, which is: " <> show tip
      S.yield event -- We can always yield the current event, as that
                    -- is the source for the upper bound anyway.
      flip S.takeWhile source' $ \case
        CS.RollForward blk _ct -> blockChainPoint blk <= tip
        CS.RollBackward{}      -> True -- We skip rollbacks as these can ever only to an earlier point
      lift $ traceInfo trace $ "Reached current tip as of when indexing started (this was: " <> show (getTipPoint event) <> ")"

  where
    getTipPoint :: CS.ChainSyncEvent a -> C.ChainPoint
    getTipPoint = \case
      CS.RollForward _blk ct -> C.chainTipToChainPoint ct
      CS.RollBackward _cp ct -> C.chainTipToChainPoint ct

-- * Sqlite

-- ** Checkpoint

sqliteInitCheckpoints :: SQL.Connection -> IO ()
sqliteInitCheckpoints sqlCon = do
  SQL.execute_ sqlCon
    " CREATE TABLE IF NOT EXISTS checkpoints \
    \   ( indexer TEXT NOT NULL              \
    \   , slot_no INT NOT NULL               \
    \   , block_header_hash BLOB NOT NULL    \
    \   , PRIMARY KEY (indexer))             "


setCheckpointSqlite :: SQL.Connection -> String -> (C.SlotNo, C.Hash C.BlockHeader) -> IO ()
setCheckpointSqlite c name (slotNo, bhh) = SQL.execute c
  "INSERT OR REPLACE INTO checkpoints (indexer, slot_no, block_header_hash) values (?, ?, ?)"
  (name, slotNo, bhh)

-- | Get checkpoint (the place where we left off) for an indexer with @name@
getCheckpointSqlite :: SQL.Connection -> String -> IO (Maybe C.ChainPoint)
getCheckpointSqlite sqlCon name = do
  list <- SQL.query sqlCon "SELECT slot_no, block_header_hash FROM checkpoints WHERE indexer = ?" (SQL.Only name)
  case list of
    [(slotNo, bhh)] -> return $ Just $ C.ChainPoint slotNo bhh
    []              -> return Nothing
    _               -> error "getCheckpointSqlite: this should never happen!!"

-- ** Database path and table(s)

data DbPathAndTableName = DbPathAndTableName (Maybe FilePath) (Maybe String)
  deriving (Show)

defaultTableName :: String -> DbPathAndTableName -> (FilePath, String)
defaultTableName defaultName (DbPathAndTableName maybeDbPath maybeName) = (fromMaybe "default.db" maybeDbPath, fromMaybe defaultName maybeName)

-- * Streaming

-- * Indexer class

class Indexer a where

  -- | The @a@ itself doubles as cli configuration, no need for the following:
  -- type Config a = r | r -> a

  -- | Runtime configuration.
  data Runtime a

  -- | Event type.
  type Event a

  -- | The fold state. For map type indexers where no fold state needs
  -- to be maintained, the state is some form of empty (i.e defined to
  -- a data type with no fields).
  data State a

  -- | Fold a block into an event and produce a new state.
  toEvent :: Runtime a -> State a -> C.BlockInMode C.CardanoMode -> IO (State a, Maybe (Event a))

  -- | Initialize an indexer and return its runtime configuration. E.g
  -- open the destination to where data is persisted, etc.
  initialize :: a -> Trace.Trace IO TS.Text ->  IO (State a, LocalChainsyncRuntime, Runtime a)

  -- | Write event to persistent storage.
  persist :: Runtime a -> Event a -> IO ()
  persist runtime event = persistMany runtime [event]

  -- | Persist many events at a time, defaults to mapping over events with persist.
  persistMany :: Runtime a -> [Event a] -> IO ()
  persistMany runtime events = mapM_ (persist runtime) events

  -- | Set a checkpoint of lates ChainPoint processed. This is used when
  -- there are no events to be persisted, but sufficient amount of
  -- time has passed.
  checkpoint :: Runtime a -> (C.SlotNo, C.Hash C.BlockHeader) -> IO ()

-- * Local chainsync config

type NodeFolder = FilePath
type NodeConfig = FilePath
type SocketPath = FilePath
type SecurityParamOrNodeConfig = Either (Natural, C.NetworkId) NodeConfig
type NodeFolderOrSecurityParamOrNodeConfig = Either NodeFolder (SocketPath, SecurityParamOrNodeConfig)

-- | Configuration for local chainsync streaming setup.
data LocalChainsyncConfig = LocalChainsyncConfig
  { nodeFolderOrSecurityParamOrNodeConfig :: NodeFolderOrSecurityParamOrNodeConfig
  , interval_                             :: Interval
  , logging_                              :: Bool
  , pipelineSize_                         :: Word32
  , batchSize_                            :: Natural
  } deriving Show

-- | Resolve @LocalChainsyncConfig@ that came from e.g command line
-- arguments into an "actionable" @LocalChainsyncRuntime@ runtime
-- config which can be used to generate a stream of blocks.
initializeLocalChainsync :: LocalChainsyncConfig -> IO LocalChainsyncRuntime
initializeLocalChainsync config = do
  let interval' = interval_ config
  (socketPath, k, networkId'') <- case nodeFolderOrSecurityParamOrNodeConfig config of
    Left nodeFolder -> let
      nodeConfig = nodeFolder </> "config" </> "config.json"
      socketPath = nodeFolder </> "socket" </> "node.socket"
      in do
      (k, networkId') <- getSecurityParamAndNetworkId nodeConfig
      pure (socketPath, k, networkId')
    Right (socketPath, securityParamOrNodeConfig) -> do
      (k, networkId') <- either pure getSecurityParamAndNetworkId securityParamOrNodeConfig
      pure (socketPath, k, networkId')
  let localNodeCon = CS.mkLocalNodeConnectInfo networkId'' socketPath
  return $ LocalChainsyncRuntime localNodeCon interval' k (logging_ config) (pipelineSize_ config) (batchSize_ config)

-- | Initialize sqlite: create connection, run init (e.g create
-- destination table), create checkpoints database if doesn't exist,
-- update interval in runtime config by whether there is anywhere to
-- resume from.
initializeSqlite
  :: FilePath -> String -> (SQL.Connection -> String -> IO ()) -> LocalChainsyncRuntime -> Trace.Trace IO TS.Text -> IO (SQL.Connection, LocalChainsyncRuntime)
initializeSqlite dbPath tableName sqliteInit chainsyncRuntime trace = do
  sqlCon <- SQL.open dbPath
  SQL.execute_ sqlCon "PRAGMA journal_mode=WAL"
  sqliteInit sqlCon tableName
  sqliteInitCheckpoints sqlCon

  -- Find ChainPoint from checkpoints table and update chainsync
  -- runtime configuration if that is later than what was passed in
  -- from cli configuration.
  let cliInterval = interval chainsyncRuntime
  maybeChainPoint <- getCheckpointSqlite sqlCon tableName
  newInterval <- case maybeChainPoint of
    Just cp -> if chainPointLaterThanFrom cp cliInterval
      then do
      traceInfo trace $ "Found checkpoint that is later than CLI argument from checkpoints table: " <> show cp
      return $ cliInterval { from = cp }
      else do
      traceInfo trace $ "Found checkpoint that is not later than CLI argument, checkpoint: " <> show cp <> ", cli: " <> show (from cliInterval)
      return cliInterval
    _ -> do
      traceInfo trace $ "No checkpoint found, going with CLI starting point: " <> show (from cliInterval)
      return cliInterval
  let chainsyncRuntime' = chainsyncRuntime { interval = newInterval }

  return (sqlCon, chainsyncRuntime')

-- | Static configuration for block source
data LocalChainsyncRuntime = LocalChainsyncRuntime
  { localNodeConnection :: C.LocalNodeConnectInfo C.CardanoMode
  , interval            :: Interval
  , securityParam       :: Natural
  , logging             :: Bool
  , pipelineSize        :: Word32
  , batchSize           :: Natural
  }

blockSource :: LocalChainsyncRuntime -> Trace.Trace IO TS.Text -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSource cc trace = blocks' (localNodeConnection cc) from'
  & (if logging cc then Marconi.logging trace else id)
  & takeUpTo trace upTo'
  & S.drop 1 -- The very first event from local chainsync is always a
             -- rewind. We skip this because we don't have anywhere to
             -- rollback to anyway.
  & RB.rollbackRingBuffer (securityParam cc)
       tipDistance
       blockSlotNoBhh
  where
    Interval from' upTo' = interval cc
    blocks'
      :: C.LocalNodeConnectInfo C.CardanoMode
      -> C.ChainPoint
      -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
    blocks' = let
      pipelineSize' = pipelineSize cc
      in if pipelineSize' > 1
      then CS.blocks
      else CS.blocksPipelined pipelineSize'

runIndexer :: forall a . (Indexer a, Show a) => a -> IO ()
runIndexer cli = do
  c <- defaultConfigStdout
  withTrace c "mafoc" $ \trace -> do
    traceInfo trace $ "Indexer started with configuration: " <> show cli
    (initialState, localChainsyncRuntime, indexerRuntime) <- initialize cli trace
    S.effects
      -- Start streaming blocks over local chainsync
      $ blockSource localChainsyncRuntime trace
      -- Pick out ChainPoint
      & S.map (\blk -> (blockSlotNoBhh blk, blk))
      -- Fold over stream of blocks with state, emit indexer events as
      -- `Maybe event` (because not all blocks generate an event)
      & foldYield (\st (cp, a) -> do
                      (st', b) <- toEvent indexerRuntime st a
                      return (st', (cp, b))
                  ) initialState

      -- Persist events with `persist` or `persistMany` (buffering writes by
      -- batchSize in the latter case)
      & buffered indexerRuntime trace (batchSize localChainsyncRuntime)

  where
    buffered
      :: Runtime a -> Trace.Trace IO TS.Text -> Natural
      -> S.Stream (S.Of (SlotNoBhh, Maybe (Event a))) IO ()
      -> S.Stream (S.Of (SlotNoBhh, Maybe (Event a))) IO ()
    buffered indexerRuntime' trace bufferSize source = do

      let initialState :: UTCTime -> (Natural, UTCTime, [Event a])
          initialState t = (0, t, [])

          step :: (Natural, UTCTime, [Event a]) -> (SlotNoBhh, Maybe (Event a)) -> IO ((Natural, UTCTime, [Event a]), (SlotNoBhh, Maybe (Event a)))
          step state@(n, lastCheckpointTime, xs) t@(slotNoBhh, maybeEvent) = do
            now :: UTCTime <- getCurrentTime
            let isCheckpointTime = diffUTCTime now lastCheckpointTime > 10
                -- Write checkpoint and return state with last checkpoint time set to `now`
                writeCheckpoint = do
                  checkpoint indexerRuntime' slotNoBhh
                  traceInfo trace $ "Checkpointing at " <> show slotNoBhh
                  return $ initialState now
            state' <- case maybeEvent of
              -- There is an event
              Just x -> let
                succN = succ n
                bufferFull = succN == bufferSize
                in if bufferFull || isCheckpointTime
                -- If buffer is full or if it's checkpoint time, we flush the buffer and checkpoint
                then do
                traceDebug trace $ "Checkpointing because " <> if
                  | bufferFull       -> "buffer full"
                  | isCheckpointTime -> "it's checkpoint time"
                  | otherwise        -> error "Checkpointing because: this should never happen !!"
                persistMany indexerRuntime' (reverse $ x : xs)
                writeCheckpoint
                -- Buffer is not full, let's add event to buffer
                else return (succN, lastCheckpointTime, x : xs)
              -- It's time to checkpoint, let's persist events and checkpoint.
              _ | isCheckpointTime -> do
                traceDebug trace "Checkpointing because it's checkpoint time (but buffer is not full)"
                persistMany indexerRuntime' (reverse xs) -- This is a no-op when `xs` is empty
                writeCheckpoint
              -- It's not checkpoint time and there is no event, pass buffer state unchanged.
                | otherwise -> return state

            return (state', t)

      currentTime :: UTCTime <- lift $ getCurrentTime
      foldYield step (initialState currentTime) source

traceInfo :: Trace.Trace IO TS.Text -> String -> IO ()
traceInfo trace msg = Trace.logInfo trace $ renderStrict $ layoutPretty defaultLayoutOptions $ pretty msg

traceDebug :: Trace.Trace IO TS.Text -> String -> IO ()
traceDebug trace msg = Trace.logDebug trace $ renderStrict $ layoutPretty defaultLayoutOptions $ pretty msg
