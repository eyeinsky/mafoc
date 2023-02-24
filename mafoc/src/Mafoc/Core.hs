{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Core where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Function ((&))
import Data.Functor (($>))
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
import Cardano.BM.Data.Trace (Trace)
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers (envNetworkId)
import Mafoc.RollbackRingBuffer qualified as RB
import Marconi.ChainIndex.Indexers.MintBurn ()
import Marconi.ChainIndex.Logging qualified as Marconi

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
  :: UpTo
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (cp, C.ChainTip))) IO ()
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (cp, C.ChainTip))) IO ()
takeUpTo upTo' source = case upTo' of
  SlotNo slotNo -> flip S.takeWhile source $ \case
    RB.RollForward blk _ -> blockSlotNo blk <= slotNo
    _                    -> True
  Infinity -> source
  CurrentTip -> S.lift (S.next source) >>= \case
    Left r -> return r
    Right (event, source') -> do
      lift $ putStrLn $ "Indexing up to current tip, which is: " <> show (getTipPoint event) -- TODO replace me with a better logger
      S.yield event -- We can always yield the current event, as that
                    -- is the source for the upper bound anyway.
      flip S.takeWhile source' $ \case
        RB.RollForward blk _ -> blockChainPoint blk <= getTipPoint event
        _                    -> True

  where
    getTipPoint :: RB.Event a (b, C.ChainTip) -> C.ChainPoint
    getTipPoint = \case
      RB.RollForward _ (_, ct) -> C.chainTipToChainPoint ct
      RB.RollBackward (_, ct)  -> C.chainTipToChainPoint ct

-- * Additions to cardano-api

getSecurityParamAndNetworkId :: FilePath -> IO (Natural, C.NetworkId)
getSecurityParamAndNetworkId nodeConfig = do
  (env :: C.Env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure (fromIntegral $ C.envSecurityParam env, envNetworkId env)

-- | Convert event from @ChainSyncEvent@ to @Event@.
fromChainSyncEvent
  :: Monad m
  => S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) m r
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (C.ChainPoint, C.ChainTip))) m r
fromChainSyncEvent = S.map $ \e -> case e of
  CS.RollForward a ct   -> RB.RollForward a (blockChainPoint a, ct)
  CS.RollBackward cp ct -> RB.RollBackward (cp, ct)

instance Ord C.ChainTip where
  compare C.ChainTipAtGenesis C.ChainTipAtGenesis = EQ
  compare C.ChainTipAtGenesis _                   = LT
  compare _ C.ChainTipAtGenesis                   = GT
  compare (C.ChainTip a _ _) (C.ChainTip b _ _)   = compare a b

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

blockSlotNoBhh :: C.BlockInMode mode -> SlotNoBhh
blockSlotNoBhh (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = (slotNo, hash)

blockSlotNo :: C.BlockInMode mode -> C.SlotNo
blockSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) _) _) = slotNo

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

-- | Consume a stream @source@ in a loop and run effect @f@ on it.
loopM :: (MonadTrans t1, Monad m, Monad (t1 m)) => S.Stream (S.Of t2) m b -> (t2 -> t1 m a) -> t1 m b
loopM source f = loop source
  where
    loop source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (event, source'') -> do
        _ <- f event
        loop source''

-- | Fold a stream of @a@s yield a stream of @b@s while keeping a state of @st".
foldYield :: Monad m => (st -> a -> m (st, b)) -> st -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r
foldYield f st_ source_ = loop st_ source_
  where
    loop st source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        (st', e') <- lift $ f st e
        S.yield e'
        loop st' source'

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
  initialize :: a -> IO (State a, LocalChainsyncRuntime, Runtime a)

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
  , chunkSize_                            :: Natural
  } deriving Show

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
  return $ LocalChainsyncRuntime localNodeCon interval' k (logging_ config) (pipelineSize_ config) (chunkSize_ config)

-- | Initialize sqlite: create connection, run init (e.g create
-- destination table), create checkpoints database if doesn't exist,
-- update interval in runtime config by whether there is anywhere to
-- resume from.
initializeSqlite
  :: FilePath -> String -> (SQL.Connection -> String -> IO ()) -> LocalChainsyncRuntime -> IO (SQL.Connection, LocalChainsyncRuntime)
initializeSqlite dbPath tableName sqliteInit chainsyncRuntime = do
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
      putStrLn $ "Found checkpoint that is later than cli argument from checkpoints table: " <> show cp
      return $ cliInterval { from = cp }
      else do
      putStrLn $ "Found checkpoint that is not later than cli argument, checkpoint: " <> show cp <> ", cli: " <> show (from cliInterval)
      return cliInterval
    _ -> do
      putStrLn $ "No checkpoint found, going with cli starting point: " <> show (from cliInterval)
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
  , chunkSize           :: Natural
  }

blockSource :: LocalChainsyncRuntime -> Trace IO TS.Text -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSource cc trace = blocks' (localNodeConnection cc) from'
  & (if logging cc then Marconi.logging trace else id)
  & fromChainSyncEvent
  & takeUpTo upTo'
  & S.drop 1 -- The very first event from local chainsync is always a
             -- rewind. We skip this because we don't have anywhere to
             -- rollback to anyway.
  & RB.rollbackRingBuffer (securityParam cc)
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

runIndexer :: forall a . Indexer a => a -> IO ()
runIndexer cli = do
  (initialState, localChainsyncRuntime, indexerRuntime) <- initialize cli
  c <- defaultConfigStdout
  withTrace c "mafoc" $ \trace -> S.effects
    -- Start streaming blocks over local chainsync
    $ (blockSource localChainsyncRuntime trace :: Stream Block)
    -- Pick out ChainPoint
    & (S.map topLevelChainPoint :: Stream Block -> Stream (SlotNoBhh, Block))
    -- Fold over stream of blocks with state, emit events as `Maybe event`
    & foldYield (\st (cp, a) -> do
                    (st', b) <- toEvent indexerRuntime st a
                    return (st', (cp, b))
                ) initialState

    -- Persist events with `persist` or `persistMany` (buffering writes by
    -- chunkSize in the latter case)
    & buffered indexerRuntime (chunkSize localChainsyncRuntime)

  where
    topLevelChainPoint :: Block -> (SlotNoBhh, Block)
    topLevelChainPoint b = (blockSlotNoBhh b, b)

    buffered
      :: Runtime a -> Natural
      -> Stream (SlotNoBhh, Maybe (Event a)) -> Stream (SlotNoBhh, Maybe (Event a))
    buffered indexerRuntime' bufferSize source = do

      let initialState :: UTCTime -> (Natural, UTCTime, [Event a])
          initialState t = (0, t, [])

          step :: (Natural, UTCTime, [Event a]) -> (SlotNoBhh, Maybe (Event a)) -> IO ((Natural, UTCTime, [Event a]), (SlotNoBhh, Maybe (Event a)))
          step state@(n, lastCheckpointTime, xs) t@(slotNoBhh, maybeEvent) = do
            now :: UTCTime <- getCurrentTime
            let isCheckpointTime = diffUTCTime now lastCheckpointTime > 10
                -- Write checkpoint and return state with last checkpoint time set to `now`
                writeCheckpoint = checkpoint indexerRuntime' slotNoBhh $> initialState now
            state' <- case maybeEvent of
              -- There is an event
              Just x -> let succN = succ n
                in if succN == bufferSize || isCheckpointTime
                -- If buffer is full or if it's checkpoint time, we flush the buffer and checkpoint
                then do
                putStrLn $ "Checkpointing by flushing full buffer (buffer size " <> show bufferSize <> ")"
                persistMany indexerRuntime' (reverse $ x : xs)
                writeCheckpoint
                -- Buffer is not full, let's add event to buffer
                else return (succN, lastCheckpointTime, x : xs)
              -- It's time to checkpoint, let's persist events and checkpoint.
              _ | isCheckpointTime -> do
                putStrLn $ "Checkpointing by adding a row to checkpoints table: " <> show slotNoBhh
                persistMany indexerRuntime' (reverse xs) -- This is a no-op when `xs` is empty
                writeCheckpoint
              -- It's not checkpoint time and there is no event, pass buffer state unchanged.
                | otherwise -> return state

            return (state', t)

      currentTime :: UTCTime <- lift $ getCurrentTime
      foldYield step (initialState currentTime) source

type Block = C.BlockInMode C.CardanoMode
type Stream e = S.Stream (S.Of e) IO ()
type SlotNoBhh = (C.SlotNo, C.Hash C.BlockHeader)
