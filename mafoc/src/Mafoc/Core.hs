{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DerivingStrategies     #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE OverloadedLabels       #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE StarIsType             #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilyDependencies #-}
module Mafoc.Core
  ( module Mafoc.Core
  , module Mafoc.Upstream
  ) where

import Control.Concurrent qualified as IO
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TChan qualified as TChan
import Control.Monad.Trans.Class (lift)
import Data.ByteString.Char8 qualified as C8
import Data.Coerce (coerce)
import Data.Function (on, (&))
import Data.List qualified as L
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Proxy (Proxy (Proxy))
import Data.String (IsString)
import Data.Text qualified as TS
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import Data.Word (Word32, Word64)
import Database.SQLite.Simple qualified as SQL
import GHC.OverloadedLabels (IsLabel (fromLabel))
import Numeric.Natural (Natural)
import Prettyprinter (Pretty (pretty), defaultLayoutOptions, layoutPretty)
import Prettyprinter.Render.Text (renderStrict)
import Streaming qualified as S
import Streaming.Prelude qualified as S
import System.Directory (listDirectory, removeFile)
import System.FilePath ((</>))
import Text.Read qualified as Read

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace qualified as Trace
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi
import Marconi.ChainIndex.Indexers.MintBurn ()
import Marconi.ChainIndex.Types qualified as Marconi
import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.RollbackRingBuffer qualified as RB
import Mafoc.Logging qualified as Logging
import Mafoc.Upstream (SlotNoBhh, blockChainPoint, blockSlotNo, blockSlotNoBhh, chainPointSlotNo, foldYield,
                       getNetworkId, getSecurityParamAndNetworkId, querySecurityParam, tipDistance)

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
  checkpoint :: Runtime a -> State a -> (C.SlotNo, C.Hash C.BlockHeader) -> IO ()


runIndexer :: forall a . (Indexer a, Show a) => a -> IO ()
runIndexer cli = do
  c <- defaultConfigStdout
  withTrace c "mafoc" $ \trace -> do
    traceInfo trace $ "Indexer started with configuration: " <> show cli
    (initialState, lcr, indexerRuntime) <- initialize cli trace
    S.effects
      -- Start streaming blocks over local chainsync
      $ blockSource
          (securityParam lcr)
          (localNodeConnection lcr)
          (interval lcr)
          (pipelineSize lcr)
          (concurrencyPrimitive lcr)
          (logging lcr)
          trace
      -- Pick out ChainPoint
      & S.map (\blk -> (blockSlotNoBhh blk, blk))
      -- Fold over stream of blocks with state, emit indexer events as
      -- `Maybe event` (because not all blocks generate an event)
      & foldYield (\st (cp, a) -> do
                      (st', b) <- toEvent indexerRuntime st a
                      return (st', (cp, b, st'))
                  ) initialState

      -- Persist events with `persist` or `persistMany` (buffering writes by
      -- batchSize in the latter case)
      & buffered indexerRuntime trace (batchSize lcr)

  where
    buffered
      :: Runtime a -> Trace.Trace IO TS.Text -> Natural
      -> S.Stream (S.Of (SlotNoBhh, Maybe (Event a), State a)) IO ()
      -> S.Stream (S.Of (SlotNoBhh, Maybe (Event a), State a)) IO ()
    buffered indexerRuntime' trace bufferSize source = do

      let initialState :: UTCTime -> (Natural, UTCTime, [Event a])
          initialState t = (0, t, [])

          step :: (Natural, UTCTime, [Event a]) -> (SlotNoBhh, Maybe (Event a), State a) -> IO ((Natural, UTCTime, [Event a]), (SlotNoBhh, Maybe (Event a), State a))
          step state@(n, lastCheckpointTime, xs) t@(slotNoBhh, maybeEvent, indexerState) = do
            now :: UTCTime <- getCurrentTime
            let isCheckpointTime = diffUTCTime now lastCheckpointTime > 10
                -- Write checkpoint and return state with last checkpoint time set to `now`
                writeCheckpoint = do
                  checkpoint indexerRuntime' indexerState slotNoBhh
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

-- * Local chainsync

newtype NodeFolder = NodeFolder FilePath
  deriving newtype (Show, IsString)
newtype NodeConfig = NodeConfig FilePath
  deriving newtype (Show, IsString)
newtype SocketPath = SocketPath FilePath
  deriving newtype (Show, IsString)
newtype NodeInfo a = NodeInfo (Either NodeFolder (SocketPath, a))
  deriving Show

-- | Configuration for local chainsync streaming setup.
data LocalChainsyncConfig a = LocalChainsyncConfig
  { nodeInfo              :: NodeInfo a
  , interval_             :: Interval
  , logging_              :: Bool
  , pipelineSize_         :: Word32
  , batchSize_            :: Natural
  , concurrencyPrimitive_ :: Maybe ConcurrencyPrimitive
  } deriving Show

type LocalChainsyncConfig_ = LocalChainsyncConfig (Either C.NetworkId NodeConfig)

-- ** Get and derive stuff from LocalChainsyncConfig

instance IsLabel "nodeConfig" (NodeFolder -> NodeConfig) where
  fromLabel (NodeFolder nodeFolder) = NodeConfig (mkPath nodeFolder)
    where
      mkPath :: FilePath -> FilePath
      mkPath nodeFolder' = nodeFolder' </> "config" </> "config.json"
instance IsLabel "nodeConfig" (LocalChainsyncConfig NodeConfig -> NodeConfig) where
  fromLabel = #nodeConfig . nodeInfo
instance IsLabel "nodeConfig" (NodeInfo NodeConfig -> NodeConfig) where
  fromLabel = either #nodeConfig snd . (\(NodeInfo e) -> e)

instance IsLabel "socketPath" (LocalChainsyncConfig a -> SocketPath) where
  fromLabel = #socketPath . nodeInfo
instance IsLabel "socketPath" (NodeInfo a -> SocketPath) where
  fromLabel = either (coerce mkPath) fst . (\(NodeInfo e) -> e)
    where
      mkPath :: FilePath -> FilePath
      mkPath nodeFolder' = nodeFolder' </> "socket" </> "node.socket"

instance IsLabel "getNetworkId" (NodeConfig -> IO C.NetworkId) where
  fromLabel (NodeConfig nodeConfig') = getNetworkId nodeConfig'
instance IsLabel "getNetworkId" (NodeInfo NodeConfig -> IO C.NetworkId) where
  fromLabel (NodeInfo nodeInfo') = case nodeInfo' of
      Left nodeFolder                 -> #getNetworkId (#nodeConfig nodeFolder :: NodeConfig)
      Right (_socketPath, nodeConfig) -> #getNetworkId nodeConfig
instance IsLabel "getNetworkId" (LocalChainsyncConfig_ -> IO C.NetworkId) where
  fromLabel lcc = let
    NodeInfo nodeInfo' = nodeInfo lcc
    in case nodeInfo' of
      Left nodeFolder -> #getNetworkId (#nodeConfig nodeFolder :: NodeConfig)
      Right (_socketPath, networkIdOrNodeConfig) -> do
        either pure (getNetworkId . coerce) networkIdOrNodeConfig

-- * LocalChainsyncRuntime

-- | Static configuration for block source
data LocalChainsyncRuntime = LocalChainsyncRuntime
  { localNodeConnection  :: C.LocalNodeConnectInfo C.CardanoMode
  , interval             :: Interval
  , securityParam        :: Marconi.SecurityParam
  , logging              :: Bool
  , pipelineSize         :: Word32
  , batchSize            :: Natural

  -- * Internal
  , concurrencyPrimitive :: Maybe ConcurrencyPrimitive
  }

initializeLocalChainsync :: LocalChainsyncConfig a -> C.NetworkId -> IO LocalChainsyncRuntime
initializeLocalChainsync config networkId = do
  let SocketPath socketPath' = #socketPath config
  let localNodeCon = CS.mkLocalNodeConnectInfo networkId socketPath'
  securityParam' <- querySecurityParam localNodeCon
  return $ LocalChainsyncRuntime
    localNodeCon
    (interval_ config)
    securityParam'
    (logging_ config)
    (pipelineSize_ config)
    (batchSize_ config)
    (concurrencyPrimitive_ config)

-- | Resolve @LocalChainsyncConfig@ that came from e.g command line
-- arguments into an "actionable" @LocalChainsyncRuntime@ runtime
-- config which can be used to generate a stream of blocks.
initializeLocalChainsync_ :: LocalChainsyncConfig_ -> IO LocalChainsyncRuntime
initializeLocalChainsync_ config = do
  networkId <- #getNetworkId config
  initializeLocalChainsync config networkId

blockSourceFromLocalChainsyncRuntime
  :: LocalChainsyncRuntime
  -> Trace.Trace IO TS.Text
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSourceFromLocalChainsyncRuntime lcr trace = blockSource
  (securityParam lcr)
  (localNodeConnection lcr)
  (interval lcr)
  (pipelineSize lcr)
  (concurrencyPrimitive lcr)
  (logging lcr)
  trace

blockSource
  :: Marconi.SecurityParam
  -> C.LocalNodeConnectInfo C.CardanoMode
  -> Interval
  -> Word32
  -> Maybe ConcurrencyPrimitive
  -> Bool
  -> Trace.Trace IO TS.Text
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSource securityParam' lnc interval' pipelineSize' concurrencyPrimitive' logging' trace = blocks'
  & (if logging' then Logging.logging trace else id)
  & takeUpTo trace upTo'
  & S.drop 1 -- The very first event from local chainsync is always a
             -- rewind. We skip this because we don't have anywhere to
             -- rollback to anyway.
  & RB.rollbackRingBuffer (fromIntegral securityParam')
       tipDistance
       blockSlotNoBhh
  where
    Interval fromCp upTo' = interval'
    blocks' :: S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
    blocks' = if pipelineSize' > 1
      then CS.blocksPipelined pipelineSize' lnc fromCp
      else case concurrencyPrimitive' of
      Just a -> do
        let msg = "Using " <> show a <> " as concurrency variable to pass blocks"
        lift $ traceInfo trace msg
        case a of
          MVar -> CS.blocksPrim IO.newEmptyMVar IO.putMVar IO.takeMVar lnc fromCp
          Chan -> CS.blocksPrim IO.newChan IO.writeChan IO.readChan lnc fromCp
          TChan -> CS.blocksPrim
            TChan.newTChanIO
            (\chan e -> STM.atomically $ STM.writeTChan chan e)
            (STM.atomically . STM.readTChan)
            lnc fromCp
      Nothing -> CS.blocksPrim IO.newChan IO.writeChan IO.readChan lnc fromCp

-- | This is a very internal data type to help swap the concurrency
-- primitive used to pass blocks from the local chainsync's green thread
-- to the indexer.
data ConcurrencyPrimitive
  = MVar
  | Chan
  | TChan
  deriving (Show, Read, Enum, Bounded)

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


-- * Trace

traceInfo :: Trace.Trace IO TS.Text -> String -> IO ()
traceInfo trace msg = Trace.logInfo trace $ renderStrict $ layoutPretty defaultLayoutOptions $ pretty msg

traceDebug :: Trace.Trace IO TS.Text -> String -> IO ()
traceDebug trace msg = Trace.logDebug trace $ renderStrict $ layoutPretty defaultLayoutOptions $ pretty msg

-- * Ledger state checkpoint

listExtLedgerStates :: FilePath -> IO [(FilePath, SlotNoBhh)]
listExtLedgerStates dirPath = L.sortBy (flip compare `on` snd) . mapMaybe parse <$> listDirectory dirPath
  where
    parse :: FilePath -> Maybe (FilePath, SlotNoBhh)
    parse fn = either (const Nothing) Just . fmap (fn,) $ bhhFromFileName fn

-- | Load ledger state from file, while taking the chain point it's at from the file name.
loadLedgerStateWithChainpoint :: NodeConfig -> Trace.Trace IO TS.Text -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_, C.ChainPoint)
loadLedgerStateWithChainpoint nodeConfig@(NodeConfig nodeConfig') trace = listExtLedgerStates "." >>= \case
  -- A ledger state exists on disk, resume from there
  (fn, (slotNo, bhh)) : _ -> do
    (cfg, extLedgerState) <- loadLedgerState nodeConfig fn
    let cp = C.ChainPoint slotNo bhh
    traceInfo trace $ "Found on-disk ledger state, resuming from: " <> show cp
    return (cfg, extLedgerState, cp)
  -- No existing ledger states, start from the beginning
  [] -> do
    (cfg, st) <- Marconi.getInitialExtLedgerState nodeConfig'
    traceInfo trace "No on-disk ledger state found, resuming from genesis"
    return (cfg, st, C.ChainPointAtGenesis)

loadLedgerState :: NodeConfig -> FilePath -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
loadLedgerState (NodeConfig nodeConfig) ledgerStatePath = do
  cfg <- Marconi.getLedgerConfig nodeConfig
  let O.ExtLedgerCfg topLevelConfig = cfg
  extLedgerState <- Marconi.loadExtLedgerState (O.configCodec topLevelConfig) ledgerStatePath >>= \case
    Right (_, extLedgerState) -> return extLedgerState
    Left msg                  -> error $ "Error while deserialising file " <> ledgerStatePath <> ", error: " <> show msg
  return (cfg, extLedgerState)

storeLedgerState :: Marconi.ExtLedgerCfg_ -> SlotNoBhh -> Marconi.ExtLedgerState_ -> IO ()
storeLedgerState ledgerCfg slotNoBhh extLedgerState = do
  let O.ExtLedgerCfg topLevelConfig = ledgerCfg
  putStrLn $ "Write ledger state"
  Marconi.writeExtLedgerState (bhhToFileName slotNoBhh) (O.configCodec topLevelConfig) extLedgerState
  putStrLn $ "Wrote ledger state"
  mapM_ (removeFile . bhhToFileName) . drop 2 . map snd =<< listExtLedgerStates "."
  putStrLn $ "Removed other files"

-- | Initialization for ledger state indexers
initializeLedgerStateAndDatabase
  :: LocalChainsyncConfig NodeConfig
  -> Trace.Trace IO TS.Text
  -> DbPathAndTableName                  -- ^ Path to sqlite db and table name from cli
  -> (SQL.Connection -> String -> IO ()) -- ^ Function which takes a connection and a table name and creates the table.
  -> String
  -> IO ( Marconi.ExtLedgerState_, Maybe C.EpochNo
        , LocalChainsyncRuntime
        , SQL.Connection, String, Marconi.ExtLedgerCfg_)
initializeLedgerStateAndDatabase chainsyncConfig trace dbPathAndTableName sqliteInit defaultTableName' = do
  let nodeConfig = #nodeConfig chainsyncConfig
  let (SocketPath socketPath') = #socketPath chainsyncConfig

  networkId <- #getNetworkId nodeConfig
  let localNodeConnectInfo = CS.mkLocalNodeConnectInfo networkId socketPath'
  securityParam' <- querySecurityParam localNodeConnectInfo

  let (dbPath, tableName) = defaultTableName defaultTableName' dbPathAndTableName
  sqlCon <- sqliteOpen dbPath
  sqliteInit sqlCon tableName

  (ledgerConfig, extLedgerState, startFrom) <- loadLedgerStateWithChainpoint nodeConfig trace

  let chainsyncRuntime' = LocalChainsyncRuntime
        localNodeConnectInfo
        ((interval_ chainsyncConfig) {from = startFrom})
        securityParam'
        (logging_ chainsyncConfig)
        (pipelineSize_ chainsyncConfig)
        (batchSize_ chainsyncConfig)
        (concurrencyPrimitive_ chainsyncConfig)

  return ( extLedgerState, Marconi.getEpochNo extLedgerState
         , chainsyncRuntime'
         , sqlCon, tableName, ledgerConfig)

-- | Load ledger state from disk
initializeLedgerState :: NodeConfig -> FilePath -> IO (SlotNoBhh, Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
initializeLedgerState nodeConfig ledgerStatePath = do
  slotNoBhh <- either (error $ "Can't parse chain-point from filename: " <> ledgerStatePath) return $ bhhFromFileName ledgerStatePath
  (ledgerCfg, extLedgerState) <- loadLedgerState nodeConfig ledgerStatePath
  return (slotNoBhh, ledgerCfg, extLedgerState)

bhhToFileName :: SlotNoBhh -> FilePath
bhhToFileName (slotNo, blockHeaderHash) = L.intercalate "_"
  [ "ledgerState"
  , show slotNo'
  , TS.unpack (C.serialiseToRawBytesHexText blockHeaderHash)
  ]
  where
    slotNo' = coerce slotNo :: Word64

bhhFromFileName :: String -> Either String SlotNoBhh
bhhFromFileName str = case splitOn '_' str of
  _ : slotNoStr : blockHeaderHashHex : _ -> (,)
    <$> parseSlotNo_ slotNoStr
    <*> eitherParseHashBlockHeader_ blockHeaderHashHex
  _ -> Left "Can't parse ledger state file name, must be <slot no> _ <block header hash>"
  where

splitOn :: Eq a => a -> [a] -> [[a]]
splitOn x xs = case span (/= x) xs of
  (prefix, _x : rest) -> prefix : recurse rest
  (lastChunk, [])     -> [lastChunk]
  where
    recurse = splitOn x

parseSlotNo_ :: String -> Either String C.SlotNo
parseSlotNo_ str = maybe (leftError "Can't read SlotNo" str) (Right . C.SlotNo) $ Read.readMaybe str

-- eitherParseHashBlockHeader :: String -> Either RawBytesHexError (C.Hash C.BlockHeader) -- cardano-api-1.35.4:Cardano.Api.SerialiseRaw.
eitherParseHashBlockHeader = C.deserialiseFromRawBytesHex (C.proxyToAsType Proxy) . C8.pack

eitherParseHashBlockHeader_ :: String -> Either String (C.Hash C.BlockHeader)
eitherParseHashBlockHeader_ = either (Left . show) Right . eitherParseHashBlockHeader

leftError :: String -> String -> Either String a
leftError label str = Left $ label <> ": '" <> str <> "'"

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

-- ** Initialise

-- | Initialize sqlite: create connection, run init (e.g create
-- destination table), create checkpoints table if doesn't exist,
-- update interval in runtime config by whether there is anywhere to
-- resume from.
initializeSqlite
  :: FilePath -> String -> (SQL.Connection -> String -> IO ()) -> LocalChainsyncRuntime -> Trace.Trace IO TS.Text -> IO (SQL.Connection, LocalChainsyncRuntime)
initializeSqlite dbPath tableName sqliteInit chainsyncRuntime trace = do
  sqlCon <- sqliteOpen dbPath
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

sqliteOpen :: FilePath -> IO SQL.Connection
sqliteOpen dbPath = do
  sqlCon <- SQL.open dbPath
  SQL.execute_ sqlCon "PRAGMA journal_mode=WAL"
  return sqlCon
