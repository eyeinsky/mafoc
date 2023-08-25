{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DerivingStrategies     #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE MultiWayIf             #-}
{-# LANGUAGE NamedFieldPuns         #-}
{-# LANGUAGE OverloadedLabels       #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE StarIsType             #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilyDependencies #-}
module Mafoc.Core
  ( module Mafoc.Core
  , module Mafoc.Upstream
  , module Mafoc.Upstream.Formats

  -- * Re-exports
  , Marconi.CurrentEra
  ) where

import Control.Concurrent qualified as IO
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TChan qualified as TChan
import Control.Exception qualified as E
import Control.Monad.Trans.Class (lift)
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Set qualified as Set
import Data.String (IsString)
import Data.Text qualified as TS
import Data.Time (UTCTime, diffUTCTime, getCurrentTime)
import Data.Word (Word32)
import Database.SQLite.Simple qualified as SQL
import GHC.OverloadedLabels (IsLabel (fromLabel))
import Numeric.Natural (Natural)
import Options.Applicative qualified as O
import Prettyprinter (Doc, Pretty (pretty), defaultLayoutOptions, layoutPretty, (<+>))
import Prettyprinter.Render.Text (renderStrict)
import Streaming qualified as S
import Streaming.Prelude qualified as S
import System.FilePath ((</>))

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace qualified as Trace
import Cardano.Streaming qualified as CS
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi
import Marconi.ChainIndex.Indexers.MintBurn ()
import Marconi.ChainIndex.Types qualified as Marconi
import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.Exceptions qualified as E
import Mafoc.Logging qualified as Logging
import Mafoc.RollbackRingBuffer qualified as RB
import Mafoc.Upstream ( SlotNoBhh, blockChainPoint, blockSlotNo, blockSlotNoBhh, chainPointSlotNo, defaultConfigStderr
                      , foldYield, getNetworkId, getSecurityParamAndNetworkId, querySecurityParam, tipDistance)
import Mafoc.Upstream.Formats (SlotNoBhhString(SlotNoBhhString), AssetIdString(AssetIdString))
import Mafoc.StateFile qualified as StateFile

-- * Indexer class

-- | Class for an indexer. The argument @a@ doubles as both a type
-- representation (a "tag") for the indexer, and also as the initial
-- configuration required to run the indexer.
class Indexer a where

  -- | A text description of the indexer, used for help messages.
  description :: TS.Text

  -- | A CLI parser for @a@.
  parseCli :: O.Parser a

  -- | The @a@ itself doubles as cli configuration, no need for the following:
  -- type Config a = r | r -> a

  -- | Runtime configuration, i.e the reader for the indexer, used for
  -- e.g the db connection, for communication with other threads
  -- (respond to queries).
  data Runtime a

  -- | Event type, i.e the "business requirement". Any input block is
  -- converted to zero or more events which are then to be persisted.
  data Event a

  -- | The fold state. Some don't require a state so, for those it's
  -- defined as a data type with no fields, equivalent to unit. As a
  -- consequence these indexers can be resumed from arbitrary chain
  -- points on request.
  data State a

  -- | Convert a state and a block to events and a new state.
  toEvents :: Runtime a -> State a -> C.BlockInMode C.CardanoMode -> (State a, [Event a])

  -- | Initialize an indexer from @a@ to a runtime for local
  -- chainsync, indexer's runtime configuration and the indexer state.
  initialize :: a -> Trace.Trace IO TS.Text -> IO (State a, LocalChainsyncRuntime, Runtime a)

  -- | Persist many events at a time, defaults to mapping over events with persist.
  persistMany :: Runtime a -> [Event a] -> IO ()

  -- | Checkpoint indexer by writing the chain point and the state at
  -- that point, destination being provided by the
  -- runtime. Checkpoints are used for resuming
  checkpoint :: Runtime a -> State a -> (C.SlotNo, C.Hash C.BlockHeader) -> IO ()

-- | Run an indexer
runIndexer :: forall a . (Indexer a, Show a) => a -> BatchSize -> StopSignal -> IO ()
runIndexer cli batchSize stopSignal = do
  c <- defaultConfigStderr
  withTrace c "mafoc" $ \trace -> do
    traceInfo trace
       $ "Indexer started\n  Configuration: \n    " <> pretty (show cli)
                     <> "\n  Batch size: " <> pretty (show batchSize)
    (indexerInitialState, lcr, indexerRuntime) <- initialize cli trace

    -- Start streaming blocks over local chainsync
    blockSource
          (securityParam lcr)
          (localNodeConnection lcr)
          (interval lcr)
          (pipelineSize lcr)
          (concurrencyPrimitive lcr)
          (logging lcr)
          trace
          stopSignal

      -- Fold over stream of blocks by converting them to events, then
      -- pass them on together with a chain point and indexer state
      & foldYield (\indexerState blockInMode -> do
                      let (indexerState', events) = toEvents indexerRuntime indexerState blockInMode
                      return (indexerState', (blockSlotNoBhh blockInMode, events, indexerState'))
                  ) indexerInitialState

      -- Persist events in batches and write a checkpoint. Do this
      -- either when `batchSize` amount of events is collected or when
      -- a time limit is reached.
      & batchedPersist indexerRuntime trace batchSize

-- * Buffered output

data BatchState a
  = BatchState { lastCheckpointTime :: UTCTime
               , slotNoBhh          :: SlotNoBhh
               , indexerState       :: State a
               , batchFill          :: BatchSize
               , bufferedEvents     :: [[Event a]]
               }
  | BatchEmpty { lastCheckpointTime :: UTCTime
               , slotNoBhh          :: SlotNoBhh
               , indexerState       :: State a
               }
  | NoProgress { lastCheckpointTime :: UTCTime }

getBatchFill :: BatchState a -> BatchSize
getBatchFill = \case
  BatchState{batchFill} -> batchFill
  _                     -> 0

getBufferedEvents :: BatchState a -> [[Event a]]
getBufferedEvents = \case
  BatchState{bufferedEvents} -> bufferedEvents
  _                          -> []

batchedPersist
  :: forall a . Indexer a
  => Runtime a
  -> Trace.Trace IO TS.Text
  -> BatchSize
  -> S.Stream (S.Of (SlotNoBhh, [Event a], State a)) IO ()
  -> IO ()
batchedPersist indexerRuntime trace batchSize source = do
  now :: UTCTime <- getCurrentTime
  batchState <- S.foldM_ step (pure $ NoProgress now) pure source
  case batchState of
    BatchState{bufferedEvents, indexerState, slotNoBhh} -> do
      persistAndCheckpoint bufferedEvents "exiting" indexerState slotNoBhh
    BatchEmpty{slotNoBhh, indexerState} -> do
      traceInfo trace $ "Checkpointing with empty batch at " <+> pretty slotNoBhh
      checkpoint indexerRuntime indexerState slotNoBhh
    NoProgress{} ->
      traceInfo trace "No progress made, exiting."
  traceInfo trace "Done."
  where
    persistAndCheckpoint :: [[Event a]] -> Doc () -> State a -> SlotNoBhh -> IO ()
    persistAndCheckpoint bufferedEvents msg indexerState slotNoBhh = do
      let (slotNo, bhh) = slotNoBhh
      traceInfo trace $ "Persisting and checkpointing at " <+> pretty (C.ChainPoint slotNo bhh) <+> " because " <+> msg
      case concat $ reverse bufferedEvents of
        events@(_ : _) -> persistMany indexerRuntime events
        []             -> pure ()
      checkpoint indexerRuntime indexerState slotNoBhh

    step :: BatchState a -> (SlotNoBhh, [Event a], State a) -> IO (BatchState a)
    step batchState (slotNoBhh, newEvents, indexerState) = do
      let lastCheckpointTime' = lastCheckpointTime batchState

      now :: UTCTime <- getCurrentTime
      let persistAndCheckpoint' :: [[Event a]] -> Doc () -> IO (BatchState a)
          persistAndCheckpoint' bufferedEvents' msg = do
            persistAndCheckpoint bufferedEvents' msg indexerState slotNoBhh
            return $ BatchEmpty now slotNoBhh indexerState

          isCheckpointTime :: Bool
          isCheckpointTime = diffUTCTime now lastCheckpointTime' > 10

      if not (null newEvents)
        -- There are new events
        then let
          batchFill = getBatchFill batchState
          bufferedEvents = getBufferedEvents batchState
          bufferFill' = batchFill + toEnum (length newEvents)
          bufferedEvents' = newEvents : bufferedEvents
          in if | isCheckpointTime         ->
                  persistAndCheckpoint' bufferedEvents' "it's checkpoint time"
                | bufferFill' >= batchSize ->
                  persistAndCheckpoint' bufferedEvents' "buffer is full"
                | otherwise                -> let
                    batchState' = BatchState lastCheckpointTime' slotNoBhh indexerState bufferFill' bufferedEvents'
                    in return batchState'

        -- There are no new events
        else if
          -- .. but it's checkpoint time so we persist events and checkpoint
          | isCheckpointTime
          , bufferedEvents@(_ : _) <- getBufferedEvents batchState
            -> persistAndCheckpoint' bufferedEvents "it's checkpoint time, but buffer is not full"
          -- Nothing to do, continue with original state
          | isCheckpointTime
            -> do
              checkpoint indexerRuntime indexerState slotNoBhh
              return $ BatchEmpty now slotNoBhh indexerState
          -- Nothing to do, continue with original state
          | otherwise
            -> return batchState

newtype BatchSize = BatchSize Natural
  deriving newtype (Eq, Show, Read, Num, Enum, Ord)

-- * Stop signal

newtype StopSignal = StopSignal (IO.MVar Bool)

-- | Stop streaming when the signal MVar is filled with a True.
takeWhileStopSignal :: StopSignal -> S.Stream (S.Of a) IO r -> S.Stream (S.Of a) IO ()
takeWhileStopSignal (StopSignal stopSignalMVar) = S.takeWhileM (\_ -> not <$> IO.readMVar stopSignalMVar)

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

instance IsLabel "getNetworkId" (LocalChainsyncConfig NodeConfig -> IO C.NetworkId) where
  fromLabel lcc = #getNetworkId (#nodeConfig lcc :: NodeConfig)

-- * LocalChainsyncRuntime

-- | Static configuration for block source
data LocalChainsyncRuntime = LocalChainsyncRuntime
  { localNodeConnection  :: C.LocalNodeConnectInfo C.CardanoMode
  , interval             :: Interval
  , securityParam        :: Marconi.SecurityParam
  , logging              :: Bool
  , pipelineSize         :: Word32

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
  -> StopSignal
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSourceFromLocalChainsyncRuntime lcr = blockSource
  (securityParam lcr)
  (localNodeConnection lcr)
  (interval lcr)
  (pipelineSize lcr)
  (concurrencyPrimitive lcr)
  (logging lcr)

blockSource
  :: Marconi.SecurityParam
  -> C.LocalNodeConnectInfo C.CardanoMode
  -> Interval
  -> Word32
  -> Maybe ConcurrencyPrimitive
  -> Bool
  -> Trace.Trace IO TS.Text
  -> StopSignal
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSource securityParam' lnc interval' pipelineSize' concurrencyPrimitive' logging' trace stopSignal = blocks'
  & (if logging' then Logging.logging trace else id)
  & takeUpTo trace upTo' stopSignal
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
        let msg = "Using " <> pretty (show a) <> " as concurrency variable to pass blocks"
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
  -> StopSignal
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) IO ()
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) IO ()
takeUpTo trace upTo' stopSignal source = case upTo' of
  SlotNo slotNo -> do
    lift $ traceDebug trace $ "Will index up to " <> pretty slotNo
    flip takeWhile' source $ \case
      CS.RollForward blk _ -> blockSlotNo blk <= slotNo
      CS.RollBackward{}    -> True
  Infinity -> takeWhile' (const True) source
  CurrentTip -> S.lift (S.next source) >>= \case
    Left r -> return r
    Right (event, source') -> do
      let tip = getTipPoint event :: C.ChainPoint
      lift $ traceDebug trace $ "Will index up to current tip, which is: " <> pretty tip
      S.yield event -- We can always yield the current event, as that
                    -- is the source for the upper bound anyway.
      flip takeWhile' source' $ \case
        CS.RollForward blk _ct -> blockChainPoint blk <= tip
        CS.RollBackward{}      -> True -- We skip rollbacks as these can ever only to an earlier point
      lift $ traceInfo trace $ "Reached current tip as of when indexing started (this was: " <+> pretty (getTipPoint event) <+> ")"

  where
    getTipPoint :: CS.ChainSyncEvent a -> C.ChainPoint
    getTipPoint = \case
      CS.RollForward _blk ct -> C.chainTipToChainPoint ct
      CS.RollBackward _cp ct -> C.chainTipToChainPoint ct

    -- | Take while either a stop signal is received or when the predicate becomes false.
    takeWhile' :: (a -> Bool) -> S.Stream (S.Of a) IO r -> S.Stream (S.Of a) IO ()
    takeWhile' p = S.takeWhile p . takeWhileStopSignal stopSignal

-- * Trace

traceInfo :: Trace.Trace IO TS.Text -> Doc () -> IO ()
traceInfo trace msg = Trace.logInfo trace $ renderStrict $ layoutPretty defaultLayoutOptions msg

traceInfoStr :: Trace.Trace IO TS.Text -> String -> IO ()
traceInfoStr trace msg = Trace.logInfo trace $ renderStrict $ layoutPretty defaultLayoutOptions $ pretty msg

traceDebug :: Trace.Trace IO TS.Text -> Doc () -> IO ()
traceDebug trace msg = Trace.logDebug trace $ renderStrict $ layoutPretty defaultLayoutOptions msg

renderPretty :: Pretty a => a -> TS.Text
renderPretty = renderStrict . layoutPretty defaultLayoutOptions . pretty

-- * Ledger state checkpoint

-- | Load ledger state from file, while taking the chain point it's at from the file name.
loadLedgerStateWithChainpoint :: NodeConfig -> Trace.Trace IO TS.Text -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_, C.ChainPoint)
loadLedgerStateWithChainpoint nodeConfig@(NodeConfig nodeConfig') trace = do
  ((cfg, ls), cp) <- StateFile.loadLatest "ledgerState" (loadLedgerState nodeConfig) (Marconi.getInitialExtLedgerState nodeConfig')
  case cp of
    C.ChainPointAtGenesis -> traceInfo trace "No ledger state found, initiated from genesis"
    C.ChainPoint{} -> traceInfo trace $ "Found ledger state from " <> pretty cp
  return (cfg, ls, cp)

loadLedgerState :: NodeConfig -> FilePath -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
loadLedgerState (NodeConfig nodeConfig) ledgerStatePath = do
  cfg <- Marconi.getLedgerConfig nodeConfig
  let O.ExtLedgerCfg topLevelConfig = cfg
  extLedgerState <- Marconi.loadExtLedgerState (O.configCodec topLevelConfig) ledgerStatePath >>= \case
    Right (_, extLedgerState)   -> return extLedgerState
    Left cborDeserialiseFailure -> E.throwIO $ E.Can't_deserialise_LedgerState_from_CBOR ledgerStatePath cborDeserialiseFailure
  return (cfg, extLedgerState)

storeLedgerState :: Marconi.ExtLedgerCfg_ -> SlotNoBhh -> Marconi.ExtLedgerState_ -> IO ()
storeLedgerState (O.ExtLedgerCfg topLevelConfig) slotNoBhh extLedgerState =
  Marconi.writeExtLedgerState (StateFile.toName "ledgerState" slotNoBhh) (O.configCodec topLevelConfig) extLedgerState

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
        (concurrencyPrimitive_ chainsyncConfig)

  return ( extLedgerState, Marconi.getEpochNo extLedgerState
         , chainsyncRuntime'
         , sqlCon, tableName, ledgerConfig)

-- | Load ledger state from disk
initializeLedgerState :: NodeConfig -> FilePath -> IO (SlotNoBhh, Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
initializeLedgerState nodeConfig ledgerStatePath = do
  slotNoBhh <- StateFile.bhhFromFileName ledgerStatePath & \case
     Left errMsg -> E.throwIO $ E.Can't_parse_chain_point_from_LedgerState_file_name ledgerStatePath errMsg
     Right slotNoBhh' -> return slotNoBhh'
  (ledgerCfg, extLedgerState) <- loadLedgerState nodeConfig ledgerStatePath
  return (slotNoBhh, ledgerCfg, extLedgerState)

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
    _               -> E.throwIO $ E.The_impossible_happened "Indexer can't have more than one checkpoint in sqlite"

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
      traceInfo trace $ "Found checkpoint that is later than CLI argument from checkpoints table: " <> pretty cp
      return $ cliInterval { from = cp }
      else do
      traceInfo trace $ "Found checkpoint that is not later than CLI argument, checkpoint: " <> pretty cp <> ", cli: " <> pretty (from cliInterval)
      return cliInterval
    _ -> do
      traceInfo trace $ "No checkpoint found, going with CLI starting point: " <> pretty (from cliInterval)
      return cliInterval
  let chainsyncRuntime' = chainsyncRuntime { interval = newInterval }

  return (sqlCon, chainsyncRuntime')

sqliteOpen :: FilePath -> IO SQL.Connection
sqliteOpen dbPath = do
  sqlCon <- SQL.open dbPath
  SQL.execute_ sqlCon "PRAGMA journal_mode=WAL"
  return sqlCon

-- * Address filter

mkMaybeAddressFilter :: [C.Address C.ShelleyAddr] -> Maybe (C.Address C.ShelleyAddr -> Bool)
mkMaybeAddressFilter addresses = case addresses of
  [] -> Nothing
  _ -> Just $ \address -> address `elem` Set.fromList addresses
