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

  -- * Mafoc.Logging
  , renderPretty
  , traceInfo
  ) where

import Control.Concurrent qualified as IO
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TChan qualified as TChan
import Control.Exception qualified as E
import Data.Set qualified as Set
import Data.Text qualified as TS
import Data.Time (UTCTime, diffUTCTime, getCurrentTime, NominalDiffTime)
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import GHC.OverloadedLabels (IsLabel (fromLabel))
import Options.Applicative qualified as O
import Prettyprinter (Doc, Pretty (pretty), (<+>))
import Streaming qualified as S
import Streaming.Prelude qualified as S
import System.FilePath ((</>))
import Servant.Server qualified as Servant

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace qualified as Trace
import Cardano.BM.Data.Severity qualified as CM
import Cardano.Streaming qualified as CS

import Mafoc.Exceptions qualified as E
import Mafoc.Logging (traceInfo, traceDebug, renderPretty, traceNotice)
import Mafoc.Logging qualified as Logging
import Mafoc.RollbackRingBuffer qualified as RB
import Mafoc.Upstream ( SlotNoBhh, blockChainPoint, blockSlotNo, blockSlotNoBhh, chainPointSlotNo, defaultConfigStderrSeverity
                      , foldYield, getNetworkId, getSecurityParamAndNetworkId, querySecurityParam, tipDistance
                      , NodeFolder(NodeFolder), NodeConfig(NodeConfig), SocketPath(SocketPath), TxIndexInBlock
                      , txAddressDatums, txDatums, plutusDatums, allDatums, maybeDatum, txPlutusDatums
                      , LedgerEra, slotEra
                      , SecurityParam(SecurityParam), CurrentEra
                      )
import Mafoc.Upstream.Formats (SlotNoBhhString(SlotNoBhhString), AssetIdString(AssetIdString))
import Mafoc.Signal qualified as Signal
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

type RunIndexer =
     BatchSize
  -> Signal.Stop
  -> Signal.Checkpoint
  -> Signal.ChainsyncStats
  -> CM.Severity
  -> CheckpointInterval
  -> IO ()

-- | Run an indexer
runIndexer
  :: forall a . (Indexer a, Show a)
  => a
  -> Maybe (Runtime a -> IO.MVar (State a, BatchState a) -> IO ())
  -> RunIndexer
runIndexer cli maybeApiServer batchSize stopSignal checkpointSignal statsSignal minSeverity checkpointInterval = do
  c <- defaultConfigStderrSeverity minSeverity
  withTrace c "mafoc" $ \trace -> do
    initialNotice cli batchSize minSeverity checkpointInterval trace

    (indexerInitialState, lcr, indexerRuntime) <- initialize cli trace
    let (fromChainPoint, upTo) = interval lcr
    initialBatchState <- NoProgress fromChainPoint <$> getCurrentTime
    persistStep'' <- let
      persistStep' = persistStep trace indexerRuntime checkpointSignal batchSize (checkpointIntervalPredicate checkpointInterval)
      in case maybeApiServer of
        Nothing -> return persistStep'
        Just runServer -> do
          mvar <- IO.newMVar (indexerInitialState, initialBatchState)
          _ <- IO.forkIO $ runServer indexerRuntime mvar -- start HTTP API server
          return $ \batchState tup@(_, _, indexerState) -> do
            _ <- IO.swapMVar mvar (indexerState, batchState) -- update mvar with indexer state and batch state
            persistStep' batchState tup

    maybeProfiling <- traverse (Logging.profilerInit trace (show cli)) (profiling lcr)

    -- Start streaming blocks over local chainsync
    traceNotice trace $ "Starting local chainsync mini-protocol at: " <> pretty fromChainPoint
    batchState <- blockProducer (localNodeConnection lcr) (pipelineSize lcr) fromChainPoint (concurrencyPrimitive lcr)
      & (if logging lcr then Logging.logging trace statsSignal else id)
      & maybe id Logging.profileStep maybeProfiling
      & S.drop 1 -- The very first event from local chainsync is always a
                 -- rewind. We skip this because we don't have anywhere to
                 -- rollback to anyway.
      & takeUpTo trace upTo stopSignal
      & rollbackRingBuffer (securityParam lcr)

      -- Fold over stream of blocks by converting them to events, then
      -- pass them on together with a chain point and indexer state
      & foldYield (\indexerState blockInMode -> do
                      let (indexerState', events) = toEvents indexerRuntime indexerState blockInMode
                      return (indexerState', (blockSlotNoBhh blockInMode, events, indexerState'))
                  ) indexerInitialState

      -- Persist events in batches and write a checkpoint. Do this
      -- either when `batchSize` amount of events is collected or when
      -- a time limit is reached.
      & S.foldM_ persistStep'' (pure initialBatchState) pure

    -- Streaming done, write final batch of events and checkpoint
    maybeCp <- persistStepFinal indexerRuntime batchState trace

    -- Write last profiler event
    _ <- traverse (\profiling -> Logging.profilerEnd profiling maybeCp) maybeProfiling

    traceInfo trace "Done."

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
  | NoProgress { chainPointAtStart  :: C.ChainPoint
               , lastCheckpointTime :: UTCTime
               }

getBatchFill :: BatchState a -> BatchSize
getBatchFill = \case
  BatchState{batchFill} -> batchFill
  _                     -> 0

getBufferedEvents :: BatchState a -> [[Event a]]
getBufferedEvents = \case
  BatchState{bufferedEvents} -> bufferedEvents
  _                          -> []

persistStep
  :: forall a . Indexer a
  => Trace.Trace IO TS.Text -> Runtime a -> Signal.Checkpoint -> BatchSize -> CheckpointPredicateInterval
  -> BatchState a -> (SlotNoBhh, [Event a], State a) -> IO (BatchState a)
persistStep trace indexerRuntime checkpointSignal batchSize isCheckpointTimeP batchState (slotNoBhh, newEvents, indexerState) = do
  let lastCheckpointTime' = lastCheckpointTime batchState

  now :: UTCTime <- getCurrentTime
  checkpointRequested :: Bool <- Signal.resetGet checkpointSignal
  let persistAndCheckpoint' :: [[Event a]] -> Doc () -> IO (BatchState a)
      persistAndCheckpoint' bufferedEvents' msg = do
        let (slotNo, bhh) = slotNoBhh
        traceInfo trace $ "Persisting and checkpointing at " <+> pretty (C.ChainPoint slotNo bhh) <+> " because " <+> msg
        persistMany indexerRuntime (concat $ reverse bufferedEvents')
        checkpoint indexerRuntime indexerState slotNoBhh
        return $ BatchEmpty now slotNoBhh indexerState

      isCheckpointTime :: Bool
      isCheckpointTime = isCheckpointTimeP lastCheckpointTime' now

      bufferedEvents = getBufferedEvents batchState
      bufferedAndNewEvents = newEvents : bufferedEvents

  if | checkpointRequested ->
         persistAndCheckpoint' bufferedAndNewEvents "checkpoint was requested"
     -- There are new events
     | not (null newEvents) -> let
         batchFill = getBatchFill batchState
         bufferFill' = batchFill + toEnum (length newEvents)
         in if | isCheckpointTime         ->
                 persistAndCheckpoint' bufferedAndNewEvents "it's checkpoint time"
               | bufferFill' >= batchSize ->
                 persistAndCheckpoint' bufferedAndNewEvents "buffer is full"
               | otherwise                -> let
                   batchState' = BatchState lastCheckpointTime' slotNoBhh indexerState bufferFill' bufferedAndNewEvents
                   in return batchState'
     -- There are no new events
     | otherwise -> if
         -- .. but it's checkpoint time so we persist events and checkpoint
         | isCheckpointTime
         , (_ : _) <- bufferedEvents
           -> persistAndCheckpoint' bufferedEvents "it's checkpoint time, but buffer is not full"
         -- Nothing to do, continue with original state
         | isCheckpointTime
           -> do
             checkpoint indexerRuntime indexerState slotNoBhh
             return $ BatchEmpty now slotNoBhh indexerState
         -- Nothing to do, continue with original state
         | otherwise
           -> return batchState

persistStepFinal :: Indexer a => Runtime a -> BatchState a -> Trace.Trace IO TS.Text -> IO (Maybe C.ChainPoint)
persistStepFinal indexerRuntime batchState trace = case batchState of
  BatchState{bufferedEvents, indexerState, slotNoBhh} -> do
    let cp = #chainPoint slotNoBhh :: C.ChainPoint
    persistMany indexerRuntime (concat bufferedEvents)
    checkpoint indexerRuntime indexerState slotNoBhh
    traceNotice trace $ "Exiting at " <+> pretty cp <+> ", persisted and checkpointed."
    return $ Just cp
  BatchEmpty{slotNoBhh, indexerState} -> do
    let cp = #chainPoint slotNoBhh :: C.ChainPoint
    checkpoint indexerRuntime indexerState slotNoBhh
    traceNotice trace $ "Exiting at " <+> pretty cp <+> ", checkpointed."
    return $ Just cp
  NoProgress{} -> do
    traceNotice trace "No progress made, exiting."
    return Nothing

newtype BatchSize = BatchSize Natural
  deriving newtype (Eq, Show, Read, Num, Enum, Ord)

-- * Checkpoint interval

data CheckpointInterval
  = Never
  | Every NominalDiffTime
  deriving (Eq, Show)

type CheckpointPredicateInterval = UTCTime -> UTCTime -> Bool

checkpointIntervalPredicate :: CheckpointInterval -> CheckpointPredicateInterval
checkpointIntervalPredicate = \case
  Never -> \_lastCheckpointTime _now -> True
  Every n ->  \lastCheckpointTime now -> diffUTCTime now lastCheckpointTime > n


-- * HTTP

-- ** REST API

class Indexer a => IndexerHttpApi a where

  type API a

  server :: Runtime a -> IO.MVar (State a, BatchState a) -> Servant.Server (API a)

-- * Trace

initialNotice :: Show a => a -> BatchSize -> CM.Severity -> CheckpointInterval -> Trace.Trace IO TS.Text -> IO ()
initialNotice cli batchSize minSeverity checkpointInterval trace = traceNotice trace
  $ "Indexer started\n  Configuration: \n    " <> pretty (show cli)
                <> "\n  Batch size: " <> pretty (show batchSize)
                <> "\n  Logging severity: " <> pretty (show minSeverity)
                <> "\n  Checkpoint interval: " <> pretty (show checkpointInterval)

-- * Local chainsync: config and runtime

newtype NodeInfo a = NodeInfo (Either NodeFolder (SocketPath, a))
  deriving Show

-- | Configuration for local chainsync streaming setup.
data LocalChainsyncConfig a = LocalChainsyncConfig
  { nodeInfo              :: NodeInfo a
  , intervalInfo          :: (Interval, Maybe DbPathAndTableName)
  , logging_              :: Bool
  , profiling_            :: Maybe Logging.ProfilingConfig
  , pipelineSize_         :: Word32
  , concurrencyPrimitive_ :: ConcurrencyPrimitive
  } deriving Show

type LocalChainsyncConfig_ = LocalChainsyncConfig (Either C.NetworkId NodeConfig)

-- ** Get and derive stuff from LocalChainsyncConfig

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
  , interval             :: (C.ChainPoint, UpTo)
  , securityParam        :: SecurityParam
  , logging              :: Bool
  , profiling            :: Maybe Logging.ProfilingConfig
  , pipelineSize         :: Word32

  -- * Internal
  , concurrencyPrimitive :: ConcurrencyPrimitive
  }

modifyStartingPoint :: LocalChainsyncRuntime -> (C.ChainPoint -> C.ChainPoint) -> LocalChainsyncRuntime
modifyStartingPoint lcr f = lcr { interval = (f oldCp, upTo) }
  where (oldCp, upTo) = interval lcr

initializeLocalChainsync :: LocalChainsyncConfig a -> C.NetworkId  -> Trace.Trace IO TS.Text -> IO LocalChainsyncRuntime
initializeLocalChainsync localChainsyncConfig networkId trace = do
  let SocketPath socketPath' = #socketPath localChainsyncConfig
  let localNodeCon = CS.mkLocalNodeConnectInfo networkId socketPath'
  securityParam' <- querySecurityParam localNodeCon

  -- Resolve possible SlotNo in interval start:
  let (Interval from upTo, maybeDbPathAndTableName) = intervalInfo localChainsyncConfig
  cliChainPoint <- intervalStartToChainSyncStart trace maybeDbPathAndTableName from

  return $ LocalChainsyncRuntime
    localNodeCon
    (cliChainPoint, upTo)
    securityParam'
    (logging_ localChainsyncConfig)
    (profiling_ localChainsyncConfig)
    (pipelineSize_ localChainsyncConfig)
    (concurrencyPrimitive_ localChainsyncConfig)

-- | Resolve @LocalChainsyncConfig@ that came from e.g command line
-- arguments into an "actionable" @LocalChainsyncRuntime@ runtime
-- config which can be used to generate a stream of blocks.
initializeLocalChainsync_ :: LocalChainsyncConfig_ -> Trace.Trace IO TS.Text -> IO LocalChainsyncRuntime
initializeLocalChainsync_ config trace = do
  networkId <- #getNetworkId config
  initializeLocalChainsync config networkId trace

rollbackRingBuffer
  :: SecurityParam
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO ()
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
rollbackRingBuffer securityParam' = RB.rollbackRingBuffer (fromIntegral securityParam') tipDistance blockSlotNoBhh

blockProducer
  :: forall r
   . C.LocalNodeConnectInfo C.CardanoMode
  -> Word32
  -> C.ChainPoint
  -> ConcurrencyPrimitive
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
blockProducer lnc pipelineSize' fromCp concurrencyPrimitive' = let
  blocks
    :: forall a
     . IO a
    -> (a -> CS.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ())
    -> (a -> IO (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode)))
    -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  blocks = if pipelineSize' > 1
    then CS.blocksPipelinedPrim pipelineSize' lnc fromCp
    else CS.blocksPrim lnc fromCp

  mvar f = f IO.newEmptyMVar IO.putMVar IO.takeMVar
  chan f = f IO.newChan IO.writeChan IO.readChan
  tchan f = f TChan.newTChanIO (\mv e -> STM.atomically $ STM.writeTChan mv e) (STM.atomically . STM.readTChan)

  in case concurrencyPrimitive' of
       MVar -> mvar blocks
       Chan -> chan blocks
       TChan -> tchan blocks

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
  { from :: (Bool, Either C.SlotNo C.ChainPoint)
  , upTo :: UpTo
  } deriving (Show)

takeUpTo
  :: Trace.Trace IO TS.Text
  -> UpTo
  -> Signal.Stop
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
    takeWhile' p = S.takeWhile p . Signal.takeWhileStopSignal stopSignal

-- * Ledger state checkpoint

loadLatestTrace :: String -> IO a ->(FilePath -> IO a) -> Trace.Trace IO TS.Text -> IO (a, C.ChainPoint)
loadLatestTrace prefix init_ load trace = do
  (a, cp) <- StateFile.loadLatest prefix load init_
  case cp of
    C.ChainPointAtGenesis -> traceInfo trace $ "Load state: " <> pretty prefix <> " not found, starting with initial state from genesis"
    C.ChainPoint{} -> traceInfo trace $ "Load state: " <> pretty prefix <> " found at "  <> pretty cp
  return (a, cp)

-- * Sqlite

-- | Helper to query with a single param
query1 :: (SQL.ToField q, SQL.FromRow r) => SQL.Connection -> SQL.Query -> q -> IO [r]
query1 con query param = SQL.query con query $ SQL.Only param

-- ** Optional conditions

mkParam :: SQL.ToField v => SQL.Query -> TS.Text -> v -> (SQL.NamedParam, SQL.Query)
mkParam condition label value = (label SQL.:= value, condition)

-- | Convert @[":field1 = field1", ":field2 = field2"]@ into @":field1 = field1 AND :field2 = field2"@
andFilters :: [SQL.Query] -> SQL.Query
andFilters = \case
  [] -> " TRUE "
  filters -> coerce $ TS.intercalate " AND " $ coerce filters

-- ** Database path and table name defaulting

data DbPathAndTableName = DbPathAndTableName (Maybe FilePath) (Maybe String)
  deriving (Show)

defaultTableName :: String -> DbPathAndTableName -> (FilePath, String)
defaultTableName defaultName (DbPathAndTableName maybeDbPath maybeName) = (fromMaybe "default.db" maybeDbPath, fromMaybe defaultName maybeName)

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

-- ** Initialise

-- | If ChainPointAtGenesis is returned, then there was no chain point in the database.
initializeSqlite :: FilePath -> String -> IO (SQL.Connection, C.ChainPoint)
initializeSqlite dbPath tableName = do
  sqlCon <- sqliteOpen dbPath
  sqliteInitCheckpoints sqlCon
  checkpointedChainPoint <- fromMaybe C.ChainPointAtGenesis <$> getCheckpointSqlite sqlCon tableName
  return (sqlCon, checkpointedChainPoint)

sqliteOpen :: FilePath -> IO SQL.Connection
sqliteOpen dbPath = do
  sqlCon <- SQL.open dbPath
  SQL.execute_ sqlCon "PRAGMA journal_mode=WAL"
  return sqlCon

-- ** ChainPoint

eventsToSingleChainpoint :: [(C.SlotNo, C.Hash C.BlockHeader)] -> Maybe C.ChainPoint
eventsToSingleChainpoint = \case
  ((slotNo, hash) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
  _                    -> Nothing


chainPointForSlotNo :: SQL.Connection -> String -> C.SlotNo -> IO (Maybe C.ChainPoint)
chainPointForSlotNo sqlCon tableName slotNo = fmap eventsToSingleChainpoint $ SQL.query sqlCon query $ SQL.Only slotNo
  where
    query :: SQL.Query
    query = "SELECT slot_no, block_header_hash     \
            \  FROM " <> fromString tableName <> " \
            \ WHERE slot_no = ?                    "

previousChainPointForSlotNo :: SQL.Connection -> String -> C.SlotNo -> IO (Maybe C.ChainPoint)
previousChainPointForSlotNo sqlCon tableName slotNo = fmap eventsToSingleChainpoint $ SQL.query sqlCon query $ SQL.Only slotNo
  where
    query :: SQL.Query
    query = "SELECT slot_no, block_header_hash     \
            \  FROM " <> fromString tableName <> " \
            \ WHERE slot_no < ?                    \
            \ ORDER BY slot_no DESC                \
            \ LIMIT 1                              "

-- | Convert starting point from CLI to chainpoint, possibly with the help of header DB.
intervalStartToChainSyncStart
  :: Trace.Trace IO TS.Text
  -> Maybe DbPathAndTableName
  -> (Bool, Either C.SlotNo C.ChainPoint)
  -> IO C.ChainPoint
intervalStartToChainSyncStart trace maybeDbPathAndTableName (include, eitherSlotOrCp)
  | Just dbPathAndTableName <- maybeDbPathAndTableName, Left slotNo <- eitherSlotOrCp =
    let (dbPath, tableName) = defaultTableName "blockbasics" dbPathAndTableName
    in do
      sqlCon <- SQL.open dbPath
      if include
        then let
          in case slotNo of
               0 -> pure C.ChainPointAtGenesis
               _slotNo -> do
                 maybeChainPoint <- previousChainPointForSlotNo sqlCon tableName slotNo
                 case maybeChainPoint of
                   Just cp -> pure cp
                   Nothing -> E.throwIO $ E.Can't_find_previous_ChainPoint_to_slot slotNo

        else chainPointForSlotNo sqlCon tableName slotNo >>= \case
          Nothing -> E.throwIO $ E.SlotNo_not_found slotNo
          Just cp -> do
            traceInfo trace $ pretty slotNo <> " resolved to " <> pretty cp
            pure cp

  | Right cp <- eitherSlotOrCp = pure cp
  | otherwise = E.throwIO E.No_headerDb_specified

-- * Address filter

mkMaybeAddressFilter :: [C.Address C.ShelleyAddr] -> Maybe (C.Address C.ShelleyAddr -> Bool)
mkMaybeAddressFilter addresses = case addresses of
  [] -> Nothing
  _ -> Just $ \address -> address `elem` Set.fromList addresses

-- * Generic

todo :: a
todo = undefined
