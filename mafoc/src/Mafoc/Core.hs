{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE DerivingVia    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Core
  ( module Mafoc.Core
  , module Mafoc.Upstream
  , module Mafoc.Upstream.Formats

  -- * Mafoc.Logging
  , renderPretty
  , traceInfo
  ) where

import Control.Concurrent qualified as IO
import Control.Concurrent.Async qualified as IO
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TChan qualified as TChan
import Control.Exception qualified as E
import Data.Set qualified as Set
import Data.List qualified as L
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
import Cardano.BM.Setup qualified as Trace
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
                      , queryNodeTip
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

  -- | Initialize an indexer from configuration @a@ to a tuple of
  --
  --     (1) indexer state. Stateful indexers load this from a file or
  --     database, stateless indexers return a dummy state (equivalent
  --     to @()@)
  --
  --     (2) chainsync runtime. To do this, they query security param
  --     from node, resolve @SlotNo@ to @ChainPoint@, and get the
  --     @NetworkId@ either directly from cli or via cardano-node's
  --     config file.
  --
  --     (3) indexer-specific runtime. This includes sqlite
  --     connection, relevant table names, filtering functions, and
  --     whatever else static an indexer might need.
  --
  initialize :: a -> Trace.Trace IO TS.Text -> IO (State a, LocalChainsyncRuntime, Runtime a)

  -- | Persist many events at a time, defaults to mapping over events with persist.
  persistMany :: Runtime a -> [Event a] -> IO ()

  -- | Checkpoint indexer by writing the chain point and the state at
  -- that point, destination being provided by the
  -- runtime. Checkpoints are used for resuming
  checkpoint :: Runtime a -> State a -> (C.SlotNo, C.Hash C.BlockHeader) -> IO ()

data CommonConfig = CommonConfig
  { batchSize :: BatchSize
  , stopSignal :: Signal.Stop
  , checkpointSignal :: Signal.Checkpoint
  , statsSignal :: Signal.ChainsyncStats
  , severity :: CM.Severity
  , checkpointInterval :: CheckpointInterval
  }

-- | Run an indexer
runIndexer
  :: forall a . (Indexer a, Show a)
  => a
  -> CommonConfig
  -> Maybe (RunHttpApi a)
  -> IO ()
runIndexer cli commonConfig@CommonConfig{batchSize, stopSignal, checkpointSignal, statsSignal, severity, checkpointInterval} maybeApiServer = stderrTrace severity $ \trace -> do
  initialNotice cli commonConfig Nothing trace
  (indexerInitialState, lcr, indexerRuntime) <- initialize cli trace
  let (fromChainPoint, _upTo) = interval lcr
  lastCheckpointTime <- getCurrentTime
  let initialBatchState = NoProgress
        { chainPointAtStart = fromChainPoint
        , chainPointCurrent = fromChainPoint
        , indexerState = indexerInitialState
        , lastCheckpointTime
        }

  let
    persistStep' :: BatchStepper a
    persistStep' = persistStep indexerRuntime checkpointSignal batchSize (checkpointIntervalPredicate checkpointInterval) trace

  persistStep'' <- case maybeApiServer of
    Nothing -> return persistStep'
    Just runServer -> hookHttpApi indexerRuntime indexerInitialState initialBatchState runServer persistStep'

  traceNotice trace $ "Starting local chainsync mini-protocol at: " <> pretty fromChainPoint
  runIndexerStream statsSignal stopSignal indexerInitialState persistStep'' initialBatchState indexerRuntime trace lcr
  traceInfo trace "Done."

runIndexerStream
  :: Indexer a
  => Signal.ChainsyncStats
  -> Signal.Stop
  -> State a
  -> BatchStepper a
  -> BatchState a
  -> Runtime a
  -> Trace.Trace IO TS.Text
  -> LocalChainsyncRuntime
  -> IO ()
runIndexerStream statsSignal stopSignal indexerInitialState persistStep_ initialBatchState indexerRuntime trace lcr = let
  (fromChainPoint, upTo) = interval lcr
  (profilerStep_, profilerEnd_) = case profiling lcr of
    Just profilerRuntime -> (Logging.profileStep profilerRuntime, Logging.profilerEnd profilerRuntime)
    Nothing -> (id, \_ -> return ())

  in do
  batchState <- blockProducer (localNodeConnection lcr) (pipelineSize lcr) fromChainPoint (concurrencyPrimitive lcr)
    & (if logging lcr then Logging.logging trace statsSignal else id)
    & profilerStep_
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
    & S.foldM_ persistStep_ (pure initialBatchState) pure

  -- Streaming done, write final batch of events and checkpoint
  maybeCp <- persistStepFinal indexerRuntime batchState trace

  -- Write last profiler event
  _ <- profilerEnd_ maybeCp

  return ()

-- * Parallel

type StatelessIndexer a = (Indexer a, Coercible (State a) ())

stateless :: (State a ~ s, Coercible s ()) => s
stateless = coerce ()


data IntervalLength
  = Slots Natural
  | Percent Scientific
  deriving (Show)

percentToSlots :: C.SlotNo -> C.SlotNo -> Scientific -> Natural
percentToSlots start end percent = truncate result
  where
    result = (fromIntegral (end - start) * percent) / 100 :: Scientific

data ParallelismConfig = ParallelismConfig
  { intervalLength :: IntervalLength
  , maybeMaxThreads :: Maybe Natural
  } deriving (Show)

defaultParallelism :: ParallelismConfig
defaultParallelism = ParallelismConfig
  { intervalLength = Slots 3_000_000 -- 10 seconds worth of work with 15k blocks/second
  , maybeMaxThreads = Nothing        -- Pick as many threads as there are capabilities
  }

-- | Run stateless indexer in parallel
runIndexerParallel
  :: forall a . (StatelessIndexer a, Show a)
  => a
  -> CommonConfig
  -> ParallelismConfig
  -> IO ()
runIndexerParallel
  cli
  commonConfig@CommonConfig{severity, checkpointInterval, batchSize, checkpointSignal, statsSignal, stopSignal}
  parallelismConfig@ParallelismConfig{intervalLength, maybeMaxThreads}
  = stderrTrace severity $ \trace0 -> do
  let trace = Trace.appendName "parallel" trace0

  (_state, lcrInitial, indexerRuntime) <- initialize cli trace

  -- Full chain interval to be indexed
  (start, end) <- do
    nodeTip <- queryNodeTip (localNodeConnection lcrInitial)
    let (start, upTo') = interval lcrInitial
    return (start, getUpperBound nodeTip upTo')

  headerDb <- ensureHeaderDb $ maybeHeaderDb lcrInitial

  let persistStep_ :: Trace.Trace IO TS.Text -> BatchStepper a
      persistStep_ = persistStep indexerRuntime checkpointSignal batchSize (checkpointIntervalPredicate checkpointInterval)

  let intervalLengthSlots = case intervalLength of
        Slots intervalLengthSlots' -> intervalLengthSlots'
        Percent percent -> percentToSlots (#slotNo start) end percent

  numCapabilities <- IO.getNumCapabilities
  let maxThreads = maybe numCapabilities fromIntegral maybeMaxThreads
  initialNotice cli commonConfig (Just (parallelismConfig, intervalLengthSlots, maxThreads, numCapabilities)) trace

  -- get chain point for full interval end
  maybeEndCp <- chainPointNearSlotNo headerDb "<=" "DESC" end
  endCp <- maybe (E.throwIO $ E.SlotNo_not_found end) return maybeEndCp
  let fullInterval' = (start, endCp)

  threadLimit <- IO.newQSem maxThreads
  let loop inProgress asyncs = do
        IO.waitQSem threadLimit
        maybeInterval <- nextInterval headerDb (fromIntegral intervalLengthSlots) fullInterval' inProgress
        case maybeInterval of
          Just interval'@(sectionStart, sectionEnd) -> do
            async <- IO.async $ do
              let sectionTrace = mkSectionTrace interval' trace
                  lcr = lcrInitial { interval = (sectionStart, SlotNo $ #slotNo sectionEnd) }
              lastCheckpointTime <- getCurrentTime
              let initialBatchState = NoProgress
                    { chainPointAtStart = sectionStart
                    , lastCheckpointTime
                    , chainPointCurrent = sectionStart
                    , indexerState = stateless
                    }
              traceInfo sectionTrace "Section indexer started."
              runIndexerStream statsSignal stopSignal stateless (persistStep_ sectionTrace) initialBatchState indexerRuntime sectionTrace lcr
              traceInfo sectionTrace "Section indexer done."
              IO.signalQSem threadLimit
            loop (interval' : inProgress) (async : asyncs)
          Nothing -> return asyncs

  mapM_ IO.wait =<< loop [] []

nextIntervalStart :: HeaderDb -> C.ChainPoint -> [(C.ChainPoint, C.ChainPoint)] -> IO C.ChainPoint
nextIntervalStart headerDb start inProgress = case map snd inProgress of
  x : xs -> return $ L.foldl' max x xs
  [] -> case start of
    C.ChainPointAtGenesis -> return C.ChainPointAtGenesis
    C.ChainPoint{} -> let
      startSlotNo = #slotNo start
      in chainPointNearSlotNo headerDb "<" "DESC" startSlotNo >>= \case
        Just less -> return less
        Nothing -> E.throwIO $ E.Can't_find_previous_ChainPoint_to_slot startSlotNo

nextIntervalEnd :: HeaderDb -> C.ChainPoint -> C.ChainPoint -> C.SlotNo -> IO C.ChainPoint
nextIntervalEnd headerDb intervalStart cliEnd maxSize =
  let preliminaryEnd = #slotNo intervalStart + maxSize
  in if #slotNo cliEnd <= preliminaryEnd
    then return cliEnd
    else do
      maybeEnd <- chainPointNearSlotNo headerDb "<=" "DESC" preliminaryEnd
      maybe (E.throwIO $ E.SlotNo_not_found preliminaryEnd) return maybeEnd

nextInterval :: HeaderDb -> C.SlotNo -> (C.ChainPoint, C.ChainPoint) -> [(C.ChainPoint, C.ChainPoint)] -> IO (Maybe (C.ChainPoint, C.ChainPoint))
nextInterval headerDb maxSize (start, end) inProgress = do
  intervalStart :: C.ChainPoint <- nextIntervalStart headerDb start inProgress
  if intervalStart >= end
    then return Nothing
    else do
      intervalEnd :: C.ChainPoint <- nextIntervalEnd headerDb intervalStart end maxSize
      return $ Just (intervalStart, intervalEnd)

mkSectionTrace :: (C.ChainPoint, C.ChainPoint) -> Trace.Trace IO TS.Text -> Trace.Trace IO TS.Text
mkSectionTrace (start, end) trace = Trace.appendName sectionName trace
  where sectionName = "section(" <> slotText start <> ", " <> slotText end <> "]"

traceChainIntervals :: (Doc () -> IO ()) -> [(C.ChainPoint, C.ChainPoint)] -> IO ()
traceChainIntervals tracer chainIntervals = do
  tracer $ "Intervals" <+> "(" <> pretty (length chainIntervals) <> "):"
  forM_ chainIntervals $ \(intervalStart, intervalEnd) -> do
    let start' = #slotNo intervalStart :: C.SlotNo
        end' = #slotNo intervalEnd :: C.SlotNo
    tracer $ "(" <> pretty start' <> ", " <> pretty end' <> "]"

-- | For parallel indexing, ensure that a header database was received
-- from the CLI. IO is required for precise exceptions.
ensureHeaderDb :: Maybe HeaderDb -> IO HeaderDb
ensureHeaderDb = \case
  Just headerDb -> return headerDb
  Nothing -> E.throwIO $ E.No_headerDb_specified "Parallel indexing requires a block header database"

-- ** Chain intervals

getUpperBound :: C.ChainTip -> UpTo -> C.SlotNo
getUpperBound tip upTo = case upTo of
  SlotNo requested -> min safeTipSlot requested
  Infinity -> safeTipSlot
  CurrentTip -> safeTipSlot
  where
    safetyMargin = 129600 -- See https://github.com/CardanoSolutions/kupo/discussions/136#discussioncomment-7141135
    safeTipSlot = #slotNo tip - safetyMargin :: C.SlotNo

getChainIntervals :: (C.ChainPoint, C.SlotNo) -> Natural -> HeaderDb -> IO ([(C.ChainPoint, C.ChainPoint)], (SQL.Connection, String))
getChainIntervals (startCp, end) intervalLength (headerDbSqlConnection, headerDbTable) = do
  firstLowerBound :: C.ChainPoint <- case startCp of
    C.ChainPointAtGenesis -> return C.ChainPointAtGenesis
    C.ChainPoint startSlotNo' _ -> do
      maybeCp <- previousChainPointForSlotNo headerDbSqlConnection headerDbTable startSlotNo'
      maybe (E.throwIO $ E.Previous_ChainPoint_for_slot_not_found startSlotNo') return maybeCp

  lowerBounds' <- getSqliteIntervalBounds headerDbSqlConnection headerDbTable intervalLength (#slotNo startCp) end Nothing
  let chainBounds = firstLowerBound : map #chainPoint lowerBounds'
  return
    ( zip chainBounds $ tail chainBounds -- (start, end]
    , (headerDbSqlConnection, headerDbTable)
    )


-- | With help of header db, get the @ChainPoint@s which divide the chain into equal intervals.
getSqliteIntervalBounds :: SQL.Connection -> String -> Natural -> C.SlotNo -> C.SlotNo -> Maybe Natural -> IO [SlotNoBhh]
getSqliteIntervalBounds headerDbSqlConnection headerDbTable intervalLength start end maybeLimit = SQL.query_ headerDbSqlConnection query
  where
    showViaNat a = fromShow (fromIntegral a :: Natural)

    limit :: SQL.Query
    limit = maybe "" (\limit' -> "LIMIT " <> fromShow limit') maybeLimit

    query :: SQL.Query
    query = " SELECT slot_no, block_header_hash \
            \   FROM " <> fromString headerDbTable <> " headers \
            \   JOIN ("<> upperBounds <>") interval ON headers.slot_no = interval.max_slot_no \
            \  WHERE headers.slot_no <= " <> showViaNat end <> " \
            \ " <> limit

    upperBounds :: SQL.Query
    upperBounds =
      " SELECT MAX(slot_no) AS max_slot_no, ((slot_no - " <> showViaNat start <> ") / " <> fromShow intervalLength <> ") AS x \
      \   FROM blockbasics \
      \  WHERE x >= -1 \
      \  GROUP BY x "


printChainIntervals :: (StatelessIndexer a, Show a) => a -> CommonConfig -> ParallelismConfig -> IO ()
printChainIntervals cli commonConfig@CommonConfig{severity} parallelismConfig@ParallelismConfig{intervalLength, maybeMaxThreads} = stderrTrace severity $ \trace -> do

  (_state, lcrInitial, _indexerRuntime) <- initialize cli trace
  nodeTip <- queryNodeTip (localNodeConnection lcrInitial)
  let (start, upTo) = interval lcrInitial
      end = getUpperBound nodeTip upTo
  let intervalLengthSlots = case intervalLength of
        Slots intervalLengthSlots' -> intervalLengthSlots'
        Percent percent -> percentToSlots (#slotNo start) end percent

  numCapabilities <- IO.getNumCapabilities
  let maxThreads = maybe numCapabilities fromIntegral maybeMaxThreads
  initialNotice cli commonConfig (Just (parallelismConfig, intervalLengthSlots, maxThreads, numCapabilities)) trace

  headerDb <- ensureHeaderDb $ maybeHeaderDb lcrInitial
  (chainIntervals, (sqlCon, tableName)) <- getChainIntervals (start, end) intervalLengthSlots headerDb

  putStrLn "--- start ---"
  labelPrint "start" (#slotNo start :: C.SlotNo)
  labelPrint "upTo" upTo
  labelPrint "intervalLengthSlots" intervalLengthSlots
  putStrLn "Tips:"
  labelPrint " - node tip" (#slotNo nodeTip :: C.SlotNo)
  labelPrint " - header db tip" . map SQL.fromOnly =<< SQL.query_ @(SQL.Only C.SlotNo) sqlCon ("select MAX(slot_no) from " <> fromString tableName)
  putStrLn "Intervals"
  traceChainIntervals print chainIntervals
  putStrLn "--- end ---"


-- * Buffered output

data BatchState a
  = BatchState { lastCheckpointTime :: UTCTime
               , indexerState       :: State a
               , slotNoBhh          :: SlotNoBhh
               , batchFill          :: BatchSize
               , bufferedEvents     :: [[Event a]]
               }
  | BatchEmpty { lastCheckpointTime :: UTCTime
               , indexerState       :: State a
               , slotNoBhh          :: SlotNoBhh
               }
  | NoProgress { lastCheckpointTime :: UTCTime
               , indexerState       :: State a
               , chainPointAtStart  :: C.ChainPoint
               , chainPointCurrent  :: C.ChainPoint
               }

getBatchFill :: BatchState a -> BatchSize
getBatchFill = \case
  BatchState{batchFill} -> batchFill
  _                     -> 0

getBufferedEvents :: BatchState a -> [[Event a]]
getBufferedEvents = \case
  BatchState{bufferedEvents} -> bufferedEvents
  _                          -> []

type BatchStepper a = BatchState a -> (SlotNoBhh, [Event a], State a) -> IO (BatchState a)

persistStep
  :: forall a . Indexer a
  => Runtime a -> Signal.Checkpoint -> BatchSize -> CheckpointPredicateInterval -> Trace.Trace IO TS.Text
  -> BatchStepper a
persistStep
  indexerRuntime checkpointSignal batchSize isCheckpointTimeP trace0
  batchState (slotNoBhh, newEvents, indexerState)

  = do

  let lastCheckpointTime' = lastCheckpointTime batchState
      trace = Trace.appendName "checkpoint" trace0

  now :: UTCTime <- getCurrentTime
  checkpointRequested :: Bool <- Signal.resetGet checkpointSignal
  let persistAndCheckpoint' :: [[Event a]] -> Doc () -> IO (BatchState a)
      persistAndCheckpoint' bufferedEvents' msg = do
        let (slotNo, bhh) = slotNoBhh
        traceInfo trace $ "Persisting and checkpointing at " <+> pretty (C.ChainPoint slotNo bhh) <+> " because " <+> msg
        persistMany indexerRuntime (concat $ reverse bufferedEvents')
        checkpoint indexerRuntime indexerState slotNoBhh
        return $ freshBatchState now

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
               | otherwise                -> return BatchState
                   { lastCheckpointTime = lastCheckpointTime'
                   , indexerState
                   , slotNoBhh
                   , batchFill = bufferFill'
                   , bufferedEvents = bufferedAndNewEvents
                   }

     -- There are no new events
     | otherwise -> if
         -- .. but it's checkpoint time so we persist events and checkpoint
         | isCheckpointTime
         , (_ : _) <- bufferedEvents -> persistAndCheckpoint' bufferedEvents "it's checkpoint time, but buffer is not full"
         -- Nothing to do, continue with original state
         | isCheckpointTime -> checkpoint indexerRuntime indexerState slotNoBhh $> freshBatchState now
         -- Nothing to do, continue with original state
         | otherwise -> return $ case batchState { indexerState } of
             BatchState{..} -> BatchState{slotNoBhh, .. }
             BatchEmpty{..} -> BatchEmpty{slotNoBhh, .. }
             NoProgress{..} -> NoProgress{chainPointCurrent = #chainPoint slotNoBhh, .. }
  where
    freshBatchState now = BatchEmpty { lastCheckpointTime = now, indexerState, slotNoBhh }

persistStepFinal :: Indexer a => Runtime a -> BatchState a -> Trace.Trace IO TS.Text -> IO (Maybe C.ChainPoint)
persistStepFinal indexerRuntime batchState trace = case batchState of
  BatchState{bufferedEvents, indexerState, slotNoBhh} -> do
    let cp = #chainPoint slotNoBhh :: C.ChainPoint
    persistMany indexerRuntime (concat bufferedEvents)
    checkpoint indexerRuntime indexerState slotNoBhh
    traceNotice trace $ "Exiting at" <+> prettySlot cp <> ", persisted and checkpointed."
    return $ Just cp
  BatchEmpty{slotNoBhh, indexerState} -> do
    let cp = #chainPoint slotNoBhh :: C.ChainPoint
    checkpoint indexerRuntime indexerState slotNoBhh
    traceNotice trace $ "Exiting at" <+> prettySlot cp <> ", checkpointed."
    return $ Just cp
  NoProgress{indexerState, chainPointCurrent} -> do
    case chainPointCurrent of
      C.ChainPoint slotNo bhh -> do
        checkpoint indexerRuntime indexerState (slotNo, bhh)
        traceNotice trace "No events found. Checkpointed and exiting."
        return $ Just chainPointCurrent
      C.ChainPointAtGenesis -> do
        traceNotice trace "No progress made, exiting."
        return Nothing
  where
    prettySlot cp = pretty (#slotNo cp :: C.SlotNo)

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

type RunHttpApi a = Runtime a -> IO.MVar (State a, BatchState a) -> IO ()

hookHttpApi
  :: Runtime a
  -> State a
  -> BatchState a
  -> RunHttpApi a
  -> BatchStepper a
  -> IO (BatchStepper a)
hookHttpApi indexerRuntime indexerInitialState initialBatchState runServer persistStep' = do
  mvar <- IO.newMVar (indexerInitialState, initialBatchState)
  _ <- IO.forkIO $ runServer indexerRuntime mvar -- start HTTP API server
  return $ \batchState tup@(_, _, indexerState) -> do
    _ <- IO.swapMVar mvar (indexerState, batchState) -- update mvar with indexer state and batch state
    persistStep' batchState tup

-- * Trace

stderrTrace :: CM.Severity -> (Trace.Trace IO TS.Text -> IO a) -> IO a
stderrTrace severity f = do
  c <- defaultConfigStderrSeverity severity
  Trace.withTrace c "mafoc" f

initialNotice :: Show a => a -> CommonConfig -> Maybe (ParallelismConfig, Natural, Int, Int) -> Trace.Trace IO TS.Text -> IO ()
initialNotice cli CommonConfig{batchSize, severity, checkpointInterval} maybeParallelism trace = do
  traceNotice trace
    $ "Indexer started\n  Configuration: "
    <> "\n    " <> pretty (show cli)
    <> "\n  Batch size" <+> pretty (show batchSize)
    <> "\n  Logging severity" <+> pretty (show severity)
    <> "\n  Checkpoint interval" <+> pretty (show checkpointInterval)
    <> case maybeParallelism of
         Just (ParallelismConfig{intervalLength}, intervalLengthSlots, maxThreads, numCapabilities) -> let
           text = case intervalLength of
                    Slots slots -> pretty (show slots {- same as `intervalLengthSlots` -}) <+> "slot batches per thread"
                    Percent percent -> pretty (show percent) <+> "% of interval, i.e" <+> pretty (show intervalLengthSlots) <+> "slot batches per thread"
           in "\n  Parallelism: processing" <+> text
           <> "\n  # of threads used:" <+> pretty maxThreads <+> ("(" <> pretty numCapabilities <> " reported by operating system)")
         Nothing -> "\n  No parallelism"

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
  , profiling            :: Maybe Logging.ProfilingRuntime
  , pipelineSize         :: Word32

  -- * Internal
  , concurrencyPrimitive :: ConcurrencyPrimitive
  , maybeHeaderDb        :: Maybe HeaderDb
  }

ensureStartFromCheckpoint :: LocalChainsyncRuntime -> C.ChainPoint -> IO LocalChainsyncRuntime
ensureStartFromCheckpoint lcr@LocalChainsyncRuntime{interval = (requestedStartingPoint, upTo)} stateCp = if requestedStartingPoint <= stateCp
  then return $ lcr { interval = (stateCp, upTo) }
  else E.throwIO $ E.No_state_for_requested_starting_point
       { E.requested = requestedStartingPoint
       , E.fromState = stateCp
       }

-- | Use either the existing chain point provided in the runtime, or the argument if it's later.
useLaterStartingPoint :: LocalChainsyncRuntime -> C.ChainPoint -> LocalChainsyncRuntime
useLaterStartingPoint lcr cp = lcr { interval = (max oldCp cp, upTo) }
  where (oldCp, upTo) = interval lcr

initializeLocalChainsync :: LocalChainsyncConfig a -> C.NetworkId  -> Trace.Trace IO TS.Text -> String -> IO LocalChainsyncRuntime
initializeLocalChainsync localChainsyncConfig networkId trace cliShow = do
  let SocketPath socketPath' = #socketPath localChainsyncConfig
      localNodeCon = CS.mkLocalNodeConnectInfo networkId socketPath'
  securityParam' <- querySecurityParam localNodeCon

  -- Resolve any bare SlotNo to ChainPoint
  let (Interval from upTo, maybeDbPathAndTableName) = intervalInfo localChainsyncConfig
  (cliChainPoint, maybeHeaderDb) <- intervalStartToChainSyncStart trace maybeDbPathAndTableName from

  -- Init profiling
  maybeProfiling :: Maybe Logging.ProfilingRuntime <- traverse (Logging.profilerInit trace cliShow) (profiling_ localChainsyncConfig)

  return $ LocalChainsyncRuntime
    localNodeCon
    (cliChainPoint, upTo)
    securityParam'
    (logging_ localChainsyncConfig)
    maybeProfiling
    (pipelineSize_ localChainsyncConfig)
    (concurrencyPrimitive_ localChainsyncConfig)
    maybeHeaderDb

-- | Resolve @LocalChainsyncConfig@ that came from e.g command line
-- arguments into an "actionable" @LocalChainsyncRuntime@ runtime
-- config which can be used to generate a stream of blocks.
initializeLocalChainsync_ :: LocalChainsyncConfig_ -> Trace.Trace IO TS.Text -> String -> IO LocalChainsyncRuntime
initializeLocalChainsync_ config trace cliShow = do
  networkId <- #getNetworkId config
  initializeLocalChainsync config networkId trace cliShow

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
    takeWhile' predicate = S.takeWhile predicate . Signal.takeWhileStopSignal stopSignal

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

fromShow :: (Show a, IsString b) => a -> b
fromShow = fromString . show

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

instance Pretty DbPathAndTableName where
  pretty (DbPathAndTableName maybeDbFilePath maybeTable) = pretty $ db <> ":" <> table
    where
      db = fromMaybe "default.db" maybeDbFilePath
      table = fromMaybe "default" maybeTable

defaultTableName :: String -> DbPathAndTableName -> (FilePath, String)
defaultTableName defaultName (DbPathAndTableName maybeDbPath maybeName) = (fromMaybe "default.db" maybeDbPath, fromMaybe defaultName maybeName)

-- ** Header DB

type HeaderDb = (SQL.Connection, String)

openHeaderDb :: DbPathAndTableName -> IO HeaderDb
openHeaderDb dbPathAndTableName = let
  (dbPath, tableName) = defaultTableName "blockbasics" dbPathAndTableName
  in SQL.open dbPath <&> (, tableName)

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

-- ** Initialize

-- | Adjust @LocalChainsyncRuntime@ with chekpointed chainpoint if it
-- is later than what was requested from the CLI.
useSqliteCheckpoint :: FilePath -> String -> Trace.Trace IO TS.Text -> LocalChainsyncRuntime -> IO (SQL.Connection, LocalChainsyncRuntime)
useSqliteCheckpoint dbPath tableName trace chainsyncRuntime = do
  sqlCon <- sqliteOpen dbPath
  sqliteInitCheckpoints sqlCon
  chainsyncRuntime' <- getCheckpointSqlite sqlCon tableName >>= \case
    Just existingCheckpoint -> let (requested, upTo) = interval chainsyncRuntime
      in if requested > existingCheckpoint
      then do
      traceInfo trace
        $ "Found chekpoint at" <+> pretty existingCheckpoint
        <+> "but using chainpoint requested from CLI because it's later" <+> pretty requested
      pure $ chainsyncRuntime { interval = (requested, upTo) }
      else do
      traceInfo trace
        $ "Found chekpoint at" <+> pretty existingCheckpoint
        <+> ", using that instead of the one requested from CLI" <+> pretty requested
        -- todo use exclamation mark to force
      pure $ chainsyncRuntime { interval = (existingCheckpoint, upTo) }
    Nothing -> pure chainsyncRuntime
  return (sqlCon, chainsyncRuntime')

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

-- | Resolve @SlotNo@ to @ChainPoint@ by finding block header hash from an Sqlite table
chainPointForSlotNo :: HeaderDb -> C.SlotNo -> IO (Maybe C.ChainPoint)
chainPointForSlotNo (sqlCon, tableName) slotNo = fmap eventsToSingleChainpoint $ SQL.query sqlCon query $ SQL.Only slotNo
  where
    query :: SQL.Query
    query = "SELECT slot_no, block_header_hash     \
            \  FROM " <> fromString tableName <> " \
            \ WHERE slot_no = ?                    "

chainPointNearSlotNo :: HeaderDb -> SQL.Query -> SQL.Query -> C.SlotNo -> IO (Maybe C.ChainPoint)
chainPointNearSlotNo (sqlCon, tableName) op order slotNo = fmap eventsToSingleChainpoint $ SQL.query sqlCon (mkChainPointSlotNoQuery tableName op order) $ SQL.Only slotNo

mkChainPointSlotNoQuery :: String -> SQL.Query -> SQL.Query -> SQL.Query
mkChainPointSlotNoQuery tableName op order =
  "SELECT slot_no, block_header_hash     \
  \  FROM " <> fromString tableName <> " \
  \ WHERE slot_no " <> op <> " ?         \
  \ ORDER BY slot_no " <> order <> "     \
  \ LIMIT 1                              "

previousChainPointForSlotNo :: SQL.Connection -> String -> C.SlotNo -> IO (Maybe C.ChainPoint)
previousChainPointForSlotNo sqlCon tableName slotNo = getSecond <$> query1 sqlCon query slotNo
  where
    query :: SQL.Query
    query = "SELECT slot_no, block_header_hash     \
            \  FROM " <> fromString tableName <> " \
            \ WHERE slot_no <= ?                   \
            \ ORDER BY slot_no DESC                \
            \ LIMIT 2                              "

    getSecond = \case
      [_, x] -> Just x
      _ -> Nothing

-- | Convert starting point from CLI to chainpoint, possibly with the help of header DB.
intervalStartToChainSyncStart
  :: Trace.Trace IO TS.Text
  -> Maybe DbPathAndTableName
  -> (Bool, Either C.SlotNo C.ChainPoint)
  -> IO (C.ChainPoint, Maybe HeaderDb)
intervalStartToChainSyncStart trace maybeDbPathAndTableName (include, eitherSlotOrCp)
  -- If a direct ChainPoint is given, return that:
  | Right cp <- eitherSlotOrCp = (cp,) <$> traverse openHeaderDb maybeDbPathAndTableName
  -- If only the SlotNo is given, resolve it to ChainPoint by help of a database
  | Just dbPathAndTableName <- maybeDbPathAndTableName, Left slotNo <- eitherSlotOrCp = do
    headerDb@(sqlCon, tableName) <- openHeaderDb dbPathAndTableName
    if include
      then let
        in case slotNo of
             0 -> pure (C.ChainPointAtGenesis, Just headerDb)
             _slotNo -> do
               maybeChainPoint <- previousChainPointForSlotNo sqlCon tableName slotNo
               case maybeChainPoint of
                 Just cp -> pure (cp, Just headerDb)
                 Nothing -> E.throwIO $ E.Can't_find_previous_ChainPoint_to_slot slotNo

      else chainPointForSlotNo (sqlCon, tableName) slotNo >>= \case
        Nothing -> E.throwIO $ E.SlotNo_not_found slotNo
        Just cp -> do
          traceInfo trace $ pretty slotNo <> " resolved to " <> pretty cp
          pure (cp, Just headerDb)

  | otherwise = E.throwIO $ E.No_headerDb_specified "Interval start was specified as slot number, to resolve this to block header hash a block header database is needed."

-- * Address filter

mkMaybeAddressFilter :: [C.Address C.ShelleyAddr] -> Maybe (C.Address C.ShelleyAddr -> Bool)
mkMaybeAddressFilter addresses = case addresses of
  [] -> Nothing
  _ -> Just $ \address -> address `elem` Set.fromList addresses

-- * Generic

slotText :: C.ChainPoint -> TS.Text
slotText cp = TS.pack $ show (coerce (#slotNo cp :: C.SlotNo) :: Word64)
