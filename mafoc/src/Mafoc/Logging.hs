{-# OPTIONS_GHC -Wno-orphans #-}

module Mafoc.Logging where

import Control.Concurrent qualified as IO
import System.IO qualified as IO
import System.Environment qualified as IO
import System.Process qualified as IO
import Data.ByteString.Lazy qualified as BS
import Data.ByteString.Lazy.Char8 qualified as BS8
import Prettyprinter (Pretty (pretty), defaultLayoutOptions, layoutPretty, (<+>), Doc)

import Cardano.Api qualified as C
import Cardano.BM.Trace qualified as CM
import Data.Text qualified as TS
import Data.Time (NominalDiffTime, UTCTime, defaultTimeLocale, diffUTCTime, formatTime, getCurrentTime)
import GHC.Stats qualified as Stats
import System.Posix.Process qualified as Posix
import Data.Aeson qualified as A
import Data.Aeson.KeyMap qualified as A
import Data.Aeson ((.=))

import Prettyprinter.Render.Text (renderStrict)
import Streaming.Prelude qualified as S
import Text.Printf (printf)

import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))

import Mafoc.Upstream (foldYield)
import Mafoc.Upstream.Orphans ()
import Mafoc.Signal qualified as Signal
import Mafoc.Exceptions qualified as E

-- * Trace severity levels

traceEmerg :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceEmerg = mkDocLog CM.logEmergency

traceCritical :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceCritical = mkDocLog CM.logCritical

traceAlert :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceAlert = mkDocLog CM.logAlert

traceError :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceError = mkDocLog CM.logError

traceWarning :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceWarning = mkDocLog CM.logWarning

traceNotice :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceNotice = mkDocLog CM.logNotice

traceInfo :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceInfo = mkDocLog CM.logInfo

traceDebug :: CM.Trace IO TS.Text -> Doc () -> IO ()
traceDebug = mkDocLog CM.logDebug

mkDocLog :: (CM.Trace IO TS.Text -> TS.Text -> IO ()) -> CM.Trace IO TS.Text -> Doc ann -> IO ()
mkDocLog logger trace msg = logger trace $ renderDefault msg

renderPretty :: Pretty a => a -> TS.Text
renderPretty = renderDefault . pretty

renderDefault :: Doc a -> TS.Text
renderDefault = renderStrict . layoutPretty defaultLayoutOptions

-- * Profiling

data ProfilingConfig = ProfilingConfig
  { sampleRate_ :: NominalDiffTime
  , destination :: FilePath
  , comment :: String
  } deriving (Show)

data ProfilingRuntime = ProfilingRuntime
  { sampleRate :: NominalDiffTime
  , handle :: IO.Handle
  , pid :: IO.Pid
  } deriving (Show)

getHandle :: String -> (Doc () -> IO ()) -> IO IO.Handle
getHandle outStr logMsg = do
  handle <- case outStr of
    "-" -> do
      logMsg "stdout"
      return IO.stdout
    filePath -> do
      logMsg $ "file '" <> pretty filePath <> "'"
      IO.openFile filePath IO.WriteMode
  IO.hSetBuffering handle IO.LineBuffering $> handle

profilerInit :: CM.Trace IO TS.Text -> String -> ProfilingConfig -> IO ProfilingRuntime
profilerInit trace showedCli (ProfilingConfig sampleRate outStr comment) = do
  let logMsg dest = traceNotice trace $ "Profiling enabled: using " <> dest <> " for output, sample rate " <> pretty (show sampleRate)
  ensureRtsStats
  handle <- getHandle outStr logMsg
  pid <- Posix.getProcessID
  startEvent <- getStartEvent showedCli pid
  hPutLn handle $ A.encode $ startEvent <> A.fromList ["comment" .= comment]
  return $ ProfilingRuntime sampleRate handle pid

-- * API

profileStep
  :: ProfilingRuntime
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
profileStep ProfilingRuntime{sampleRate, handle, pid} source = do
  initialSyncStats <- lift $ mkEmptyStats <$> getCurrentTime
  foldYield step initialSyncStats source
  where
    step syncStats0 ev = do
      let before = syncStatsLastMessage syncStats0
          (cp, _ct, syncStats1) = syncStatsStep syncStats0 ev

      now <- getCurrentTime
      let timeSinceLastSample = diffUTCTime now before
      if timeSinceLastSample > sampleRate
        then do
          rts <- getLiveBytes
          pt <- getProcessTimesKm
          rss <- getRss pid
          let interval = A.fromList [ "t0" .= before, "t1" .= now ]
              cp' = A.fromList [ "chainPoint" .= cp ]
          hPutLn handle $ A.encode $ A.Object $ mconcat [syncStatsKv syncStats1, rts, rss, pt, interval, cp']
          pure (mkEmptyStats now, ev)
        else return (syncStats1, ev)

profilerEnd :: ProfilingRuntime -> Maybe C.ChainPoint -> IO ()
profilerEnd ProfilingRuntime{handle, pid} maybeCp = do
  resources <- getResourcesEvent pid
  hPutLn handle $ A.encode $ A.Object $ resources <> A.fromList [ "chainPoint" .= maybeCp ]

-- * Get event data

getResourcesEvent :: IO.Pid -> IO (A.KeyMap A.Value)
getResourcesEvent pid = do
  now <- getCurrentTime
  pt <- getProcessTimesKm
  rss <- getRss pid
  let timing = A.fromList [ "t" .= now ]
  return $ mconcat [timing, pt, rss]

getStartEvent :: String -> IO.Pid -> IO (A.KeyMap A.Value)
getStartEvent showedCli pid = do
  now <- getCurrentTime
  args <- IO.getArgs
  pt <- getProcessTimesKm
  rss <- getRss pid
  let cli = A.fromList [ "cli" .= A.object [ "show" .= showedCli, "raw" .= args ] ]
      timing = A.fromList [ "t" .= now ]
      system = A.fromList [ "threaded" .= IO.rtsSupportsBoundThreads ]
  return $ mconcat [cli, timing, system, pt, rss]

getProcessTimesKm :: IO (A.KeyMap A.Value)
getProcessTimesKm = do
  t <- Posix.getProcessTimes
  return $ A.fromList
    [ "processTime" .= A.object
      [ "elapsed" .= fromEnum (Posix.elapsedTime t)
      , "system" .= fromEnum (Posix.systemTime t)
      , "user" .= fromEnum (Posix.userTime t) ]]

getRss :: IO.Pid -> IO (A.KeyMap A.Value)
getRss pid = do
  str <- IO.readProcess "ps" ["-p", show pid, "-o", "rss", "--no-headers"] []
  case readMaybe str of
    Just (kb :: Int) -> return $ A.fromList ["rss" .= kb]
    Nothing -> E.throwIO $ E.The_impossible_happened $ "The command 'ps' should be available and the resident set size (memory size) for the PID " <> TS.pack (show pid) <> " should exist."

getLiveBytes :: IO (A.KeyMap A.Value)
getLiveBytes = do
  rts <- Stats.getRTSStats
  return $ A.fromList [ "max_live_bytes" .= Stats.max_live_bytes rts ]

syncStatsKv :: SyncStats -> A.KeyMap A.Value
syncStatsKv syncStats = A.fromList
  [ "blocks" .= syncStatsNumBlocks syncStats
  , "rollbacks" .= syncStatsNumRollbacks syncStats
  ]

-- ** Helpers

ensureRtsStats :: IO ()
ensureRtsStats = do
  rtsStatsDisabled <- not <$> Stats.getRTSStatsEnabled
  when rtsStatsDisabled $ E.throwIO E.RTS_GC_stats_not_enabled

bs8show :: Show a => a -> BS.ByteString
bs8show a = BS8.pack (show a)

hPutLn :: IO.Handle -> BS.ByteString -> IO ()
hPutLn h bs = BS.hPut h $ bs <> "\n"

-- * Log sync stats

data SyncStats = SyncStats
  { syncStatsNumBlocks :: !Int
  -- ^ Number of applied blocks since last message
  , syncStatsNumRollbacks :: !Int
  -- ^ Number of rollbacks
  , syncStatsLastMessage :: !UTCTime
  -- ^ Timestamp of last printed message
  }

mkEmptyStats :: UTCTime -> SyncStats
mkEmptyStats time = SyncStats
  { syncStatsNumBlocks = 0
  , syncStatsNumRollbacks = 0
  , syncStatsLastMessage = time
  }

logging
  :: CM.Trace IO TS.Text
  -> Signal.ChainsyncStats
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
logging trace0 statsSignal source = do
  now <- lift getCurrentTime
  foldYield step (mkEmptyStats now) source
  where
    trace = CM.appendName "chainsync" trace0
    step syncStats0 ev = do
      now <- getCurrentTime
      statsRequested <- Signal.resetGet statsSignal
      let timeSinceLastMsg = diffUTCTime now $ syncStatsLastMessage syncStats0
          (cp, ct, syncStats1) = syncStatsStep syncStats0 ev
          minSecondsBetweenMsg = 10 :: NominalDiffTime
          logMsg = mkMessage syncStats1 timeSinceLastMsg cp ct

      if | statsRequested -> traceAlert trace logMsg $> (mkEmptyStats now, ev)
         | timeSinceLastMsg > minSecondsBetweenMsg -> traceInfo trace logMsg $> (mkEmptyStats now, ev)
         | otherwise -> return (syncStats1, ev)

syncStatsStep :: SyncStats -> ChainSyncEvent (C.BlockInMode C.CardanoMode) -> (C.ChainPoint, C.ChainTip, SyncStats)
syncStatsStep syncStats0 ev = case ev of
  RollForward bim ct' -> (#chainPoint bim, ct', syncStats0 { syncStatsNumBlocks = syncStatsNumBlocks syncStats0 + 1 })
  RollBackward cp' ct' -> (cp', ct', syncStats0 { syncStatsNumRollbacks = syncStatsNumRollbacks syncStats0 + 1 })

mkMessage :: SyncStats -> NominalDiffTime -> C.ChainPoint -> C.ChainTip -> Doc a
mkMessage syncStats timeSinceLastMsg cp ct  = syncStatus cp ct
  <+> reportBlocks (syncStatsNumBlocks syncStats) timeSinceLastMsg
  <+> reportRollbacks (syncStatsNumRollbacks syncStats) timeSinceLastMsg
  <+> "Last block processed" <+> pretty cp <> "."

reportBlocks :: Int -> NominalDiffTime -> Doc ann
reportBlocks numBlocks t = "Processed" <+> pretty numBlocks <+> "blocks in the last" <+> seconds <+> "seconds at " <+> rate'
  where
    seconds = pretty (formatTime defaultTimeLocale "%s" t)
    rate = fromIntegral numBlocks / realToFrac t :: Double
    rate' = pretty (printf "%.0f blocks/sec." rate :: String)

reportRollbacks :: Int -> NominalDiffTime -> Doc ann
reportRollbacks numRollbacks t = num <+> "rollbacks in the last" <+> seconds <+> "seconds."
  where
    num = if numRollbacks == 0 then "No" else pretty numRollbacks
    seconds = pretty (formatTime defaultTimeLocale "%s" t)

syncStatus :: C.ChainPoint -> C.ChainTip -> Doc a
syncStatus cp ct = let
  cpn = coerce (#slotNo cp :: C.SlotNo) :: Word64
  ctn = coerce (#slotNo ct :: C.SlotNo) :: Word64
  in if
  | C.ChainPoint{} <- cp, C.ChainTip{} <- ct -> if ctn - cpn < 100
    then "Synchronised."
    else let pct = (100 :: Double) * fromIntegral cpn / fromIntegral ctn
      in pretty (printf "Synchronising. Current slot %d out of %d (%0.2f%%)." cpn ctn pct :: String)
  | otherwise -> "Starting."
  -- TODO: MAGIC number here. Is there a better number?
  -- 100 represents the number of slots before the
  -- node where we consider the chain-index to be synced.
