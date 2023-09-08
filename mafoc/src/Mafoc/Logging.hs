{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Mafoc.Logging where

import Cardano.Api qualified as C
import Cardano.BM.Trace qualified as CM
import Data.Text qualified as TS
import Data.Time (NominalDiffTime, UTCTime, defaultTimeLocale, diffUTCTime, formatTime, getCurrentTime)
import Prettyprinter (Pretty (pretty), defaultLayoutOptions, layoutPretty, (<+>), Doc)

import Prettyprinter.Render.Text (renderStrict)
import Streaming.Prelude qualified as S
import Text.Printf (printf)

import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Marconi.ChainIndex.Orphans ()

import Mafoc.Upstream (foldYield)
import Mafoc.Signal qualified as Signal

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
logging tracer statsSignal source = do
  now <- lift getCurrentTime
  foldYield step (mkEmptyStats now) source
  where
    step syncStats0 ev = do
      now <- getCurrentTime
      statsRequested <- Signal.resetGet statsSignal
      let timeSinceLastMsg = diffUTCTime now $ syncStatsLastMessage syncStats0
          (cp, ct, syncStats1) = case ev of
            RollForward bim ct' -> (#chainPoint bim, ct', syncStats0 { syncStatsNumBlocks = syncStatsNumBlocks syncStats0 + 1 })
            RollBackward cp' ct' -> (cp', ct', syncStats0 { syncStatsNumRollbacks = syncStatsNumRollbacks syncStats0 + 1 })

          minSecondsBetweenMsg = 10 :: NominalDiffTime

          logMsg = mkMessage syncStats1 timeSinceLastMsg cp ct

      if | statsRequested -> traceAlert tracer logMsg $> (mkEmptyStats now, ev)
         | timeSinceLastMsg > minSecondsBetweenMsg -> traceInfo tracer logMsg $> (mkEmptyStats now, ev)
         | otherwise -> return (syncStats1, ev)

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
