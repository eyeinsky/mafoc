{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Mafoc.Logging where

import Cardano.Api (
  Block (Block),
  BlockHeader (BlockHeader),
  BlockInMode (BlockInMode),
  CardanoMode,
  ChainPoint (ChainPoint),
  ChainTip (ChainTip),
  SlotNo (SlotNo),
 )
import Cardano.BM.Trace qualified as CM
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.Text qualified as TS
import Data.Time (NominalDiffTime, UTCTime, defaultTimeLocale, diffUTCTime, formatTime, getCurrentTime)
import Prettyprinter (Pretty (pretty), defaultLayoutOptions, layoutPretty, (<+>), Doc)

import Prettyprinter.Render.Text (renderStrict)
import Streaming (Of, Stream, effect)
import Streaming.Prelude qualified as S
import Text.Printf (printf)

import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Marconi.ChainIndex.Orphans ()

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
  , syncStatsLastMessage :: !(Maybe UTCTime)
  -- ^ Timestamp of last printed message
  }

logging
  :: CM.Trace IO TS.Text
  -> Stream (Of (ChainSyncEvent (BlockInMode CardanoMode))) IO r
  -> Stream (Of (ChainSyncEvent (BlockInMode CardanoMode))) IO r
logging tracer s = effect $ do
  stats <- newIORef (SyncStats 0 0 Nothing)
  return $ S.chain (update stats) s
  where
    minSecondsBetweenMsg :: NominalDiffTime
    minSecondsBetweenMsg = 10

    update :: IORef SyncStats -> ChainSyncEvent (BlockInMode CardanoMode) -> IO ()
    update statsRef (RollForward bim ct) = do
      let cp = case bim of (BlockInMode (Block (BlockHeader slotNo hash _blockNo) _txs) _eim) -> ChainPoint slotNo hash
      modifyIORef' statsRef $ \stats ->
        stats{syncStatsNumBlocks = syncStatsNumBlocks stats + 1}
      printMessage statsRef cp ct
    update statsRef (RollBackward cp ct) = do
      modifyIORef' statsRef $ \stats ->
        stats{syncStatsNumRollbacks = syncStatsNumRollbacks stats + 1}
      printMessage statsRef cp ct

    printMessage statsRef cp ct = do
      SyncStats{syncStatsNumBlocks, syncStatsNumRollbacks, syncStatsLastMessage} <- readIORef statsRef

      now <- getCurrentTime

      let timeSinceLastMsg = diffUTCTime now <$> syncStatsLastMessage

      let blocksMsg = case timeSinceLastMsg of
            Nothing -> id
            Just t -> \k ->
              "Processed"
                <+> pretty syncStatsNumBlocks
                <+> "blocks in the last"
                <+> pretty (formatTime defaultTimeLocale "%s" t)
                <+> "seconds"
                <+> let rate = fromIntegral syncStatsNumBlocks / realToFrac t :: Double
                     in pretty (printf "(%.0f blocks/sec)." rate :: String)
                          <+> k

      let rollbackMsg = case timeSinceLastMsg of
            Nothing -> id
            Just t -> \k ->
              ( case syncStatsNumRollbacks of
                  0 -> "No"
                  _ -> pretty syncStatsNumRollbacks
              )
                <+> "rollbacks in the last"
                <+> pretty (formatTime defaultTimeLocale "%s" t)
                <+> "seconds."
                <+> k

      let syncMsg = case (cp, ct) of
            (ChainPoint (SlotNo chainPointSlot) _, ChainTip (SlotNo chainTipSlot) _header _blockNo)
              -- TODO: MAGIC number here. Is there a better number?
              -- 100 represents the number of slots before the
              -- node where we consider the chain-index to be synced.
              | chainTipSlot - chainPointSlot < 100 ->
                  "Synchronised."
            (ChainPoint (SlotNo chainPointSlotNo) _, ChainTip (SlotNo chainTipSlotNo) _header _blockNo) ->
              let pct = (100 :: Double) * fromIntegral chainPointSlotNo / fromIntegral chainTipSlotNo
               in pretty
                    ( printf
                        "Synchronising. Current slot %d out of %d (%0.2f%%)."
                        chainPointSlotNo
                        chainTipSlotNo
                        pct
                      :: String
                    )
            _ -> "Starting."

      let shouldPrint = case timeSinceLastMsg of
            Nothing -> True
            Just t
              | t > minSecondsBetweenMsg -> True
              | otherwise -> False

      when shouldPrint $ do
        CM.logInfo tracer $
          renderStrict $
            layoutPretty defaultLayoutOptions $
              syncMsg <+> (blocksMsg $ rollbackMsg $ "Last block processed" <+> pretty cp <> ".")
        modifyIORef' statsRef $ \stats ->
          stats
            { syncStatsNumBlocks = 0
            , syncStatsNumRollbacks = 0
            , syncStatsLastMessage = Just now
            }
