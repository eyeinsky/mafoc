{-# LANGUAGE BlockArguments   #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE MultiWayIf       #-}
module Mafoc.RollbackRingBuffer where

import Control.Exception qualified as IO
import Control.Monad.IO.Class (liftIO)
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming.Helpers qualified as CS
import Mafoc.Common (SlotNoBhh, streamPassReturn)
import Mafoc.RingBuffer qualified as RB

-- The difference between a ring buffer and an elastic ring buffer is
-- that elastic ring buffer always yields blocks that are further than
-- security parameter away from the chain tip -- regardless of whether
-- buffer is full or not.
rollbackRingBuffer
  :: forall a r
   . Natural
  -> (a -> C.ChainTip -> Natural)
  -> (a -> SlotNoBhh)
  -> S.Stream (S.Of (CS.ChainSyncEvent a)) IO r -> S.Stream (S.Of a) IO r
rollbackRingBuffer 0 _ _ source = S.mapM f source
  where
    f = \case
      CS.RollForward event _ct -> return event
      -- Zero-sized buffer can't handle rollbacks, thus always throw.
      CS.RollBackward cp ct    -> IO.throwIO $ CS.RollbackLocationNotFound cp ct
rollbackRingBuffer securityParam eventTipDistance eventSlotNoBhh source = liftIO (RB.new securityParam) >>= loop source
  where
    loop source' rb = streamPassReturn source' $ \chainSyncEvent source'' -> do
      loop source'' =<< case chainSyncEvent of
        CS.RollForward event ct -> case eventTipDistance event ct `compare` securityParam of
          -- Block is closer to tip than security param
          LT  -> do
            (rb', as) <- liftIO $ RB.flushWhile (\e -> eventTipDistance e ct >= securityParam) rb
            mapM_ S.yield as
            (rb'', ma) <- liftIO $ RB.push event rb'
            maybe (return ()) S.yield ma
            return rb''

          -- Chain tip is securityParam-or-more away from event, yield buffer then current block
          _ -> do
            (rb', events) <- liftIO $ RB.flush rb -- todo: don't build intermediate list
            mapM_ S.yield events
            S.yield event
            return $ RB.reset rb'

        CS.RollBackward cp ct -> case cp of
          C.ChainPointAtGenesis   -> return $ RB.reset rb
          C.ChainPoint slotNo bhh -> do
            -- If we unpushWhile all newer elements than rollback
            -- point and if the buffer still has events then the
            -- now-newest event in the buffer must be the rollback
            -- point.
            rb' <- liftIO $ RB.unpushWhile_ (\event' -> eventSlotNoBhh event' /= (slotNo, bhh)) rb
            case RB.ringBufferFill rb' of
              0 -> liftIO $ IO.throwIO $ CS.RollbackLocationNotFound cp ct
              _ -> return rb'
