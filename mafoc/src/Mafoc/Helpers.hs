{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Helpers where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Numeric.Natural (Natural)
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Mafoc.RollbackRingBuffer (Event (RollBackward, RollForward))
import Marconi.ChainIndex.Indexers.MintBurn ()

-- * Additions to cardano-api

getSecurityParam :: FilePath -> IO Natural
getSecurityParam nodeConfig = do
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure $ fromIntegral $ C.envSecurityParam env

-- | Convert event from @ChainSyncEvent@ to @Event@.
fromChainSyncEvent
  :: Monad m
  => S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) m r
  -> S.Stream (S.Of (Event (C.BlockInMode mode) (C.ChainPoint, C.ChainTip))) m r
fromChainSyncEvent = S.map $ \e -> case e of
  CS.RollForward a ct   -> RollForward a (blockChainPoint a, ct)
  CS.RollBackward cp ct -> RollBackward (cp, ct)

instance Ord C.ChainTip where
  compare C.ChainTipAtGenesis C.ChainTipAtGenesis = EQ
  compare C.ChainTipAtGenesis _                   = LT
  compare _ C.ChainTipAtGenesis                   = GT
  compare (C.ChainTip a _ _) (C.ChainTip b _ _)   = compare a b

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

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

-- | Helper to create loops
streamFold :: Monad m => (a -> b -> m b) -> b -> S.Stream (S.Of a) m r -> S.Stream (S.Of a) m r
streamFold f acc_ source_ = loop acc_ source_
  where
    loop acc source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        acc' <- lift $ f e acc
        S.yield e
        loop acc' source'
