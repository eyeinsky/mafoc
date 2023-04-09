{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Common where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Coerce (coerce)
import Data.Word (Word64)
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS

-- * Additions to cardano-api

type SlotNoBhh = (C.SlotNo, C.Hash C.BlockHeader)

getSecurityParamAndNetworkId :: FilePath -> IO (Natural, C.NetworkId)
getSecurityParamAndNetworkId nodeConfig = do
  (env :: C.Env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure (fromIntegral $ C.envSecurityParam env, CS.envNetworkId env)

instance Ord C.ChainTip where
  compare C.ChainTipAtGenesis C.ChainTipAtGenesis = EQ
  compare C.ChainTipAtGenesis _                   = LT
  compare _ C.ChainTipAtGenesis                   = GT
  compare (C.ChainTip a _ _) (C.ChainTip b _ _)   = compare a b

tipDistance :: C.BlockInMode mode -> C.ChainTip -> Natural
tipDistance blk ct = let
  blockNoToNatural :: C.BlockNo -> Natural
  blockNoToNatural = fromIntegral . coerce @_ @Word64
  tipBlockNo = case ct of
    C.ChainTipAtGenesis     -> 0
    C.ChainTip _ _ blockNo' -> blockNo'
  in blockNoToNatural tipBlockNo - blockNoToNatural (blockNo blk)

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

blockNo :: C.BlockInMode mode -> C.BlockNo
blockNo (C.BlockInMode (C.Block (C.BlockHeader _slotNo _bh blockNo') _) _) = blockNo'

blockSlotNoBhh :: C.BlockInMode mode -> SlotNoBhh
blockSlotNoBhh (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = (slotNo, hash)

blockSlotNo :: C.BlockInMode mode -> C.SlotNo
blockSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) _) _) = slotNo

chainPointSlotNo :: C.ChainPoint -> C.SlotNo
chainPointSlotNo = \case
  C.ChainPoint slotNo _ -> slotNo
  C.ChainPointAtGenesis -> C.SlotNo 0

-- * Streaming

streamPassReturn
  :: Monad m
  => S.Stream (S.Of a) m r
  -> (a -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r)
  -> S.Stream (S.Of b) m r
streamPassReturn source f = lift (S.next source) >>= \case
  Left r                 -> pure r
  Right (event, source') -> f event source'

-- | Consume a stream @source@ in a loop and run effect @f@ on it.
loopM :: (MonadTrans t1, Monad m, Monad (t1 m)) => S.Stream (S.Of t2) m b -> (t2 -> t1 m a) -> t1 m b
loopM source f = loop source
  where
    loop source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (event, source'') -> do
        _ <- f event
        loop source''

-- | Fold a stream of @a@'s, yield a stream of @b@s, while keeping a state of @st".
foldYield :: Monad m => (st -> a -> m (st, b)) -> st -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r
foldYield f st source = loop st source
  where
    loop st' source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (e, source'') -> do
        (st'', e') <- lift $ f st' e
        S.yield e'
        loop st'' source''

-- * Base

-- in base since: base-4.8.0.0
minusNaturalMaybe :: Natural -> Natural -> Maybe Natural
minusNaturalMaybe a b
  | a < b = Nothing
  | otherwise = Just (a - b)
