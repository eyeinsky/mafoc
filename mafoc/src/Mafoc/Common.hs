{-# LANGUAGE LambdaCase #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Common where

import Data.Coerce (coerce)
import Data.Word (Word64)
import Numeric.Natural (Natural)

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS

-- * Additions to cardano-api

type Block = C.BlockInMode C.CardanoMode
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

tipDistance :: C.ChainPoint -> C.ChainTip -> Natural
tipDistance C.ChainPoint{} C.ChainTipAtGenesis = error "This should never happen"
tipDistance cp ct = let
  cp' = case cp of
    C.ChainPointAtGenesis -> 0
    C.ChainPoint slotNo _ -> slotNo
  ct' = case ct of
    C.ChainTipAtGenesis   -> 0
    C.ChainTip slotNo _ _ -> slotNo
  d = ct' - cp'
  in fromIntegral (coerce d :: Word64) :: Natural

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

blockSlotNoBhh :: C.BlockInMode mode -> SlotNoBhh
blockSlotNoBhh (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = (slotNo, hash)

blockSlotNo :: C.BlockInMode mode -> C.SlotNo
blockSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) _) _) = slotNo

chainPointSlotNo :: C.ChainPoint -> C.SlotNo
chainPointSlotNo = \case
  C.ChainPoint slotNo _ -> slotNo
  C.ChainPointAtGenesis -> C.SlotNo 0
