{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Common where

import Control.Exception (throwIO, Exception)
import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Coerce (coerce)
import Data.Word (Word64)
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Types qualified as Marconi
import Marconi.ChainIndex.Utils qualified as Marconi
import Ouroboros.Consensus.HardFork.Combinator.AcrossEras qualified as O

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

-- ** Query node

deriving instance Exception C.AcquiringFailure
deriving instance Exception O.EraMismatch
newtype UnspecifiedException = UnspecifiedException String deriving Show
instance Exception UnspecifiedException

-- | Query the current era of the local node's current state.
queryCurrentEra :: C.LocalNodeConnectInfo C.CardanoMode -> IO C.AnyCardanoEra
queryCurrentEra localNodeConnectInfo =
 C.queryNodeLocalState localNodeConnectInfo Nothing queryInMode >>= \case
  Left acquiringFailure -> throwIO acquiringFailure
  Right anyCardanoEra -> return anyCardanoEra

 where
  queryInMode :: C.QueryInMode C.CardanoMode C.AnyCardanoEra
  queryInMode = C.QueryCurrentEra C.CardanoModeIsMultiEra

-- | Query security param from the local node given a Shelley based era.
querySecurityParamEra :: C.LocalNodeConnectInfo C.CardanoMode -> C.ShelleyBasedEra era -> IO Marconi.SecurityParam
querySecurityParamEra localNodeConnectInfo shelleyBasedEra = do
  C.queryNodeLocalState localNodeConnectInfo Nothing queryInMode >>= \case
    Left acquiringFailure -> throwIO acquiringFailure
    Right rest -> case rest of
      Left eraMismatch -> throwIO eraMismatch
      Right genesisParams -> return $ getSecurityParam genesisParams
  where
    queryInMode :: C.QueryInMode C.CardanoMode (Either O.EraMismatch C.GenesisParameters)
    queryInMode =
      C.QueryInEra (Marconi.toShelleyEraInCardanoMode shelleyBasedEra) $
        C.QueryInShelleyBasedEra shelleyBasedEra C.QueryGenesisParameters

    getSecurityParam :: C.GenesisParameters -> Marconi.SecurityParam
    getSecurityParam = fromIntegral . C.protocolParamSecurity

querySecurityParam :: C.LocalNodeConnectInfo C.CardanoMode -> IO Marconi.SecurityParam
querySecurityParam localNodeConnectInfo = do
  C.AnyCardanoEra era <- queryCurrentEra localNodeConnectInfo
  case Marconi.shelleyBasedToCardanoEra era of
    Nothing -> throwIO $ UnspecifiedException "The security parameter can only be queried in shelley based era."
    Just shelleyBasedEra -> querySecurityParamEra localNodeConnectInfo shelleyBasedEra

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
