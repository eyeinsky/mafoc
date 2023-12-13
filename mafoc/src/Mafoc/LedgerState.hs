{-# LANGUAGE DataKinds #-}
module Mafoc.LedgerState where

import Control.Monad.Trans.Except (runExceptT, withExceptT)
import Data.ByteString.Lazy qualified as BS
import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.Text qualified as TS

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Ledger.Compactible qualified as Ledger
import Cardano.Ledger.EpochBoundary qualified as Shelley
import Cardano.Ledger.Era qualified as Ledger
import Cardano.Ledger.Shelley.API qualified as Ledger
import Cardano.Protocol.TPraos.API qualified as Shelley
import Cardano.Protocol.TPraos.Rules.Tickn qualified as Shelley
import Data.Map.Strict qualified as Map
import Data.VMap qualified as VMap
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.HeaderValidation qualified as O
import Ouroboros.Consensus.Ledger.Abstract qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O
import Ouroboros.Consensus.Node qualified as O
import Ouroboros.Consensus.Protocol.Praos qualified as O
import Ouroboros.Consensus.Protocol.TPraos qualified as O
import Ouroboros.Consensus.Shelley.Ledger qualified as O
import Ouroboros.Consensus.Storage.Serialisation qualified as O

import Mafoc.Upstream (NodeConfig(NodeConfig), SlotNoBhh)
import Mafoc.Exceptions qualified as E
import Mafoc.StateFile qualified as StateFile


type ExtLedgerState_ = O.ExtLedgerState (O.HardForkBlock (O.CardanoEras O.StandardCrypto))
type ExtLedgerCfg_ = O.ExtLedgerCfg (O.HardForkBlock (O.CardanoEras O.StandardCrypto))


-- * Epoch data

-- | Get stake map from shelley ledger state.
shelleyStakeMap :: forall proto era . (Ledger.EraCrypto era ~ O.StandardCrypto) => O.LedgerState (O.ShelleyBlock proto era) -> Map.Map C.PoolId C.Lovelace
shelleyStakeMap st = Map.fromListWith (+) poolStakes
  where
    poolStakes :: [(C.PoolId, C.Lovelace)]
    poolStakes = do
      (sCred, spkHash) <- VMap.toList delegations
      case VMap.lookup sCred stakes of
        Just compactCoin -> [(C.StakePoolKeyHash spkHash, toLovelace compactCoin)]
        Nothing -> []

    toLovelace :: Ledger.CompactForm Ledger.Coin -> C.Lovelace
    toLovelace = C.Lovelace . coerce . Ledger.fromCompact

    stakeSnapshot :: Shelley.SnapShot O.StandardCrypto
    stakeSnapshot = Ledger.ssStakeMark . Ledger.esSnapshots . Ledger.nesEs $ O.shelleyLedgerState st

    stakes :: VMap.VMap VMap.VB VMap.VP (Ledger.Credential 'Ledger.Staking O.StandardCrypto) (Ledger.CompactForm Ledger.Coin)
    stakes = Ledger.unStake $ Ledger.ssStake stakeSnapshot
    -- ^ In form of staking credential to lovelace

    delegations :: VMap.VMap VMap.VB VMap.VB (Ledger.Credential 'Ledger.Staking O.StandardCrypto) (Ledger.KeyHash 'Ledger.StakePool O.StandardCrypto)
    delegations = Ledger.ssDelegations stakeSnapshot
    -- ^ In form of staking credential to pool key hash

-- | Get epoch number from shelley ledger state.
shelleyEpochNo :: forall proto era . O.LedgerState (O.ShelleyBlock proto era) -> C.EpochNo
shelleyEpochNo = Ledger.nesEL . O.shelleyLedgerState

getEpochNo :: O.CardanoLedgerState O.StandardCrypto -> Maybe C.EpochNo
getEpochNo ledgerState = fst <$> getEpochNoAndStakeMap ledgerState

getEpochNoAndStakeMap :: O.CardanoLedgerState O.StandardCrypto -> Maybe (C.EpochNo, Map.Map C.PoolId C.Lovelace)
getEpochNoAndStakeMap = \case
  O.LedgerStateByron  _st -> Nothing
  O.LedgerStateShelley st -> mkPair st
  O.LedgerStateAllegra st -> mkPair st
  O.LedgerStateMary    st -> mkPair st
  O.LedgerStateAlonzo  st -> mkPair st
  O.LedgerStateBabbage st -> mkPair st
  O.LedgerStateConway  st -> mkPair st
  where
    mkPair st = Just (shelleyEpochNo st, shelleyStakeMap st)

-- ** Nonce

getEpochNonce :: ExtLedgerState_ -> Ledger.Nonce
getEpochNonce extLedgerState = case O.headerStateChainDep (O.headerState extLedgerState) of
  O.ChainDepStateByron _ -> Ledger.NeutralNonce
  O.ChainDepStateShelley st -> tpraosNonce st
  O.ChainDepStateAllegra st -> tpraosNonce st
  O.ChainDepStateMary st -> tpraosNonce st
  O.ChainDepStateAlonzo st -> tpraosNonce st
  O.ChainDepStateBabbage st -> praosNonce st
  O.ChainDepStateConway st -> praosNonce st

-- | Get epoch nonce from TPraos ledger state (Shelley, Allegra, Mary, Alonzo).
tpraosNonce :: O.TPraosState c -> Ledger.Nonce
tpraosNonce = Shelley.ticknStateEpochNonce . Shelley.csTickn . O.tpraosStateChainDepState

-- | Get epoch nonce from Praos ledger state (Babbage, Conway).
praosNonce :: O.PraosState c -> Ledger.Nonce
praosNonce = O.praosStateEpochNonce

-- * Ledger state at genesis

genesisExtLedgerState :: C.GenesisConfig -> ExtLedgerState_
genesisExtLedgerState genesisConfig = O.pInfoInitLedger $ fst $ C.mkProtocolInfoCardano genesisConfig

genesisExtLedgerCfg :: C.GenesisConfig -> ExtLedgerCfg_
genesisExtLedgerCfg genesisConfig = O.ExtLedgerCfg $ O.pInfoConfig $ fst $ C.mkProtocolInfoCardano genesisConfig

init_ :: NodeConfig -> IO (ExtLedgerCfg_, ExtLedgerState_)
init_ nodeConfig = do
  genesisConfig <- getGenesisConfig nodeConfig
  return (genesisExtLedgerCfg genesisConfig, genesisExtLedgerState genesisConfig)

-- * Load

load :: NodeConfig -> FilePath -> IO (ExtLedgerCfg_, ExtLedgerState_)
load nodeConfig path = do
  cfg <- genesisExtLedgerCfg <$> getGenesisConfig (coerce nodeConfig)
  let O.ExtLedgerCfg topLevelConfig = cfg
  extLedgerState <- loadExtLedgerState (O.configCodec topLevelConfig) path >>= \case
    Right (_, extLedgerState)   -> return extLedgerState
    Left cborDeserialiseFailure -> E.throwIO $ E.Can't_deserialise_LedgerState_from_CBOR path cborDeserialiseFailure
  return (cfg, extLedgerState)

loadExtLedgerState
  :: O.CodecConfig (O.CardanoBlock O.StandardCrypto)
  -> FilePath
  -> IO (Either CBOR.DeserialiseFailure (BS.ByteString, ExtLedgerState_))
loadExtLedgerState codecConfig fp = do
  ledgerStateBs <- BS.readFile fp
  return $ CBOR.deserialiseFromBytes
    (O.decodeExtLedgerState
      (O.decodeDisk codecConfig)
      (O.decodeDisk codecConfig)
      (O.decodeDisk codecConfig))
    ledgerStateBs

-- * Store

store :: FilePath -> SlotNoBhh -> ExtLedgerCfg_ -> ExtLedgerState_ -> IO FilePath
store prefix slotNoBhh (O.ExtLedgerCfg topLevelConfig) extLedgerState =
  StateFile.store prefix slotNoBhh $ \path -> writeExtLedgerState path (O.configCodec topLevelConfig) extLedgerState

writeExtLedgerState :: FilePath -> O.CodecConfig (O.CardanoBlock O.StandardCrypto) -> ExtLedgerState_ -> IO ()
writeExtLedgerState fname codecConfig extLedgerState = BS.writeFile fname $ CBOR.toLazyByteString encoded
  where
    encoded = O.encodeExtLedgerState (O.encodeDisk codecConfig) (O.encodeDisk codecConfig) (O.encodeDisk codecConfig) extLedgerState

-- * Evolve

applyBlock :: ExtLedgerCfg_ -> ExtLedgerState_ -> C.BlockInMode C.CardanoMode -> ExtLedgerState_
applyBlock hfLedgerConfig extLedgerState blockInMode =
  O.lrResult $ O.tickThenReapplyLedgerResult hfLedgerConfig (C.toConsensusBlock blockInMode) extLedgerState

-- * GenesisConfig

getGenesisConfig :: NodeConfig -> IO C.GenesisConfig
getGenesisConfig (NodeConfig nodeConfigPath) = (either (fail . TS.unpack) pure =<<) $ runExceptT $ do
  nodeConfig <- C.readNodeConfig (C.File nodeConfigPath)
  withExceptT (TS.pack . show) $ C.readCardanoGenesisConfig nodeConfig
