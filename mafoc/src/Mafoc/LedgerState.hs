module Mafoc.LedgerState
  ( module Mafoc.LedgerState

  -- * Re-exports
  , Marconi.getEpochNo
  , Marconi.getEpochNonce
  , Marconi.getStakeMap
  ) where

import Control.Monad.Trans.Except (runExceptT, withExceptT)
import Data.ByteString.Lazy qualified as BS
import Codec.CBOR.Read qualified as CBOR
import Codec.CBOR.Write qualified as CBOR
import Data.Text qualified as TS

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.Ledger.Abstract qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O
import Ouroboros.Consensus.Node qualified as Consensus
import Ouroboros.Consensus.Node qualified as O
import Ouroboros.Consensus.Storage.Serialisation qualified as O
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

import Mafoc.Upstream (NodeConfig(NodeConfig), SlotNoBhh)
import Mafoc.Exceptions qualified as E
import Mafoc.StateFile qualified as StateFile


type ExtLedgerState_ = O.ExtLedgerState (O.HardForkBlock (O.CardanoEras O.StandardCrypto))
type ExtLedgerCfg_ = O.ExtLedgerCfg (O.HardForkBlock (O.CardanoEras O.StandardCrypto))

-- * Ledger state at genesis

genesisExtLedgerState :: C.GenesisConfig -> ExtLedgerState_
genesisExtLedgerState genesisConfig = Consensus.pInfoInitLedger $ fst $ C.mkProtocolInfoCardano genesisConfig

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
