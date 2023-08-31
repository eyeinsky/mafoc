{-# LANGUAGE LambdaCase             #-}

module Mafoc.LedgerState where

import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

import Mafoc.Upstream (NodeConfig(NodeConfig))
import Mafoc.Exceptions qualified as E

init_ :: NodeConfig -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
init_ nodeConfig = Marconi.getInitialExtLedgerState $ coerce nodeConfig

load :: NodeConfig -> FilePath -> IO (Marconi.ExtLedgerCfg_, Marconi.ExtLedgerState_)
load nodeConfig path = do
  cfg <- Marconi.getLedgerConfig $ coerce nodeConfig
  let O.ExtLedgerCfg topLevelConfig = cfg
  extLedgerState <- Marconi.loadExtLedgerState (O.configCodec topLevelConfig) path >>= \case
    Right (_, extLedgerState)   -> return extLedgerState
    Left cborDeserialiseFailure -> E.throwIO $ E.Can't_deserialise_LedgerState_from_CBOR path cborDeserialiseFailure
  return (cfg, extLedgerState)

store :: FilePath -> Marconi.ExtLedgerCfg_ -> Marconi.ExtLedgerState_ -> IO ()
store path (O.ExtLedgerCfg topLevelConfig) extLedgerState =
  Marconi.writeExtLedgerState path (O.configCodec topLevelConfig) extLedgerState
