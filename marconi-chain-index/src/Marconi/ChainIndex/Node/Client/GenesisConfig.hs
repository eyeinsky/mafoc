{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
module Marconi.ChainIndex.Node.Client.GenesisConfig
  ( NodeConfigPath(NodeConfigPath)
  , readNodeConfig

  -- * Re-exports
  , C.GenesisConfig
  , C.readCardanoGenesisConfig
  , C.renderGenesisConfigError
  , C.mkProtocolInfoCardano
  , initExtLedgerStateVar
  ) where

import Control.Monad.Trans.Except (ExceptT)
import Data.Text (Text)

import Cardano.Api qualified as C
import Ouroboros.Consensus.Cardano.Block qualified as Consensus
import Ouroboros.Consensus.Cardano.Block qualified as HFC
import Ouroboros.Consensus.Ledger.Extended qualified as Ledger
import Ouroboros.Consensus.Node qualified as Consensus

newtype NodeConfigPath = NodeConfigPath
  { unNetworkConfigFile :: FilePath
  } deriving (Show)

readNodeConfig :: NodeConfigPath -> ExceptT Text IO C.NodeConfig
readNodeConfig (NodeConfigPath ncf) = C.readNodeConfig (C.File ncf)

initExtLedgerStateVar
  :: C.GenesisConfig
  -> Ledger.ExtLedgerState (HFC.HardForkBlock (Consensus.CardanoEras Consensus.StandardCrypto))
initExtLedgerStateVar genesisConfig = Consensus.pInfoInitLedger $ fst protocolInfo
  where
    protocolInfo = C.mkProtocolInfoCardano genesisConfig
