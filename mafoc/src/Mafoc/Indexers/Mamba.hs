{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Indexers.Mamba where

import Data.Functor (void)
import Data.Map qualified as M
import Database.SQLite.Simple qualified as SQL
import Data.Set qualified as Set
import Data.Text qualified as TS
import Control.Exception qualified as E

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Mafoc.CLI qualified as O
import Mafoc.Core
  ( DbPathAndTableName
  , Indexer (Event, Runtime, State, checkpoint, description, parseCli, toEvents)
  , LocalChainsyncConfig
  , NodeConfig
  , defaultTableName
  , getCheckpointSqlite
  , initialize
  , initializeLocalChainsync
  , interval
  , persistMany
  , setCheckpointSqlite
  , sqliteOpen
  , sqliteInitCheckpoints
  , loadLatestTrace
  , LedgerEra, slotEra
  )
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.Exceptions qualified as E
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Indexers.MintBurn qualified as MintBurn
import Mafoc.Indexers.Utxo qualified as Utxo
import Mafoc.StateFile qualified as StateFile
import Mafoc.LedgerState qualified as LedgerState


data Mamba = Mamba
  { chainsyncConfig :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  , stateFilePrefix :: FilePath
  }
  deriving (Show)

instance Indexer Mamba where
  description = "All mamba indexers"

  parseCli =
    Mamba
      <$> O.mkCommonLocalChainsyncConfig O.commonNodeConnectionAndConfig
      <*> O.commonDbPathAndTableName
      <*> O.stateFilePrefix "mamba"

  data Event Mamba = Event
    { mintBurnEvents :: [Event MintBurn.MintBurn]
    , utxoEvents :: [Event Utxo.Utxo]
    , newEpoch :: Maybe (M.Map C.PoolId C.Lovelace, Event EpochNonce.EpochNonce)
    }
    deriving (Show)

  data Runtime Mamba = Runtime
    { sqlConnection :: SQL.Connection
    , tableMintBurn :: String
    , tableUtxo :: String
    , tableEpochStakepoolSize :: String
    , tableEpochNonce :: String
    , ledgerCfg :: LedgerState.ExtLedgerCfg_
    , ledgerStateFile :: FilePath
    , utxoStateFile :: FilePath
    , eraHistory :: C.EraHistory C.CardanoMode
    , systemStart :: C.SystemStart
    }

  data State Mamba = State
    { extLedgerState :: LedgerState.ExtLedgerState_
    , maybeEpochNo :: Maybe C.EpochNo
    , utxoState :: State Utxo.Utxo
    }

  toEvents Runtime{ledgerCfg} State{extLedgerState, maybeEpochNo, utxoState} blockInMode = (state', [event])
    where
      extLedgerState' = LedgerState.applyBlock ledgerCfg extLedgerState blockInMode :: LedgerState.ExtLedgerState_
      maybeEpochNo' = LedgerState.getEpochNo extLedgerState'
      stakeMap = LedgerState.getStakeMap extLedgerState'
      epochNonce = LedgerState.getEpochNonce extLedgerState'

      utxoRuntime = undefined :: Utxo.Runtime Utxo.Utxo
      (utxoState', utxoEvents) = toEvents @Utxo.Utxo utxoRuntime utxoState blockInMode

      mintBurnRuntime = undefined :: Runtime MintBurn.MintBurn
      (_mintBurnState', mintBurnEvents) = toEvents @MintBurn.MintBurn mintBurnRuntime MintBurn.EmptyState blockInMode

      essEvents = case EpochResolution.resolve maybeEpochNo maybeEpochNo' of
        EpochResolution.New epochNo -> Just (stakeMap, EpochNonce.Event epochNo epochNonce (#blockNo blockInMode) (#blockHeaderHash blockInMode) (#slotNo blockInMode))
        EpochResolution.SameOrAbsent -> Nothing
        EpochResolution.AssumptionBroken exception -> E.throw exception

      event = Event mintBurnEvents utxoEvents essEvents

      state' = State extLedgerState' maybeEpochNo' utxoState'

  initialize Mamba{chainsyncConfig, dbPathAndTableName, stateFilePrefix} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
        ledgerStateFile = stateFilePrefix <> "_ledgerState"
        utxoStateFile = stateFilePrefix <> "_utxo"

    ((ledgerCfg, extLedgerState), ledgerStateCp) <- loadLatestTrace ledgerStateFile (LedgerState.init_ nodeConfig) (LedgerState.load nodeConfig) trace
    (utxoState, utxoCp) <- StateFile.loadLatest utxoStateFile
      Utxo.parseState
      (Utxo.byronGenesisUtxoFromConfig <$> LedgerState.getGenesisConfig (#nodeConfig chainsyncConfig))

    let (dbPath, tablePrefix) = defaultTableName "mamba" dbPathAndTableName
        tableMintBurn = tablePrefix <> "_mintburn"
        tableUtxo = tablePrefix <> "_utxo"
        tableEpochStakepoolSize = tablePrefix <> "_epoch_sdd"
        tableEpochNonce = tablePrefix <> "_epoch_nonce"
    sqlConnection <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlConnection
    maybeCheckpointCp <- getCheckpointSqlite sqlConnection "mamba"
    let checkpointCp = fromMaybe C.ChainPointAtGenesis maybeCheckpointCp

    case Set.toList $ Set.fromList [checkpointCp, utxoCp, ledgerStateCp] of
      [] -> E.throwIO $ E.The_impossible_happened "The set can't be empty"
      _ : _ : _ -> E.throwIO $
        E.ChainPoints_don't_match [("checkpoints table", maybeCheckpointCp), ("utxo state", Just utxoCp), ("ledger state", Just ledgerStateCp)]
      [stateCp] -> do
        Utxo.sqliteInit sqlConnection tableUtxo
        MintBurn.sqliteInit sqlConnection tableMintBurn
        EpochStakepoolSize.sqliteInit sqlConnection tableEpochStakepoolSize
        EpochNonce.sqliteInit sqlConnection tableEpochNonce

        localChainsyncRuntime <- do
          networkId <- #getNetworkId nodeConfig
          localChainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace
          let cliCp = fst $ interval localChainsyncRuntime'
          if cliCp <= stateCp
            then return $ localChainsyncRuntime' {interval = (stateCp, snd $ interval localChainsyncRuntime') }
            else E.throwIO $ E.TextException $
              "Startingpoint specified on the command line is later than the starting point found in indexer state: "
              <> TS.pack (show cliCp) <> " vs " <> TS.pack (show stateCp)

        let lnc = localNodeConnection localChainsyncRuntime
        eraHistory <- either E.throwShow pure =<< C.queryNodeLocalState lnc Nothing (C.QueryEraHistory C.CardanoModeIsMultiEra)
        systemStart <- either E.throwShow pure =<< C.queryNodeLocalState lnc Nothing C.QuerySystemStart
        let state = State extLedgerState (LedgerState.getEpochNo extLedgerState) utxoState 0
            runtime = Runtime{sqlConnection, tableMintBurn, tableUtxo, tableEpochStakepoolSize, tableEpochNonce, ledgerCfg, ledgerStateFile, utxoStateFile, eraHistory, systemStart}
        return (state, localChainsyncRuntime, runtime)

  persistMany Runtime{sqlConnection, tableMintBurn, tableUtxo, tableEpochStakepoolSize, tableEpochNonce, ledgerCfg} events = do
    MintBurn.persistManySqlite sqlConnection tableMintBurn (mintBurnEvents =<< events)
    Utxo.persistManySqlite sqlConnection tableUtxo (utxoEvents =<< events)

    let epochEvents = mapMaybe newEpoch events
        essEvents = map (\(essMap, EpochNonce.Event{EpochNonce.epochNo}) -> EpochStakepoolSize.Event epochNo essMap) epochEvents
    persistMany (EpochNonce.Runtime sqlConnection tableEpochNonce ledgerCfg) $ map snd epochEvents
    persistMany (EpochStakepoolSize.Runtime sqlConnection tableEpochStakepoolSize ledgerCfg) essEvents

  checkpoint Runtime{sqlConnection, ledgerCfg, utxoStateFile, ledgerStateFile} State{extLedgerState, utxoState} slotNoBhh = do
    void $ LedgerState.store ledgerStateFile slotNoBhh ledgerCfg extLedgerState -- epochstakepoolsize, epochnonce
    void $ Utxo.storeStateFile utxoStateFile slotNoBhh utxoState
    setCheckpointSqlite sqlConnection "mamba" slotNoBhh
