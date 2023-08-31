{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Indexers.Mamba where

import Data.Map qualified as M
import Database.SQLite.Simple qualified as SQL
import Data.Set qualified as Set
import Data.Text qualified as TS

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
  , loadLedgerStateWithChainpoint
  , persistMany
  , setCheckpointSqlite
  , sqliteOpen
  , storeLedgerState
  , sqliteInitCheckpoints
  )
import Mafoc.Exceptions qualified as E
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Indexers.MintBurn qualified as MintBurn
import Mafoc.Indexers.Utxo qualified as Utxo
import Mafoc.StateFile qualified as StateFile

import Marconi.ChainIndex.Indexers.EpochState
  ( ExtLedgerCfg_
  , ExtLedgerState_
  , applyBlock
  , getEpochNo
  , getEpochNonce
  , getStakeMap
  )
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

data Mamba = Mamba
  { chainsyncConfig :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  , utxoStateFilePrefix_ :: FilePath
  }
  deriving (Show)

instance Indexer Mamba where
  description = "All mamba indexers"

  parseCli =
    Mamba
      <$> O.mkCommonLocalChainsyncConfig O.commonNodeConnectionAndConfig
      <*> O.commonDbPathAndTableName
      <*> O.commonUtxoState

  data Event Mamba = Event
    { mintBurnEvents :: [Event MintBurn.MintBurn]
    , utxoEvents :: [Utxo.Event Utxo.Utxo]
    , newEpoch :: Maybe (C.EpochNo, Ledger.Nonce, M.Map C.PoolId C.Lovelace)
    }
    deriving (Show)

  data Runtime Mamba = Runtime
    { sqlConnection :: SQL.Connection
    , tablePrefix :: String
    , ledgerCfg :: ExtLedgerCfg_
    , utxoStateFilePrefix :: FilePath
    }

  data State Mamba = State
    { extLedgerState :: ExtLedgerState_
    , maybeEpochNo :: Maybe C.EpochNo
    , utxoState :: State Utxo.Utxo
    }

  toEvents Runtime{ledgerCfg} State{extLedgerState, maybeEpochNo, utxoState} blockInMode = (state', [event])
    where
      extLedgerState' = applyBlock ledgerCfg extLedgerState blockInMode :: ExtLedgerState_
      maybeEpochNo' = getEpochNo extLedgerState'
      stakeMap = getStakeMap extLedgerState'
      epochNonce = getEpochNonce extLedgerState'

      utxoRuntime = undefined :: Utxo.Runtime Utxo.Utxo
      (utxoState', utxoEvents) = toEvents @Utxo.Utxo utxoRuntime utxoState blockInMode

      mintBurnRuntime = undefined :: Runtime MintBurn.MintBurn
      (_mintBurnState', mintBurnEvents) = toEvents @MintBurn.MintBurn mintBurnRuntime MintBurn.EmptyState blockInMode

      essEvents = case maybeEpochNo of
        Just previousEpochNo -> case maybeEpochNo' of
          Just currentEpochNo ->
            let
              epochDiff = currentEpochNo - previousEpochNo
            in
              case epochDiff of
                1 -> Just (currentEpochNo, epochNonce, stakeMap) -- first block of new epoch!
                0 -> Nothing -- same epoch as before..
                _ -> E.throw $ E.Epoch_difference_other_than_0_or_1 previousEpochNo currentEpochNo
          Nothing -> E.throw $ E.Epoch_number_disappears previousEpochNo
        Nothing -> case maybeEpochNo' of
          Just currentEpochNo -> Just (currentEpochNo, epochNonce, stakeMap) -- first epoch ever!
          Nothing -> Nothing -- no epochs, still in Byron era..
      event = Event mintBurnEvents utxoEvents essEvents

      state' = State extLedgerState' maybeEpochNo' utxoState'

  initialize Mamba{chainsyncConfig, dbPathAndTableName, utxoStateFilePrefix_} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig

    (ledgerCfg, extLedgerState, ledgerStateCp) <- loadLedgerStateWithChainpoint nodeConfig trace
    (utxoState, utxoCp) <- StateFile.loadLatest utxoStateFilePrefix_ Utxo.parseState (return mempty)

    let (dbPath, tablePrefix) = defaultTableName "mamba" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlCon
    maybeCheckpointCp <- getCheckpointSqlite sqlCon "mamba"
    let checkpointCp = fromMaybe C.ChainPointAtGenesis maybeCheckpointCp

    case Set.toList $ Set.fromList [checkpointCp, utxoCp, ledgerStateCp] of
      [] -> E.throwIO $ E.The_impossible_happened "The set can't be empty"
      _ : _ : _ -> E.throwIO $
        E.ChainPoints_don't_match [("checkpoints table", maybeCheckpointCp), ("utxo state", Just utxoCp), ("ledger state", Just ledgerStateCp)]
      [stateCp] -> do
        Utxo.sqliteInit sqlCon $ tblUtxo tablePrefix
        Marconi.MintBurn.sqliteInit sqlCon $ tblMintBurn tablePrefix
        EpochStakepoolSize.sqliteInit sqlCon $ tblEss tablePrefix
        EpochNonce.sqliteInit sqlCon $ tblEpochNonce tablePrefix

        localChainsyncRuntime <- do
          networkId <- #getNetworkId nodeConfig
          localChainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace
          let cliCp = fst $ interval localChainsyncRuntime'
          if cliCp <= stateCp
            then return $ localChainsyncRuntime' {interval = (stateCp, snd $ interval localChainsyncRuntime') }
            else E.throwIO $ E.TextException $
              "Startingpoint specified on the command line is later than the starting point found in indexer state: "
              <> TS.pack (show cliCp) <> " vs " <> TS.pack (show stateCp)

        let state = State extLedgerState (getEpochNo extLedgerState) utxoState
            runtime = Runtime sqlCon tablePrefix ledgerCfg utxoStateFilePrefix_
        return (state, localChainsyncRuntime, runtime)

  persistMany Runtime{sqlConnection, tablePrefix, ledgerCfg, utxoStateFilePrefix} events = do
    persistMany (MintBurn.Runtime sqlConnection $ tblMintBurn tablePrefix) (mintBurnEvents =<< events)
    persistMany (Utxo.Runtime sqlConnection (tblUtxo tablePrefix) utxoStateFilePrefix) (utxoEvents =<< events)

    let epochEvents = mapMaybe newEpoch events
        essEvents = map (\(epochNo, _, essMap) -> EpochStakepoolSize.Event epochNo essMap) epochEvents
        enEvents = map (\(epochNo, nonce, _) -> EpochNonce.Event epochNo nonce) epochEvents
    persistMany (EpochNonce.Runtime sqlConnection (tblEpochNonce tablePrefix) ledgerCfg) enEvents
    persistMany (EpochStakepoolSize.Runtime sqlConnection (tblEss tablePrefix) ledgerCfg) essEvents

  checkpoint Runtime{sqlConnection, ledgerCfg, utxoStateFilePrefix} State{extLedgerState, utxoState} slotNoBhh = do
    storeLedgerState ledgerCfg slotNoBhh extLedgerState -- epochstakepoolsize, epochnonce
    Utxo.storeState utxoState $ StateFile.toName utxoStateFilePrefix slotNoBhh
    setCheckpointSqlite sqlConnection "mamba" slotNoBhh

tblMintBurn, tblUtxo, tblEss, tblEpochNonce :: String -> String
tblMintBurn tablePrefix = tablePrefix <> "_mintburn"
tblUtxo tablePrefix = tablePrefix <> "_utxo"
tblEss tablePrefix = tablePrefix <> "_epoch_sdd"
tblEpochNonce tablePrefix = tablePrefix <> "_epoch_nonce"
