{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Indexers.EpochState where

import Database.SQLite.Simple qualified as SQL
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

import Mafoc.Core (Indexer (Event, Runtime, State, checkpoint, initialize, persistMany, toEvent),
                   initializeLedgerStateAndDatabase, storeLedgerState)
import Mafoc.Indexers.EpochNonce (EpochNonce)
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize (EpochStakepoolSize)
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Options.Applicative qualified as Opt


newtype EpochState = EpochState EpochStakepoolSize.EpochStakepoolSize
  deriving Show

parseCli :: Opt.ParserInfo EpochState
parseCli = Opt.info (Opt.helper <*> (EpochState <$> EpochStakepoolSize.cli)) $ Opt.fullDesc
  <> Opt.progDesc "epochstate"
  <> Opt.header "epochstate - Index epoch nonce and stakepool sizes"

data EpochStateEvent = EpochStateEvent
  { epochStake :: EpochStakepoolSize.EpochStakepoolSizeEvent
  , epochNonce :: EpochNonce.EpochNonceEvent
  }

instance Indexer EpochState where

  type Event EpochState = EpochStateEvent

  data Runtime EpochState = Runtime
    { sqlConnection :: SQL.Connection
    , tableNameEpochStakepoolSize :: String
    , tableNameEpochNonce :: String
    , ledgerCfg     :: Marconi.ExtLedgerCfg_
    }
  newtype State EpochState = State (State EpochStakepoolSize)

  toEvent runtime state blockInMode = do
    (st1, e1) <- toEvent (toEssRuntime runtime) (toEpochStakepoolSizeState state) blockInMode
    -- TODO: Calculation of _st2 is surperfluous, probably has a performance impact.
    (_st2, e2) <- toEvent (toEpochNonceRuntime runtime) (toEpochNonceState state) blockInMode
    return (State st1, EpochStateEvent <$> e1 <*> e2)

  initialize (EpochState EpochStakepoolSize.EpochStakepoolSize{EpochStakepoolSize.chainsyncConfig, EpochStakepoolSize.dbPathAndTableName}) trace = do
    -- TODO: As this is a two-table indexer, the sql initialization is
    -- a bit awkward here, as the suffixes are added here and in
    -- sqliteInit as well. The same function to create the table names
    -- can be used, but it's still awkward.
    (extLedgerState, epochNo, chainsyncRuntime', sqlCon, tablePrefix, ledgerConfig) <-
      initializeLedgerStateAndDatabase chainsyncConfig trace dbPathAndTableName sqliteInit "epochstate"
    let t1 = tablePrefix <> "_ess"
        t2 = tablePrefix <> "_nonce"
    return ( State $ EpochStakepoolSize.State extLedgerState epochNo
           , chainsyncRuntime'
           , Runtime sqlCon t1 t2 ledgerConfig)
    where
      sqliteInit c tablePrefix = do
        EpochStakepoolSize.sqliteInit c $ tablePrefix <> "_ess"
        EpochNonce.sqliteInit c $ tablePrefix <> "_nonce"

  persistMany Runtime{sqlConnection, tableNameEpochStakepoolSize, tableNameEpochNonce} events = do
    EpochStakepoolSize.sqliteInsert sqlConnection tableNameEpochStakepoolSize $ map epochStake events
    EpochNonce.sqliteInsert sqlConnection tableNameEpochNonce $ map epochNonce events

  checkpoint Runtime{ledgerCfg} (State EpochStakepoolSize.State{EpochStakepoolSize.extLedgerState}) slotNoBhh =
    storeLedgerState ledgerCfg slotNoBhh extLedgerState

-- * Conversions

toEssRuntime :: Runtime EpochState -> Runtime EpochStakepoolSize
toEssRuntime Runtime{sqlConnection, tableNameEpochStakepoolSize, ledgerCfg} =
  EpochStakepoolSize.Runtime sqlConnection tableNameEpochStakepoolSize ledgerCfg

toEpochNonceRuntime :: Runtime EpochState -> Runtime EpochNonce
toEpochNonceRuntime Runtime{sqlConnection, tableNameEpochNonce, ledgerCfg} =
  EpochNonce.Runtime sqlConnection tableNameEpochNonce ledgerCfg

toEpochStakepoolSizeState :: State EpochState -> State EpochStakepoolSize
toEpochStakepoolSizeState (State s) = s

toEpochNonceState :: State EpochState -> State EpochNonce
toEpochNonceState (State (EpochStakepoolSize.State extLedgerState maybePreviousEpochNo)) = EpochNonce.State extLedgerState maybePreviousEpochNo
