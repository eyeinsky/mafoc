{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLabels        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.EpochNonce where

import Cardano.Api qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Control.Exception qualified as E
import Database.SQLite.Simple qualified as SQL

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, sqliteOpen, defaultTableName, initializeLocalChainsync,
                   loadLatestTrace
                  )
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.LedgerState qualified as LedgerState
import Mafoc.StateFile qualified as StateFile
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data EpochNonce = EpochNonce
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer EpochNonce where

  description = "Index epoch numbers and epoch nonces"

  parseCli = EpochNonce
    <$> Opt.mkCommonLocalChainsyncConfig Opt.commonNodeConnectionAndConfig
    <*> Opt.commonDbPathAndTableName

  data Event EpochNonce = Event
    { epochNo    :: C.EpochNo
    , epochNonce :: Ledger.Nonce
    }

  data Runtime EpochNonce = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , ledgerCfg     :: Marconi.ExtLedgerCfg_
    }
  data State EpochNonce = State
    { extLedgerState       :: Marconi.ExtLedgerState_
    , maybePreviousEpochNo :: Maybe C.EpochNo
    }

  toEvents (Runtime{ledgerCfg}) state blockInMode = (State newExtLedgerState maybeEpochNo, coerce maybeEvent)
    where
    newExtLedgerState = Marconi.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeEpochNo = Marconi.getEpochNo newExtLedgerState
    epochNonce = Marconi.getEpochNonce newExtLedgerState
    maybeEvent :: [Event EpochNonce]
    maybeEvent = case EpochResolution.resolve (maybePreviousEpochNo state) maybeEpochNo of
      EpochResolution.New epochNo -> [Event epochNo epochNonce]
      EpochResolution.SameOrAbsent -> []
      EpochResolution.AssumptionBroken exception -> E.throw exception

  initialize EpochNonce{chainsyncConfig, dbPathAndTableName} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
    networkId <- #getNetworkId nodeConfig
    chainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace

    let (dbPath, tableName) = defaultTableName "epoch_nonce" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName

    ((ledgerConfig, extLedgerState), stateChainPoint) <- loadLatestTrace "ledgerState" (LedgerState.init_ nodeConfig) (LedgerState.load nodeConfig) trace

    return ( State extLedgerState (Marconi.getEpochNo extLedgerState)
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{ledgerCfg} State{extLedgerState} slotNoBhh = LedgerState.store (StateFile.toName "ledgerState" slotNoBhh) ledgerCfg extLedgerState

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " \
  \   ( epoch_no INT NOT NULL  \
  \   , nonce    BLOB NOT NULL ) "

sqliteInsert :: SQL.Connection -> String -> [Event EpochNonce] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c
  ("INSERT INTO " <> fromString tableName <>" (epoch_no, nonce) VALUES (?, ?)")
  (map (\(Event epochNo nonce) -> (epochNo, nonce)) events)
