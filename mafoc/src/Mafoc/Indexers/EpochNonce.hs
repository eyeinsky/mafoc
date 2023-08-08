{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.EpochNonce where

import Data.Coerce (coerce)

import Cardano.Api qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Data.String (fromString)
import Database.SQLite.Simple qualified as SQL

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, initializeLedgerStateAndDatabase, storeLedgerState)
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
    maybeEvent = case maybeEpochNo of
      Just epochNo -> case maybePreviousEpochNo state of
        Just previousEpochNo -> case epochNo - previousEpochNo of
          1 -> [Event epochNo epochNonce]
          0 -> []
          invalidEpochDiff -> error $ "EpochNonce indexer: assumption violated: epoch changed by " <> show invalidEpochDiff <> " instead of expected 0 or 1."
        Nothing -> [Event epochNo epochNonce]
      Nothing -> []

  initialize EpochNonce{chainsyncConfig, dbPathAndTableName} trace = do
    (extLedgerState, epochNo, chainsyncRuntime', sqlCon, tableName, ledgerConfig) <-
      initializeLedgerStateAndDatabase chainsyncConfig trace dbPathAndTableName sqliteInit "epoch_nonce"
    return ( State extLedgerState epochNo
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{ledgerCfg} State{extLedgerState} slotNoBhh = storeLedgerState ledgerCfg slotNoBhh extLedgerState

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
