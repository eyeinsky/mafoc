{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.EpochNonce where

import Cardano.Api qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Data.String (fromString)
import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, checkpoint, initialize, persistMany, toEvent),
                   LocalChainsyncConfig, NodeConfig, initializeLedgerState, storeLedgerState)
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data EpochNonce = EpochNonce
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

data EpochNonceEvent = EpochNonceEvent
  { epochNo    :: C.EpochNo
  , epochNonce :: Ledger.Nonce
  }

parseCli :: Opt.ParserInfo EpochNonce
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "epochnonce"
  <> Opt.header "epochnonce - Index epoch nonces"
  where
    cli :: Opt.Parser EpochNonce
    cli = EpochNonce
      <$> Opt.mkCommonLocalChainsyncConfig Opt.commonNodeConnectionAndConfig
      <*> Opt.commonDbPathAndTableName

instance Indexer EpochNonce where

  type Event EpochNonce = EpochNonceEvent

  data Runtime EpochNonce = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , ledgerCfg     :: Marconi.ExtLedgerCfg_
    }
  data State EpochNonce = State
    { extLedgerState       :: Marconi.ExtLedgerState_
    , maybePreviousEpochNo :: Maybe C.EpochNo
    }

  toEvent (Runtime{ledgerCfg}) state blockInMode = return (State newExtLedgerState maybeEpochNo, maybeEvent)
    where
    newExtLedgerState = Marconi.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeEpochNo = Marconi.getEpochNo newExtLedgerState
    epochNonce = Marconi.getEpochNonce newExtLedgerState
    maybeEvent :: Maybe EpochNonceEvent
    maybeEvent = case maybeEpochNo of
      Just epochNo -> case maybePreviousEpochNo state of
        Just previousEpochNo -> case epochNo - previousEpochNo of
          1 -> Just $ EpochNonceEvent epochNo epochNonce
          0 -> Nothing
          invalidEpochDiff -> error $ "EpochNonce indexer: assumption violated: epoch changed by " <> show invalidEpochDiff <> " instead of expected 0 or 1."
        Nothing -> Just $ EpochNonceEvent epochNo epochNonce
      Nothing -> Nothing

  initialize EpochNonce{chainsyncConfig, dbPathAndTableName} trace = do
    (extLedgerState, epochNo, chainsyncRuntime', sqlCon, tableName, ledgerConfig) <-
      initializeLedgerState chainsyncConfig trace dbPathAndTableName sqliteInit "epoch_nonce"
    return ( State extLedgerState epochNo
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName events

  checkpoint Runtime{ledgerCfg} State{extLedgerState} slotNoBhh = storeLedgerState ledgerCfg slotNoBhh extLedgerState

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " \
  \   ( epoch_no INT NOT NULL  \
  \   , nonce    BLOB NOT NULL ) "

sqliteInsert :: SQL.Connection -> String -> [EpochNonceEvent] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c
  ("INSERT INTO " <> fromString tableName <>" (epoch_no, nonce) VALUES (?, ?)")
  (map (\(EpochNonceEvent epochNo nonce) -> (epochNo, nonce)) events)
