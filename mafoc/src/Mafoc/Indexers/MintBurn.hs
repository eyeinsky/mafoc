{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}

{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.
-}
module Mafoc.Indexers.MintBurn where

import Data.Coerce (coerce)
import Database.SQLite.Simple qualified as SQL

import Mafoc.CLI qualified as Opt
import Mafoc.Core (
  DbPathAndTableName,
  Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
  LocalChainsyncConfig_,
  defaultTableName,
  initializeLocalChainsync_,
  initializeSqlite,
  setCheckpointSqlite,
 )
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

{- | Configuration data type which does double-duty as the tag for the
 indexer.
-}
data MintBurn = MintBurn
  { chainsync :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  }
  deriving (Show)

instance Indexer MintBurn where

  description = "Index mint and burn events"

  parseCli =
    MintBurn
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName

  data Runtime MintBurn = Runtime
    { sqlConnection :: SQL.Connection
    , tableName :: String
    }

  newtype Event MintBurn = Event Marconi.MintBurn.TxMintEvent
    deriving Show

  data State MintBurn = EmptyState

  toEvents _runtime _state blockInMode = (EmptyState, coerce [Marconi.MintBurn.toUpdate Nothing blockInMode])

  initialize MintBurn{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync
    let (dbPath, tableName) = defaultTableName "mintburn" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName Marconi.MintBurn.sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events = do
    Marconi.MintBurn.sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
