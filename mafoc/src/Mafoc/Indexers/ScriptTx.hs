{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.ScriptTx where

import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, blockChainPoint, defaultTableName, initializeLocalChainsync_,
                   initializeSqlite, setCheckpointSqlite)
import Marconi.ChainIndex.Indexers.ScriptTx qualified as Marconi

data ScriptTx = ScriptTx
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer ScriptTx where

  description = "Index transactions with scripts"

  parseCli = ScriptTx
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName

  type Event ScriptTx = Marconi.StorableEvent Marconi.ScriptTxHandle

  data Runtime ScriptTx = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  data State ScriptTx = EmptyState

  toEvents _runtime _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = (EmptyState, event)
    where
      event = let
        event'@(Marconi.ScriptTxEvent txScripts _) = Marconi.toUpdate txs (blockChainPoint blockInMode)
        in case txScripts of
             [] -> []
             _  -> [event']

  initialize ScriptTx{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync
    let (dbPath, tableName) = defaultTableName "scripttx" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName Marconi.sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events =
    Marconi.sqliteInsert sqlConnection tableName events

  checkpoint Runtime{sqlConnection, tableName} _state t = setCheckpointSqlite sqlConnection tableName t
