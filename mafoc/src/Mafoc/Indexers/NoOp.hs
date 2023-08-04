{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.NoOp where

import Database.SQLite.Simple qualified as SQL

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvent),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_, initializeSqlite,
                   setCheckpointSqlite)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer NoOp where
  description = "Don't index anything, simply drain blocks over local chainsync protocol"
  parseCli = NoOp
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName
  type Event NoOp = ()
  data State NoOp = EmptyState
  data Runtime NoOp = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  toEvent _runtime _state _blockInMode = pure (EmptyState, Nothing)
  initialize NoOp{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName (\_ _ -> return ()) chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)
  persistMany _runtime _events = return ()
  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
