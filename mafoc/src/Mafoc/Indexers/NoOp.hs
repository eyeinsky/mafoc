{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.NoOp where

import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_,
                   setCheckpointSqlite, getCheckpointSqlite, sqliteInitCheckpoints, sqliteOpen, modifyStartingPoint)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer NoOp where
  description = "Don't index anything, simply drain blocks over local chainsync protocol"
  parseCli = NoOp
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName
  newtype Event NoOp = Event ()
  data State NoOp = EmptyState
  data Runtime NoOp = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  toEvents _runtime _state _blockInMode = (EmptyState, [])
  initialize NoOp{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlCon
    checkpointedChainPoint <- fromMaybe C.ChainPointAtGenesis <$> getCheckpointSqlite sqlCon tableName
    let chainsyncRuntime' = modifyStartingPoint chainsyncRuntime (\old -> max checkpointedChainPoint old)
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)
  persistMany _runtime _events = return ()
  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
