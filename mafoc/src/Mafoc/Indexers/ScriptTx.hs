module Mafoc.Indexers.ScriptTx where

import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, blockChainPoint, defaultTableName, initializeLocalChainsync_,
                   useSqliteCheckpoint, setCheckpointSqlite, stateless)
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

  newtype Event ScriptTx = Event (Marconi.StorableEvent Marconi.ScriptTxHandle)

  data Runtime ScriptTx = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  newtype State ScriptTx = Stateless ()

  toEvents _runtime _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = (stateless, coerce event)
    where
      event = let
        event'@(Marconi.ScriptTxEvent txScripts _) = Marconi.toUpdate txs (blockChainPoint blockInMode)
        in case txScripts of
             [] -> []
             _  -> [event']

  initialize cli@ScriptTx{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace $ show cli
    let (dbPath, tableName) = defaultTableName "scripttx" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- useSqliteCheckpoint dbPath tableName trace chainsyncRuntime
    return (stateless, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events =
    Marconi.sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{sqlConnection, tableName} _state t = setCheckpointSqlite sqlConnection tableName t
