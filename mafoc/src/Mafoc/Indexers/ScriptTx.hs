{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Indexers.ScriptTx where

import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, checkpoint, initialize, persistMany, toEvent),
                   LocalChainsyncConfig_, blockChainPoint, defaultTableName, initializeLocalChainsync, initializeSqlite,
                   setCheckpointSqlite)
import Marconi.ChainIndex.Indexers.ScriptTx qualified as Marconi.ScriptTx
import Marconi.Core.Storable qualified as Marconi

data ScriptTx = ScriptTx
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

parseCli :: Opt.ParserInfo ScriptTx
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "scripttx"
  <> Opt.header "scripttx - Index transactions with scripts"
  where
    cli :: Opt.Parser ScriptTx
    cli = ScriptTx
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName

instance Indexer ScriptTx where

  type Event ScriptTx = Marconi.StorableEvent Marconi.ScriptTx.ScriptTxHandle

  data Runtime ScriptTx = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  data State ScriptTx = EmptyState

  toEvent _runtime _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = pure (EmptyState, event)
    where
      event = let
        event'@(Marconi.ScriptTx.ScriptTxEvent txScripts _) = Marconi.ScriptTx.toUpdate txs (blockChainPoint blockInMode)
        in case txScripts of
             [] -> Nothing
             _  -> Just event'

  initialize ScriptTx{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync chainsync
    let (dbPath, tableName) = defaultTableName "scripttx" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName Marconi.ScriptTx.sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events =
    Marconi.ScriptTx.sqliteInsert sqlConnection tableName events

  checkpoint Runtime{sqlConnection} t = setCheckpointSqlite sqlConnection "scripttx" t
