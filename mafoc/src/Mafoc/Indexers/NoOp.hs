{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Indexers.NoOp where

import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, checkpoint, initialize, persist, toEvent),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_, initializeSqlite,
                   setCheckpointSqlite)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

parseCli :: Opt.ParserInfo NoOp
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "noop"
  <> Opt.header "noop - drain blocks over local chainsync protocol"
  where
    cli :: Opt.Parser NoOp
    cli = NoOp
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName

instance Indexer NoOp where
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
  persist _runtime _event = return ()
  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
