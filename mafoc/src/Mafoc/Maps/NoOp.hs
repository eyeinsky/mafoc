{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Maps.NoOp where

import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, initialize, persist, toEvent),
                   LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, sqliteInitBookmarks,
                   updateIntervalFromBookmarks)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig
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
  toEvent _blk _state = Just (EmptyState, ())
  initialize NoOp{chainsync, dbPathAndTableName} = do
    chainsyncRuntime <- initializeLocalChainsync chainsync
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    sqlCon <- SQL.open dbPath
    sqliteInitBookmarks sqlCon
    chainsyncRuntime' <- updateIntervalFromBookmarks chainsyncRuntime tableName sqlCon
    return (EmptyState, chainsyncRuntime', Runtime)
  persist _runtime _event = return ()
