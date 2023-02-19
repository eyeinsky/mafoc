{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.

-}

module Mafoc.Maps.MintBurn where

import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt
import Options.Applicative qualified as Opt

import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, initialize, persist, persistMany, toEvent),
                   LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, initializeSqlite)
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

-- | Configuration data type which does double-duty as the tag for the
-- indexer.
data MintBurn = MintBurn
  { chainsync          :: LocalChainsyncConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving (Show)

parseCli :: Opt.ParserInfo MintBurn
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "mintburn"
  <> Opt.header "mintburn - Index mint and burn events"
  where
    cli :: Opt.Parser MintBurn
    cli = MintBurn
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName

instance Indexer MintBurn where

  data Runtime MintBurn = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }

  type Event MintBurn = Marconi.MintBurn.TxMintEvent

  data State MintBurn = EmptyState

  toEvent a _ = maybe Nothing (\e -> Just (EmptyState,e)) $ Marconi.MintBurn.toUpdate a

  initialize MintBurn{chainsync, dbPathAndTableName} = do
    chainsyncRuntime <- initializeLocalChainsync chainsync
    let (dbPath, tableName) = defaultTableName "mintburn" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName Marconi.MintBurn.sqliteInit chainsyncRuntime
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  persist Runtime{sqlConnection, tableName} event = do
    Marconi.MintBurn.sqliteInsert sqlConnection tableName [event]

  persistMany Runtime{sqlConnection, tableName} events = do
    Marconi.MintBurn.sqliteInsert sqlConnection tableName events
