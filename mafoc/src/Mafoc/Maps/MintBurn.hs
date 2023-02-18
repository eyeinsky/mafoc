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
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

import Mafoc.Helpers (BlockSourceConfig (BlockSourceConfig), DbPathAndTableName,
                      Indexer (Event, Runtime, State, initialize, persist, toEvent), Interval, defaultTableName,
                      findIntervalToBeIndexed, getSecurityParam, sqliteInitBookmarks)

-- | Configuration data type which does double-duty as the tag for the
-- indexer.
data MintBurn = MintBurn
  { dbPathAndTableName        :: DbPathAndTableName
  , socketPath                :: String
  , networkId                 :: C.NetworkId
  , interval                  :: Interval
  , securityParamOrNodeConfig :: Either Natural FilePath
  } deriving (Show)

parseCli :: Opt.ParserInfo MintBurn
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "mintburn"
  <> Opt.header "mintburn - Index mint and burn events"
  where
    cli :: Opt.Parser MintBurn
    cli = MintBurn
      <$> Opt.commonDbPathAndTableName
      <*> Opt.commonSocketPath
      <*> Opt.commonNetworkId
      <*> Opt.commonInterval
      <*> Opt.commonSecurityParamEither

data Runtime_ = Runtime_
  { sqlConnection :: SQL.Connection
  , tableName     :: String
  }

instance Indexer MintBurn where

  type Runtime MintBurn = Runtime_

  type Event MintBurn = Marconi.MintBurn.TxMintEvent

  data State MintBurn = EmptyState

  toEvent _ a = maybe Nothing (\e -> Just (EmptyState,e)) $ Marconi.MintBurn.toUpdate a

  persist Runtime_{sqlConnection, tableName} event = do
    Marconi.MintBurn.sqliteInsert sqlConnection tableName [event]

  initialize config = do
    let (dbPath, tableName) = defaultTableName "mintburn" $ dbPathAndTableName config
    c <- SQL.open dbPath
    Marconi.MintBurn.sqliteInit c tableName
    sqliteInitBookmarks c
    interval' <- findIntervalToBeIndexed (interval config) c tableName
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (EmptyState, BlockSourceConfig localNodeCon interval' k, Runtime_ c tableName)
