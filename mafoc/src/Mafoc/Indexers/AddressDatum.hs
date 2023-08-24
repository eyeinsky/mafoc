{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.AddressDatum where

import Data.Coerce (coerce)
import Cardano.Api qualified as C
import Database.SQLite.Simple qualified as SQL
import Marconi.ChainIndex.Indexers.AddressDatum qualified as Marconi

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, blockChainPoint, defaultTableName, initializeLocalChainsync_,
                   initializeSqlite, mkMaybeAddressFilter, setCheckpointSqlite)
import Options.Applicative qualified as Opt


data AddressDatum = AddressDatum
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  , addresses          :: [C.Address C.ShelleyAddr]
  } deriving Show

instance Indexer AddressDatum where

  description = "Index datums for addresses"

  parseCli = AddressDatum
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName
    <*> Opt.many Opt.commonAddress

  newtype Event AddressDatum = Event (Marconi.StorableEvent Marconi.AddressDatumHandle)

  data State AddressDatum = EmptyState

  data Runtime AddressDatum = Runtime
    { sqlConnection      :: SQL.Connection
    , tableName          :: String
    , maybeAddressFilter :: Maybe (C.Address C.ShelleyAddr -> Bool)
    }

  toEvents Runtime{maybeAddressFilter} _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = (EmptyState, [coerce event])
    where
      event = Marconi.toAddressDatumIndexEvent maybeAddressFilter txs (blockChainPoint blockInMode)

  persistMany Runtime{sqlConnection, tableName} events =
    Marconi.sqliteInsert sqlConnection tableName $ coerce events

  initialize AddressDatum{chainsync, dbPathAndTableName, addresses} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync
    let (dbPath, tableName) = defaultTableName "address_datums" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName Marconi.sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName $ mkMaybeAddressFilter addresses)

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh =
    setCheckpointSqlite sqlConnection tableName slotNoBhh
