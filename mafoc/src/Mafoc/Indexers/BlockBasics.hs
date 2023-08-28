{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Indexers.BlockBasics where

import Data.Coerce (coerce)
import Data.String (IsString (fromString))

import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt

import Cardano.Api qualified as C

import Mafoc.Core (chainPointForSlotNo, DbPathAndTableName, eventsToSingleChainpoint,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_, initializeSqlite,
                   setCheckpointSqlite)

-- * Block transaction count indexer

data BlockBasics = BlockBasics
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving (Show)

instance Indexer BlockBasics where

  description = "Index slot number, block header hash and number of transactions per block"

  parseCli = BlockBasics
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName

  newtype Event BlockBasics = Event (Word64, C.Hash C.BlockHeader, Int)

  data State BlockBasics = EmptyState

  data Runtime BlockBasics = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }

  toEvents _runtime _state (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _) txs) _) = (EmptyState, [coerce (coerce slotNo :: Word64, hash, length txs)])

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  initialize BlockBasics{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tableName) = defaultTableName "blockbasics" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh

-- * SQLite

sqliteInsert :: SQL.Connection -> String -> [Event BlockBasics] -> IO ()
sqliteInsert sqlCon tableName rows = SQL.executeMany sqlCon template (coerce rows :: [(Word64, C.Hash C.BlockHeader, Int)])
  where
    template = "INSERT INTO " <> fromString tableName <> " \
               \ ( slot_no           \
               \ , block_header_hash \
               \ , tx_count          \
               \ ) VALUES (?, ?, ?)  "

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit sqlCon tableName = SQL.execute_ sqlCon $
  "CREATE TABLE IF NOT EXISTS         \
  \ " <> fromString tableName <> "    \
  \ ( slot_no INT NOT NULL            \
  \ , block_header_hash BLOB NOT NULL \
  \ , tx_count INT NOT NULL )         "

lastCp :: SQL.Connection -> String -> IO (Maybe C.ChainPoint)
lastCp sqlCon tableName = fmap eventsToSingleChainpoint $ SQL.query_ sqlCon $
  "   SELECT slot_no, block_header_hash     \
  \     FROM " <> fromString tableName <> " \
  \ ORDER BY slot_no DESC                   \
  \    LIMIT 1"
