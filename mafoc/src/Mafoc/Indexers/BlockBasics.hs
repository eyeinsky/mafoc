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

import Mafoc.Core (DbPathAndTableName,
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

  toEvents _runtime _state blockInMode = (EmptyState, [coerce $ blockToRow blockInMode])

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  initialize BlockBasics{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync
    let (dbPath, tableName) = defaultTableName "blockbasics" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName sqliteInit chainsyncRuntime trace
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh

type Row = (Word64, C.Hash C.BlockHeader, Int)

blockToRow :: C.BlockInMode C.CardanoMode -> Row
blockToRow (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _) txs) _) = (coerce slotNo, hash, length txs)

sqliteInsert :: SQL.Connection -> String -> [Row] -> IO ()
sqliteInsert sqlCon tableName rows = SQL.executeMany sqlCon template rows
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
lastCp sqlCon tableName = do
  rows :: [Row] <- SQL.query_ sqlCon $
    "   SELECT slot_no, block_header_hash, tx_count \
    \     FROM " <> fromString tableName <> "       \
    \ ORDER BY slot_no DESC                         \
    \    LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                      -> Nothing