{-# LANGUAGE RecordWildCards #-}
module Mafoc.Indexers.AddressAppears where

import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import Data.Aeson qualified as A

import Cardano.Api qualified as C

import Mafoc.CLI qualified as O
import Mafoc.Utxo (txoEvent)
import Mafoc.Core
  ( Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents), stateless
  , LocalChainsyncConfig_ , initializeLocalChainsync_
  , DbPathAndTableName, defaultTableName, initializeSqlite, setCheckpointSqlite, ensureStartFromCheckpoint
  )
import Mafoc.Upstream (toAddressAny)


data AddressAppears = AddressAppears
  { chainsync :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer AddressAppears where
  description = "Index the first appearance of an address"

  parseCli = AddressAppears
    <$> O.commonLocalChainsyncConfig
    <*> O.commonDbPathAndTableName

  newtype State AddressAppears = State ()

  data Runtime AddressAppears = Runtime
    { persistMany_ :: [Event AddressAppears] -> IO ()
    , tableName :: String
    , sqlConnection :: SQL.Connection
    }

  data Event AddressAppears = Event
    { slotNo :: C.SlotNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    , address :: C.AddressAny
    } deriving (Show, Generic, A.ToJSON)

  initialize AddressAppears{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tableName) = defaultTableName defaultTable dbPathAndTableName
    (sqlConnection, checkpointedChainPoint) <- initializeSqlite dbPath tableName
    sqliteInit sqlConnection tableName
    chainsyncRuntime' <- ensureStartFromCheckpoint chainsyncRuntime checkpointedChainPoint
    let persistMany_ = persistManySqlite sqlConnection tableName
    return (stateless, chainsyncRuntime', Runtime{persistMany_, tableName, sqlConnection})

  toEvents _runtime _state (C.BlockInMode (C.Block (C.BlockHeader slotNo blockHeaderHash _) txs :: C.Block era) _) =
    (stateless, map (\address -> Event {slotNo, blockHeaderHash, address}) addresses)
    where
      addresses = do
        txOuts <- map (snd . txoEvent) txs
        (_, C.TxOut addressInEra _ _ _) <- txOuts
        pure $ toAddressAny addressInEra

  persistMany Runtime{persistMany_} events = persistMany_ events

  checkpoint Runtime{tableName, sqlConnection} _state slotNoBhh = do
    setCheckpointSqlite sqlConnection tableName slotNoBhh

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = liftIO $ SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS " <> tableName' <>
  "   ( slot_no           INT  NOT NULL \
  \   , block_header_hash BLOB NOT NULL \
  \   , address           BLOB NOT NULL UNIQUE)"
  where tableName' = fromString tableName

persistManySqlite :: SQL.Connection -> String -> [Event AddressAppears] -> IO ()
persistManySqlite sqlConnection tableName events = SQL.executeMany sqlConnection query events
  where
    query = "INSERT OR IGNORE INTO " <> fromString tableName <> " (slot_no, block_header_hash, address) VALUES (?, ?, ?)"

defaultTable :: String
defaultTable = "addressappears"

instance SQL.ToRow (Event AddressAppears) where
  toRow (Event{..}) =
    [ SQL.toField slotNo
    , SQL.toField blockHeaderHash
    , SQL.toField address
    ]
