{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TupleSections #-}
module Mafoc.Indexers.Datum where

import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C

import Mafoc.Core
  ( Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents)
  , DbPathAndTableName, defaultTableName, initializeSqlite, setCheckpointSqlite
  , LocalChainsyncConfig_, initializeLocalChainsync_, modifyStartingPoint, TxIndexInBlock, maybeDatum, txPlutusDatums
  )
import Mafoc.CLI qualified as O


data Datum = Datum
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer Datum where

  description = "Index datums by hash"

  parseCli = Datum
    <$> O.commonLocalChainsyncConfig
    <*> O.commonDbPathAndTableName

  data Event Datum = Event
    { hash :: C.Hash C.ScriptData
    , datum :: C.ScriptData
    , slotNo :: C.SlotNo
    , tbx :: TxIndexInBlock
    , txIx :: Maybe C.TxIx
    }
    deriving Show

  data State Datum = EmptyState

  data Runtime Datum = Runtime
    { sqlConnection      :: SQL.Connection
    , tableName          :: String
    }

  toEvents _runtime _state  blockInMode@(C.BlockInMode (C.Block _ txs) _) = (EmptyState, toEventsPrim (#slotNo blockInMode) txs)

  initialize Datum{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tableName) = defaultTableName "datums" dbPathAndTableName
    (sqlCon, checkpointedChainPoint) <- initializeSqlite dbPath tableName
    sqliteInit sqlCon tableName
    let chainsyncRuntime' = modifyStartingPoint chainsyncRuntime (\cliChainPoint -> max checkpointedChainPoint cliChainPoint)
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName events

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh

toEventsPrim :: C.SlotNo -> [C.Tx era] -> [Event Datum]
toEventsPrim slotNo txs = concat $ zipWith doTx [0 ..] txs
  where
    doTx :: TxIndexInBlock -> C.Tx era -> [Event Datum]
    doTx tbx tx@(C.Tx (C.TxBody C.TxBodyContent{C.txOuts}) _) = bodyDatums <> txOutDatums
      where
        bodyDatums :: [Event Datum]
        bodyDatums = map (\(hash, datum) -> Event hash datum slotNo tbx Nothing) $ txPlutusDatums tx

        txOutDatums :: [Event Datum]
        txOutDatums = catMaybes $ zipWith maybeTxOutDatum [C.TxIx 0 ..] txOuts

        maybeTxOutDatum ix out = case maybeDatum out of
          Just (Right (hash, datum)) -> Just $ Event hash datum slotNo tbx (Just ix)
          Just (Left _hash) -> Nothing
          Nothing -> Nothing

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $
  "CREATE TABLE IF NOT EXISTS "
  <> fromString tableName
  <> " ( hash        BLOB NOT NULL  \
     \ , datum       BLOB NOT NULL  \
     \ , slot_no     INT NOT NULL   \
     \ , tbx         INT NOT NULL   \
     \ , tx_ix_maybe INT           )"

sqliteInsert :: SQL.Connection -> String -> [Event Datum] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c template $ map toSql events
  where
    template = "INSERT INTO " <> fromString tableName <> " VALUES (?, ?, ?, ?, ?)" :: SQL.Query
    toSql Event{hash, datum, slotNo, tbx, txIx} = (hash, datum, slotNo, tbx, txIx)
