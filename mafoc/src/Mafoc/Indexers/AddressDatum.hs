module Mafoc.Indexers.AddressDatum where

import Options.Applicative qualified as Opt
import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_,
                   useSqliteCheckpoint, mkMaybeAddressFilter, setCheckpointSqlite, txAddressDatums, stateless)
import Mafoc.Indexers.Datum qualified as Datum

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

  data Event AddressDatum = Event
    { slotNo :: C.SlotNo
    , addressDatums :: [(C.AddressAny, C.Hash C.ScriptData)]
    , datums :: [Event Datum.Datum]
    }

  newtype State AddressDatum = Stateless ()

  data Runtime AddressDatum = Runtime
    { sqlConnection      :: SQL.Connection
    , tablePrefix        :: String
    , tableAddressDatums :: String
    , tableDatums        :: String
    , maybeAddressFilter :: Maybe (C.Address C.ShelleyAddr -> Bool)
    }

  toEvents Runtime{maybeAddressFilter} _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = (stateless, [event])
    where
      filter' = case maybeAddressFilter of
        Just p -> filter ((\case C.AddressShelley addr -> p addr; _ -> False) . fst)
        Nothing -> id
      slotNo = #slotNo blockInMode
      event = Event
        { slotNo
        , addressDatums = map (second (either id fst)) $ filter' $ txAddressDatums =<< txs
        , datums = Datum.toEventsPrim (#slotNo blockInMode) txs
        }

  initialize AddressDatum{chainsync, dbPathAndTableName, addresses} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tablePrefix) = defaultTableName "address_datums" dbPathAndTableName
        tableAddressDatums = tablePrefix <> "_address_datums"
        tableDatums = tablePrefix <> "_datums"
    (sqlCon, chainsyncRuntime') <- useSqliteCheckpoint dbPath tablePrefix trace chainsyncRuntime
    sqliteInit sqlCon tableAddressDatums tableDatums
    return (stateless, chainsyncRuntime', Runtime sqlCon tablePrefix tableAddressDatums tableDatums (mkMaybeAddressFilter addresses))

  persistMany Runtime{sqlConnection, tableAddressDatums, tableDatums} events = do
    sqliteInsertAddressDatums sqlConnection tableAddressDatums events
    Datum.sqliteInsert sqlConnection tableDatums $ datums =<< events

  checkpoint Runtime{sqlConnection, tablePrefix} _state slotNoBhh = setCheckpointSqlite sqlConnection tablePrefix slotNoBhh

sqliteInit :: SQL.Connection -> String -> String -> IO ()
sqliteInit sqlCon tableAddressDatums tableDatums = do
  sqliteInitAddressDatums sqlCon tableAddressDatums
  Datum.sqliteInit sqlCon tableDatums

sqliteInitAddressDatums :: SQL.Connection -> String -> IO ()
sqliteInitAddressDatums c tableAddressDatums = SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS "
  <> fromString tableAddressDatums
  <> " ( address    TEXT NOT NULL \
     \ , datum_hash BLOB NOT NULL \
     \ , slot_no    INT  NOT NULL )"

sqliteInsertAddressDatums :: SQL.Connection -> String -> [Event AddressDatum] -> IO ()
sqliteInsertAddressDatums c tableName events = SQL.executeMany c template $ toSql =<< events
  where
    toSql Event{slotNo, addressDatums} = map (\(address, datumHash) -> (address, datumHash, slotNo)) addressDatums
    template = "INSERT INTO " <> fromString tableName <> " VALUES (?, ?, ?, ?)" :: SQL.Query
