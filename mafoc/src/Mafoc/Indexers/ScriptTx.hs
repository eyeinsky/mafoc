module Mafoc.Indexers.ScriptTx where

import Database.SQLite.Simple qualified as SQL
import Data.List.NonEmpty qualified as NE
import Database.SQLite.Simple.FromField qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import Data.ByteString qualified as BS

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_,
                   useSqliteCheckpoint, setCheckpointSqlite, stateless,
                   LedgerEra(Shelley), startSmartFromEra
                  )


data ScriptTx = ScriptTx
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer ScriptTx where

  description = "Index transactions with scripts"

  parseCli = ScriptTx
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName

  data Event ScriptTx = Event
    { slotNo :: C.SlotNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    , txCbor :: TxCbor
    , scriptHashes :: NE.NonEmpty C.ScriptHash
    } deriving (Show)

  data Runtime ScriptTx = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }

  newtype State ScriptTx = Stateless ()

  toEvents _runtime _state blockInMode@(C.BlockInMode (C.Block _ txs :: C.Block era) _) = (stateless, mapMaybe doTx txs)
    where
      event = Event (#slotNo blockInMode) (#blockHeaderHash blockInMode)
      doTx tx = event (TxCbor $ C.serialiseToCBOR tx) <$> txScriptHashes tx

  initialize cli@ScriptTx{chainsync, dbPathAndTableName} trace = do
    chainsync' <- startSmartFromEra Shelley chainsync trace
    chainsyncRuntime <- initializeLocalChainsync_ chainsync' trace $ show cli
    let (dbPath, tableName) = defaultTableName defaultTable dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- useSqliteCheckpoint dbPath tableName trace chainsyncRuntime
    return (stateless, chainsyncRuntime', Runtime sqlCon tableName)

  persistMany Runtime{sqlConnection, tableName} events =
    sqliteInsert sqlConnection tableName events

  checkpoint Runtime{sqlConnection, tableName} _state t = setCheckpointSqlite sqlConnection tableName t

newtype TxCbor = TxCbor BS.ByteString
  deriving (Eq, Show)
  deriving newtype (SQL.ToField, SQL.FromField)

txScriptHashes :: forall era. C.Tx era -> Maybe (NE.NonEmpty C.ScriptHash)
txScriptHashes (C.Tx body _ws) = NE.nonEmpty $ catMaybes hashesMaybe
  where
    hashesMaybe :: [Maybe C.ScriptHash]
    hashesMaybe = case body of
      C.ShelleyTxBody shelleyBasedEra _ scripts _ _ _ ->
        flip map scripts $ \script ->
          case C.fromShelleyBasedScript shelleyBasedEra script of
            C.ScriptInEra _ script' -> Just $ C.hashScript script'
      _ -> [] -- Byron transactions have no scripts

-- * Sqlite

defaultTable :: String
defaultTable = "scripttx"

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $ "CREATE TABLE IF NOT EXISTS " <> fromString tableName <>
  " ( scriptAddress TEXT NOT NULL \
  \ , txCbor        BLOB NOT NULL \
  \ , slotNo        INT NOT NULL  \
  \ , blockHash     BLOB NOT NULL )"

sqliteInsert :: SQL.Connection -> String -> [Event ScriptTx] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c query $ do
  Event slotNo bhh txCbor scriptHashes <- events
  scriptHash <- NE.toList scriptHashes
  pure (scriptHash, txCbor, slotNo, bhh)
  where
    query = "INSERT INTO " <> fromString tableName <> " (scriptAddress, txCbor, slotNo, blockHash) VALUES (?, ?, ?, ?)"
