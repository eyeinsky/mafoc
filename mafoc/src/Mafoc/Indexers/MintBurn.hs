{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE AllowAmbiguousTypes #-}
{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.
-}
module Mafoc.Indexers.MintBurn where

import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL

import Cardano.Api qualified as C

import Mafoc.CLI qualified as Opt
import Mafoc.Core (
  DbPathAndTableName,
  Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
  LocalChainsyncConfig_,
  defaultTableName,
  initializeLocalChainsync_,
  useSqliteCheckpoint,
  setCheckpointSqlite,
  TxIndexInBlock,
  stateless,
  startSmartFromEra,
  SqliteTable,
  query1
 )
import Mafoc.Upstream (chainsyncStartFor, LedgerEra(Alonzo))
import Mafoc.MultiAsset (pass, restrict, getEvents)

-- | Configuration data type which does double-duty as the tag for the
-- indexer.
data MintBurn = MintBurn
  { chainsync :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  , maybePolicyIdAndAssetName :: Maybe (C.PolicyId, Maybe C.AssetName)
  }
  deriving (Show)

instance Indexer MintBurn where

  description = "Index minting and burning of custom assets"

  parseCli =
    MintBurn
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName
      <*> Opt.commonMaybeAssetId

  data Runtime MintBurn = Runtime
    { sqlConnection :: SQL.Connection
    , tableName :: String
    , toEvents_ :: C.BlockInMode C.CardanoMode -> [Event MintBurn]
    }

  data Event MintBurn = Event
    -- block
    { slotNo :: C.SlotNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    , blockNo :: C.BlockNo
    -- tx
    , txId :: C.TxId
    , txIndex :: TxIndexInBlock
    -- asset
    , policyId :: C.PolicyId
    , assetName :: C.AssetName
    , quantity :: C.Quantity
    , redeemer :: Maybe (C.ScriptData, C.Hash C.ScriptData)
    } deriving (Eq, Ord, Show, Generic)

  newtype State MintBurn = Stateless ()

  toEvents Runtime{toEvents_} _state blockInMode = (stateless, toEvents_ blockInMode)

  initialize cli@MintBurn{chainsync, dbPathAndTableName, maybePolicyIdAndAssetName} trace = do
    chainsync' <- startSmartFromEra Alonzo chainsync trace
    chainsyncRuntime <- initializeLocalChainsync_ chainsync' trace $ show cli
    let (dbPath, tableName) = defaultTableName defaultTable dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- useSqliteCheckpoint dbPath tableName trace chainsyncRuntime
    sqliteInit sqlCon tableName
    let toEvents_ = mkToEvents maybePolicyIdAndAssetName
    return (stateless, chainsyncRuntime', Runtime sqlCon tableName toEvents_)

  persistMany Runtime{sqlConnection, tableName} events = persistManySqlite sqlConnection tableName events

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh

alonzoStart :: C.ChainPoint
alonzoStart = chainsyncStartFor Alonzo

-- * Event

mkToEvents :: Maybe (C.PolicyId, Maybe C.AssetName) -> C.BlockInMode C.CardanoMode -> [Event MintBurn]
mkToEvents = \case
  Just (policyId, Just assetName) -> \blockInMode ->
    map (toEventsHelper blockInMode) $ getEvents (restrict policyId) (restrict assetName) blockInMode
  Just (policyId, Nothing) -> \blockInMode ->
    map (toEventsHelper blockInMode) $ getEvents (restrict policyId) pass blockInMode
  Nothing -> \blockInMode -> map (toEventsHelper blockInMode) $ getEvents pass pass blockInMode

toEventsHelper
  :: C.BlockInMode mode
  -> (C.TxId, TxIndexInBlock, C.PolicyId, C.AssetName, C.Quantity, Maybe (C.ScriptData, C.Hash C.ScriptData))
  -> Event MintBurn
toEventsHelper blockInMode (a, b, c, d, e, f) = Event
  (#slotNo blockInMode) (#blockHeaderHash blockInMode) (#blockNo blockInMode)
  a b c d e f

getAllEvents :: C.BlockInMode mode -> [Event MintBurn]
getAllEvents blockInMode = map (toEventsHelper blockInMode) $ getEvents pass pass blockInMode

-- * SQLite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = liftIO $ do
  let tableName' = fromString tableName
  SQL.execute_ c $
    " CREATE TABLE IF NOT EXISTS " <> tableName' <>
    "   ( slot_no           INT  NOT NULL \
    \   , block_header_hash INT  NOT NULL \
    \   , block_no          INT  NOT NULL \
    \   , tx_id             BLOB NOT NULL \
    \   , tx_index_in_block INT  NOT NULL \
    \   , policy_id         BLOB NOT NULL \
    \   , asset_name        TEXT NOT NULL \
    \   , quantity          INT  NOT NULL \
    \   , redeemer          BLOB          \
    \   , redeemer_hash     BLOB          )"

persistManySqlite :: SQL.Connection -> String -> [Event MintBurn] -> IO ()
persistManySqlite sqlConnection tableName events =
  SQL.executeMany sqlConnection template events
  where
    template = "INSERT INTO " <> fromString tableName
        <> " ( slot_no, block_header_hash, block_no \
           \ , tx_id, tx_index_in_block             \
           \ , policy_id, asset_name, quantity      \
           \ , redeemer, redeemer_hash              \
           \ ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

instance SQL.ToRow (Event MintBurn) where
  toRow Event{..} =
    SQL.toRow
      [ SQL.toField slotNo
      , SQL.toField blockHeaderHash
      , SQL.toField blockNo
      , SQL.toField txId
      , SQL.toField txIndex
      , SQL.toField policyId
      , SQL.toField assetName
      , SQL.toField quantity
      , SQL.toField $ fst <$> redeemer
      , SQL.toField $ snd <$> redeemer
      ]

instance SQL.FromRow (Event MintBurn) where
  fromRow = Event <$> SQL.field <*> SQL.field <*> SQL.field <*> SQL.field <*> SQL.field <*> SQL.field <*> SQL.field <*> SQL.field <*> redeemer
    where
      redeemer = parseRedeemer <$> SQL.field <*> SQL.field

parseRedeemer :: Maybe a -> Maybe b -> Maybe (a, b)
parseRedeemer mrd mrh = if
  | Just rd <- mrd, Just rh <- mrh -> Just (rd, rh)
  | Nothing <- mrd, Nothing <- mrh -> Nothing
  | otherwise -> fail "Either redeemer data and hash are present or they are not. One can't be present and the other one not!"

defaultTable :: String
defaultTable = "mintburn"

queryAtSlot :: C.SlotNo -> SqliteTable -> IO [Event MintBurn]
queryAtSlot slotNo (con, table) = query1 con query' slotNo
  where
    query' =
      "SELECT slot_no, block_header_hash, block_no, tx_id, tx_index_in_block \
      \     , policy_id, asset_name, quantity, redeemer, redeemer_hash \
      \  FROM " <> fromString table <>
      " WHERE slot_no = ?"

-- slot no and block no are null in dbsync but not null in mafoc
type ResultRow =
  ( Maybe C.SlotNo
  , C.Hash C.BlockHeader
  , Maybe C.BlockNo
  , TxIndexInBlock
  , C.TxId
  , C.PolicyId
  , C.AssetName
  , C.Quantity
  , Maybe C.ScriptData
  , Maybe (C.Hash C.ScriptData)
  )
