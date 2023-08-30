{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Mafoc.Indexers.Utxo where

import Control.Exception qualified as E
import Data.ByteString qualified as BS
import Data.Either (rights)
import Data.Map qualified as Map
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Binary (fromCBOR, toCBOR)
import Cardano.Binary qualified as CBOR
import Prettyprinter (Pretty (pretty))

import Mafoc.CLI qualified as O
import Mafoc.Core
  ( CurrentEra, DbPathAndTableName
  , Indexer(Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents)
  , LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync, sqliteOpen, traceInfo
  )
import Mafoc.Upstream (toAddressAny)
import Mafoc.Utxo (spendTxos, addTxId, TxoEvent, txoEvent, unsafeCastEra)
import Mafoc.StateFile qualified as StateFile
import Marconi.ChainIndex.Extract.Datum qualified as Datum

data Utxo = Utxo
  { chainsync :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  , stateFilePrefix_ :: FilePath
  }
  deriving (Show)

instance Indexer Utxo where

  description = "Index transaction outputs"

  parseCli =
    Utxo
      <$> O.commonLocalChainsyncConfig
      <*> O.commonDbPathAndTableName
      <*> O.commonUtxoState

  data Runtime Utxo = Runtime
    { sqlConnection :: SQL.Connection
    , tableName :: String
    , stateFilePrefix :: FilePath
    }

  data Event Utxo = Event
    { slotNo :: C.SlotNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    , txIndexInBlock :: Word64
    , txIn :: C.TxIn
    , value :: C.Value
    , address :: C.AddressAny
    , datum :: Maybe C.ScriptData
    , datumHash :: Maybe (C.Hash C.ScriptData)
    }
    deriving (Eq, Show)

  newtype State Utxo = State EventMap
    deriving newtype (Eq, Semigroup, Monoid, C.ToCBOR, C.FromCBOR)

  toEvents _runtime (State utxos0) blockInMode = (State utxos1, events1)
    where
      mkEvent :: BlockIndex -> (C.TxIn, C.TxOut C.CtxTx CurrentEra) -> Event Utxo
      mkEvent bx (txIn, C.TxOut a v txOutDatum _refScript) =
        Event
          { slotNo = #slotNo blockInMode
          , blockHeaderHash = #blockHeaderHash blockInMode
          , txIndexInBlock = bx
          , txIn
          , value = C.txOutValueToValue v
          , address = toAddressAny a
          , datum
          , datumHash
          }
        where
          (datum, datumHash) = case Datum.getTxOutDatumOrHash txOutDatum of
            Nothing -> (Nothing, Nothing)
            Just e -> case e of
              Left hash -> (Nothing, Just hash)
              Right (datumHash'', datum'') -> (Just datum'', Just datumHash'')

      (utxos1, _, events1) = case blockInMode of
        (C.BlockInMode (C.Block _BlockHeader txs) _eim) -> foldl step (utxos0, 0, []) txs
          where
            step
              :: forall era
               . (C.IsCardanoEra era)
              => (EventMap, Word64, [Event Utxo])
              -> C.Tx era
              -> (EventMap, Word64, [Event Utxo])
            step (utxos, bx, events) tx =
              ( utxosRemaining <> txosNew'
              , bx + 1
              , events'
              )
              where
                txId = #calculateTxId tx :: C.TxId
                spentTxIns :: [C.TxIn]
                txosNew :: [(C.TxIx, C.TxOut C.CtxTx era)]
                (spentTxIns, txosNew) = txoEvent tx :: TxoEvent era

                txosNew' :: Map.Map C.TxIn (Event Utxo)
                txosNew' =
                  txosNew
                    & unsafeCastEra
                    & addTxId txId
                    & map (\(txIn, txOut) -> (txIn, mkEvent bx (txIn, txOut)))
                    & Map.fromList

                stxos :: [Either C.TxIn (C.TxIn, Event Utxo)]
                utxosRemaining :: Map.Map C.TxIn (Event Utxo)
                (stxos, utxosRemaining) = spendTxos ([], utxos) spentTxIns $ \txIn maybeTxo -> case maybeTxo of
                  Just txo -> Right (txIn, txo)
                  Nothing -> Left txIn
                -- This is the full UTxO indexer so @utxos@ has all
                -- unspent transaction outputs, thus the txIn spent
                -- must be found within it.

                events' = map snd (rights stxos) <> events

  initialize Utxo{chainsync, dbPathAndTableName, stateFilePrefix_} trace = do
    networkId <- #getNetworkId chainsync
    chainsyncRuntime <- initializeLocalChainsync chainsync networkId
    let (dbPath, tableName) = defaultTableName "utxo" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName
    (state, cp) <- StateFile.loadLatest stateFilePrefix_ parseState (return mempty)
    case cp of
      C.ChainPoint{} -> traceInfo trace $ "Found checkpoint: " <> pretty cp
      C.ChainPointAtGenesis -> traceInfo trace $ "No checkpoint found, starting at: " <> pretty cp
    return (state, chainsyncRuntime, Runtime sqlCon tableName stateFilePrefix_)

  persistMany Runtime{sqlConnection, tableName} events = SQL.executeMany sqlConnection query events
    where
      query =
        " INSERT INTO "
          <> fromString tableName
          <> " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

  checkpoint Runtime{stateFilePrefix} state slotNoBhh = do
    storeState state $ StateFile.toName stateFilePrefix slotNoBhh
    StateFile.keepLatestTwo stateFilePrefix

-- * UTXO state

parseState :: FilePath -> IO (State Utxo)
parseState path = do
  bs <- BS.readFile path
  case C.deserialiseFromCBOR AsUtxoState bs of
    Left decoderError -> E.throwIO decoderError
    Right eventMap -> return eventMap

storeState :: State Utxo -> FilePath -> IO ()
storeState state path = store
  where
    serialised = C.serialiseToCBOR state
    store = BS.writeFile path serialised
    _store = case C.deserialiseFromCBOR AsUtxoState serialised of
       Right state' -> E.assert (state == state') $ BS.writeFile path serialised
       Left err -> E.throwIO $ userError $ "Roundtrip error: " <> show err

-- * ToCBOR/FromCBOR instances

-- ** State Utxo

instance C.HasTypeProxy (State Utxo) where
  data AsType (State Utxo) = AsUtxoState
  proxyToAsType _ = AsUtxoState

instance C.SerialiseAsCBOR (State Utxo) where
  serialiseToCBOR = CBOR.serialize'
  deserialiseFromCBOR _proxy = CBOR.decodeFull'

-- | Hash BlockHeader
deriving via C.UsingRawBytes (C.Hash C.BlockHeader) instance C.ToCBOR (C.Hash C.BlockHeader)

deriving via C.UsingRawBytes (C.Hash C.BlockHeader) instance C.FromCBOR (C.Hash C.BlockHeader)

-- | C.TxIn
deriving via C.UsingRawBytes C.TxId instance C.ToCBOR C.TxId

deriving via C.UsingRawBytes C.TxId instance C.FromCBOR C.TxId
deriving newtype instance C.ToCBOR C.TxIx
deriving newtype instance C.FromCBOR C.TxIx
instance C.ToCBOR C.TxIn where toCBOR (C.TxIn txId txIx) = toCBOR (txId, txIx)
instance C.FromCBOR C.TxIn where fromCBOR = uncurry C.TxIn <$> fromCBOR

-- | Value
deriving newtype instance C.ToCBOR C.Quantity

deriving newtype instance C.FromCBOR C.Quantity
deriving newtype instance C.ToCBOR C.AssetName
deriving newtype instance C.FromCBOR C.AssetName
deriving via C.UsingRawBytes C.PolicyId instance C.ToCBOR C.PolicyId
deriving via C.UsingRawBytes C.PolicyId instance C.FromCBOR C.PolicyId
instance C.ToCBOR C.AssetId where
  toCBOR =
    toCBOR . \case
      C.AdaAssetId -> Nothing
      C.AssetId policyId assetName -> Just (policyId, assetName)
instance C.FromCBOR C.AssetId where
  fromCBOR = maybeToAssetId <$> fromCBOR
    where
      maybeToAssetId = \case
        Nothing -> C.AdaAssetId
        Just (policyId, assetName) -> C.AssetId policyId assetName
instance C.ToCBOR C.Value where
  toCBOR = toCBOR . C.valueToList
instance C.FromCBOR C.Value where
  fromCBOR = C.valueFromList <$> fromCBOR

-- | AddressAny
deriving via C.UsingRawBytes C.AddressAny instance C.ToCBOR C.AddressAny

deriving via C.UsingRawBytes C.AddressAny instance C.FromCBOR C.AddressAny

-- | Hash ScriptData
deriving via C.UsingRawBytes (C.Hash C.ScriptData) instance C.ToCBOR (C.Hash C.ScriptData)

deriving via C.UsingRawBytes (C.Hash C.ScriptData) instance C.FromCBOR (C.Hash C.ScriptData)

-- | Event
instance C.ToCBOR (Event Utxo) where
  toCBOR e =
    toCBOR
      ( slotNo e
      , blockHeaderHash e
      , txIndexInBlock e
      , txIn e
      , value e
      , address e
      , datum e
      , datumHash e
      )

instance C.FromCBOR (Event Utxo) where
  fromCBOR = do
    (a, b, c, d, e, f, g, h) <- CBOR.fromCBOR
    return $ Event a b c d e f g h

-- * Event

type BlockIndex = Word64
type EventMap = Map.Map C.TxIn (Event Utxo)

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = do
  SQL.execute_ c $
    " CREATE TABLE IF NOT EXISTS      \
    \  "
      <> fromString tableName
      <> " \
         \     ( slot_no INT NOT NULL      \
         \     , block_hash BLOB NOT NULL  \
         \     , tx_index INT NOT NULL     \
         \     , tx_id TEXT NOT NULL       \
         \     , tx_ix INT NOT NULL        \
         \     , value BLOB                \
         \     , address TEXT NOT NULL     \
         \     , datum BLOB                \
         \     , datum_hash BLOB          )"

instance {-# OVERLAPPING #-} SQL.ToRow (Event Utxo) where
  toRow (Event slotNo bhh bx (C.TxIn txId txIx) address value datum datumHash) =
    [ SQL.toField slotNo
    , SQL.toField bhh
    , SQL.toField bx
    , SQL.toField txId
    , SQL.toField txIx
    , SQL.toField value
    , SQL.toField address
    , SQL.toField datum
    , SQL.toField datumHash
    ]
