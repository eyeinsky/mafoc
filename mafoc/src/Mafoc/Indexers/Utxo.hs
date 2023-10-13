{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
module Mafoc.Indexers.Utxo where

import Control.Exception qualified as E
import Data.ByteString qualified as BS
import Data.Map qualified as Map
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL

import Cardano.Api qualified as C
import Cardano.Binary qualified as CBOR
import Prettyprinter (Pretty (pretty))

import Mafoc.CLI qualified as O
import Mafoc.Core
  ( CurrentEra, DbPathAndTableName
  , Indexer(Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents)
  , LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, interval, sqliteOpen, traceInfo
  , SlotNoBhh, TxIndexInBlock, maybeDatum
  )
import Mafoc.Upstream (toAddressAny, NodeConfig)
import Mafoc.Utxo (spendTxos, addTxId, TxoEvent, txoEvent, unsafeCastEra, byronGenesisUtxoFromConfig)
import Mafoc.StateFile qualified as StateFile
import Mafoc.Exceptions qualified as E
import Mafoc.LedgerState qualified as LedgerState


data Utxo = Utxo
  { chainsync :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  , stateFilePrefix_ :: FilePath
  }
  deriving (Show)

instance Indexer Utxo where

  description = "Index transaction outputs"

  parseCli =
    Utxo
      <$> O.mkCommonLocalChainsyncConfig O.commonNodeConnectionAndConfig
      <*> O.commonDbPathAndTableName
      <*> O.commonUtxoState

  data Runtime Utxo = Runtime
    { sqlConnection :: SQL.Connection
    , tableName :: String
    , stateFilePrefix :: FilePath
    }

  newtype Event Utxo = Event (Txo Spent)
    deriving (Eq, Show)

  newtype State Utxo = State (EventMap Unspent)
    deriving newtype (Eq, Semigroup, Monoid, CBOR.ToCBOR, CBOR.FromCBOR)

  toEvents _runtime state blockInMode = toEventsPrim state blockInMode

  initialize Utxo{chainsync, dbPathAndTableName, stateFilePrefix_} trace = do

    networkId <- #getNetworkId chainsync
    chainsyncRuntime <- initializeLocalChainsync chainsync networkId trace
    let (dbPath, tableName) = defaultTableName "utxo" dbPathAndTableName
    sqlConnection <- sqliteOpen dbPath
    sqliteInit sqlConnection tableName

    (state, cp) <- StateFile.loadLatest
      stateFilePrefix_
      parseState
      (initialState <$> LedgerState.getGenesisConfig (#nodeConfig chainsync))

    case cp of
      C.ChainPoint{} -> traceInfo trace $ "Found checkpoint: " <> pretty cp
      C.ChainPointAtGenesis -> traceInfo trace $ "No checkpoint found, starting at: " <> pretty cp

    let chainsyncRuntime' = chainsyncRuntime { interval = (cp, snd $ interval chainsyncRuntime) }
    return (state, chainsyncRuntime', Runtime sqlConnection tableName stateFilePrefix_)

  persistMany Runtime{sqlConnection, tableName} events = persistManySqlite sqlConnection tableName events

  checkpoint Runtime{stateFilePrefix} state slotNoBhh = void $ storeStateFile stateFilePrefix slotNoBhh state

-- * Event

toEventsPrim :: State Utxo -> C.BlockInMode era -> (State Utxo, [Event Utxo])
toEventsPrim (State utxos0) blockInMode@(C.BlockInMode (C.Block bh txs) _eim) = (State utxos1, events1)
  where
    slotNo' = #slotNo blockInMode
    (utxos1, _, events1) = foldl step (utxos0, 0, []) txs
    step
      :: forall era . (C.IsCardanoEra era)
      => (EventMap Unspent, TxIndexInBlock, [Event Utxo])
      -> C.Tx era
      -> (EventMap Unspent, TxIndexInBlock, [Event Utxo])
    step (utxos, bx, events) tx =
      ( utxosRemaining <> txosNew'
      , bx + 1
      , map (Event . snd) stxos <> events
      )
      where
        txId = #calculateTxId tx :: C.TxId
        spentTxIns :: [C.TxIn]
        txosNew :: [(C.TxIx, C.TxOut C.CtxTx era)]
        (spentTxIns, txosNew) = txoEvent tx :: TxoEvent era

        txosNew' :: EventMap Unspent
        txosNew' =
          txosNew
            & unsafeCastEra
            & addTxId txId
            & map (\(txIn, txOut) -> let
                event = unspentTxo
                  slotNo'
                  (#blockHeaderHash blockInMode)
                  (#blockNo blockInMode)
                  bx txIn txOut
                in (txIn, event))
            & Map.fromList

        stxos :: [(C.TxIn, Txo Spent)]
        utxosRemaining :: EventMap Unspent
        (stxos, utxosRemaining) = spendTxos ([], utxos) spentTxIns $ \spentTxIn maybeTxo -> case maybeTxo of
          Just txo -> (spentTxIn, spend txId slotNo' txo)
          Nothing -> E.throw $ E.UTxO_not_found spentTxIn (#slotNo bh) (#blockHeaderHash bh)
          -- ^ All spent utxo's must be found in the Utxo set, thus we throw.

data Stxo = Stxo
  { txo :: TxoPrim
  , spentBy :: C.TxId
  , spentAt :: C.SlotNo
  }
  deriving (Eq, Show)

data TxoPrim = Txo
  { slotNo :: C.SlotNo
  , blockHeaderHash :: C.Hash C.BlockHeader
  , blockNo :: C.BlockNo
  , txIndexInBlock :: TxIndexInBlock
  , txIn :: C.TxIn
  , value :: C.Value
  , address :: C.AddressAny
  , datumHash :: Maybe (C.Hash C.ScriptData)
  }
  deriving (Eq, Show)

instance C.ToCBOR TxoPrim where
  toCBOR (Txo{..}) =
       CBOR.toCBOR slotNo
    <> CBOR.toCBOR blockHeaderHash
    <> CBOR.toCBOR blockNo
    <> CBOR.toCBOR txIndexInBlock
    <> CBOR.toCBOR txIn
    <> CBOR.toCBOR value
    <> CBOR.toCBOR address
    <> CBOR.toCBOR datumHash

instance C.FromCBOR TxoPrim where
  fromCBOR = Txo
    <$> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR
    <*> CBOR.fromCBOR

data Status = Spent | Unspent
type family Txo (status :: Status) where
  Txo Spent = Stxo
  Txo Unspent = TxoPrim

spend :: C.TxId -> C.SlotNo -> Txo Unspent -> Txo Spent
spend spentBy spentAt txo = Stxo { txo, spentBy, spentAt }

unspentTxo :: C.SlotNo -> C.Hash C.BlockHeader -> C.BlockNo -> TxIndexInBlock -> C.TxIn -> C.TxOut C.CtxTx CurrentEra -> Txo Unspent
unspentTxo slotNo blockHeaderHash blockNo txIndexInBlock txIn txOut@(C.TxOut a v _ _) =
  Txo
    { slotNo
    , blockHeaderHash
    , blockNo
    , txIndexInBlock
    , txIn
    , value = C.txOutValueToValue v
    , address = toAddressAny a
    , datumHash = either id fst <$> maybeDatum txOut
    }

type EventMap status = Map.Map C.TxIn (Txo status)

-- * State

initialState :: C.GenesisConfig -> State Utxo
initialState genesisConfig = State $ Map.fromList $ map mkEvent list
  where
    (hash, list) = byronGenesisUtxoFromConfig genesisConfig
    mkEvent (txIn, txOutCurrent) = (txIn, unspentTxo 0 hash 0 0 txIn txOutCurrent)
    -- TODO: Byron genesis event: unlike dbsync[1] we set slot and blockNo to zero, is that ok?
    -- [1] cardano-db-sync/cardano-db-sync/src/Cardano/DbSync/Era/Byron/Genesis.hs::194

storeStateFile :: FilePath -> SlotNoBhh -> State Utxo -> IO FilePath
storeStateFile prefix slotNoBhh state =
  StateFile.store prefix slotNoBhh $ \path -> BS.writeFile path $ C.serialiseToCBOR state

parseState :: FilePath -> IO (State Utxo)
parseState path = do
  bs <- BS.readFile path
  case C.deserialiseFromCBOR AsUtxoState bs of
    Left decoderError -> E.throwIO decoderError
    Right eventMap -> return eventMap

instance C.HasTypeProxy (State Utxo) where
  data AsType (State Utxo) = AsUtxoState
  proxyToAsType _ = AsUtxoState

instance C.SerialiseAsCBOR (State Utxo) where
  serialiseToCBOR = CBOR.serialize'
  deserialiseFromCBOR _proxy = CBOR.decodeFull'

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = do
  SQL.execute_ c $
    " CREATE TABLE IF NOT EXISTS      \
    \  "
      <> fromString tableName
      <> "     ( slot_no           INT  NOT NULL \
         \     , block_header_hash BLOB NOT NULL \
         \     , block_no          INT  NOT NULL \
         \     , tx_index          INT  NOT NULL \
         \     , tx_id             TEXT NOT NULL \
         \     , tx_ix             INT  NOT NULL \
         \     , value             BLOB NOT NULL \
         \     , address           TEXT NOT NULL \
         \     , datum_hash        BLOB          \
         \     , spent_by          TEXT NOT NULL \
         \     , spent_at          INT  NOT NULL )"

persistManySqlite :: SQL.Connection -> String -> [Event Utxo] -> IO ()
persistManySqlite sqlConnection tableName events = SQL.executeMany sqlConnection query events
  where
    query = " INSERT INTO " <> fromString tableName <> " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

instance SQL.ToRow (Event Utxo) where
  toRow (Event (Stxo (Txo slotNo bhh blockNo bx (C.TxIn txId_ txIx) address value datumHash) spentBy spentAt)) =
    [ SQL.toField slotNo
    , SQL.toField bhh
    , SQL.toField blockNo
    , SQL.toField bx
    , SQL.toField txId_
    , SQL.toField txIx
    , SQL.toField value
    , SQL.toField address
    , SQL.toField datumHash
    , SQL.toField spentBy
    , SQL.toField spentAt
    ]
