{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeOperators #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Mafoc.Indexers.Utxo where

import Control.Exception qualified as E
import Data.ByteString qualified as BS
import Data.Map qualified as Map
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL

import Cardano.Api qualified as C
import Cardano.Api.Address qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Api.TxIn qualified as C
import Cardano.Api.Value qualified as C
import Cardano.Binary (fromCBOR, toCBOR)
import Cardano.Binary qualified as CBOR
import Cardano.Chain.Block qualified as Byron
import Cardano.Chain.Common qualified as Byron
import Cardano.Chain.Genesis qualified as Byron
import Cardano.Chain.Slotting qualified as Byron
import Cardano.Chain.UTxO qualified as Byron
import Cardano.Crypto qualified as Crypto
import Cardano.Ledger.Core qualified as Ledger
import Cardano.Ledger.Shelley.API qualified as Ledger
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi
import Ouroboros.Consensus.Byron.Ledger qualified as Byron
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O
import Ouroboros.Consensus.Shelley.Ledger qualified as O
import Prettyprinter (Pretty (pretty))

import Mafoc.CLI qualified as O
import Mafoc.Core
  ( CurrentEra, DbPathAndTableName
  , Indexer(Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents)
  , LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, interval, sqliteOpen, traceInfo
  , SlotNoBhh, TxIndexInBlock, maybeDatum
  )
import Mafoc.Upstream (toAddressAny, NodeConfig)
import Mafoc.Utxo (spendTxos, addTxId, TxoEvent, txoEvent, unsafeCastEra, unsafeCastToCurrentEra)
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

  toEvents _runtime (State utxos0) blockInMode@(C.BlockInMode (C.Block bh txs) _eim) = (State utxos1, events1)
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
            Nothing -> E.throw $ E.UTxO_not_found spentTxIn (#blockNo bh) (#slotNo bh) (#blockHeaderHash bh)
            -- ^ All spent utxo's must be found in the Utxo set, thus we throw.

  initialize Utxo{chainsync, dbPathAndTableName, stateFilePrefix_} trace = do

    networkId <- #getNetworkId chainsync
    chainsyncRuntime <- initializeLocalChainsync chainsync networkId trace
    let (dbPath, tableName) = defaultTableName "utxo" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName

    -- (state, cp) <- do
    --   (_, extLedgerState) <- Marconi.getInitialExtLedgerState (coerce (#nodeConfig chainsync :: NodeConfig))
    --   StateFile.loadLatest stateFilePrefix_ parseState (return $ genesisUtxoFromLedgerState extLedgerState)

    (state, cp) <- do
      genesisConfig <- LedgerState.getGenesisConfig (#nodeConfig chainsync)
      StateFile.loadLatest stateFilePrefix_ parseState (return $ byronGenesisUtxoFromConfig genesisConfig)

    case cp of
      C.ChainPoint{} -> traceInfo trace $ "Found checkpoint: " <> pretty cp
      C.ChainPointAtGenesis -> traceInfo trace $ "No checkpoint found, starting at: " <> pretty cp

    let chainsyncRuntime' = chainsyncRuntime { interval = (cp, snd $ interval chainsyncRuntime) }
    return (state, chainsyncRuntime', Runtime sqlCon tableName stateFilePrefix_)

  persistMany Runtime{sqlConnection, tableName} events = persistManySqlite sqlConnection tableName events

  checkpoint Runtime{stateFilePrefix} state slotNoBhh = void $ storeStateFile stateFilePrefix slotNoBhh state

-- * Library

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

-- * Initial Utxo

byronGenesisUtxoFromConfig :: C.GenesisConfig -> State Utxo
byronGenesisUtxoFromConfig (C.GenesisCardano _ byronConfig _ _ _) = State $ Map.fromList $ map (\(txIn, txOut) -> (txIn, mkEvent' txIn txOut)) byronTxOuts
  where
    genesisHash = Byron.configGenesisHash byronConfig :: Byron.GenesisHash
    hash = C.HeaderHash $ Crypto.abstractHashToShort (Byron.unGenesisHash genesisHash)

    mkEvent' txIn txOutCurrent = unspentTxo 0 hash 0 0 txIn txOutCurrent
    -- TODO: Byron genesis event: unlike dbsync[1] we set slot and blockNo to zero, is that ok?
    -- [1] cardano-db-sync/cardano-db-sync/src/Cardano/DbSync/Era/Byron/Genesis.hs::194

    byronTxOuts :: [(C.TxIn, C.TxOut ctx0 CurrentEra)]
    byronTxOuts = byronGenesisAddressBalances byronConfig
      & map byronGenesisBalanceToTxOut
      & map (\(txIn, txOut) -> (txIn, unsafeCastToCurrentEra txOut))

    byronGenesisAddressBalances :: Byron.Config -> [(Byron.Address, Byron.Lovelace)]
    byronGenesisAddressBalances config =
        avvmBalances <> nonAvvmBalances
      where
        networkMagic :: Byron.NetworkMagic
        networkMagic = Byron.makeNetworkMagic (Byron.configProtocolMagic config)
        f = Byron.makeRedeemAddress networkMagic . Crypto.fromCompactRedeemVerificationKey

        avvmBalances :: [(Byron.Address, Byron.Lovelace)]
        avvmBalances = map (\(a, b) -> (f a, b)) $ Map.toList (Byron.unGenesisAvvmBalances $ Byron.configAvvmDistr config)

        nonAvvmBalances :: [(Byron.Address, Byron.Lovelace)]
        nonAvvmBalances =
          Map.toList $ Byron.unGenesisNonAvvmBalances (Byron.configNonAvvmBalances config)

    byronGenesisBalanceToTxOut :: (Byron.Address, Byron.Lovelace) -> (C.TxIn, C.TxOut ctx C.ByronEra)
    byronGenesisBalanceToTxOut (address, value) = (txIn, txOut)
      where
        txIn = C.fromByronTxIn $ Byron.TxInUtxo (addressHash address) 0
        txOut = fromByronTxOut $ Byron.TxOut address value

        addressHash :: Byron.Address -> Crypto.Hash Byron.Tx
        addressHash =
          fromMaybe (E.throw $ E.The_impossible_happened "Hashing addresses from Byron genesis file shouldn't fail")
            . Crypto.abstractHashFromBytes . Crypto.abstractHashToBytes . Crypto.serializeCborHash

genesisUtxoFromLedgerState :: Marconi.ExtLedgerState_ -> State Utxo
genesisUtxoFromLedgerState extLedgerState = State $ case O.ledgerState extLedgerState of
  O.LedgerStateByron (st :: O.LedgerState Byron.ByronBlock) -> ledgerStateEventMapByron st
  O.LedgerStateShelley st -> ledgerStateEventMapShelley C.ShelleyBasedEraShelley st
  O.LedgerStateAllegra st -> ledgerStateEventMapShelley C.ShelleyBasedEraAllegra st
  O.LedgerStateMary st -> ledgerStateEventMapShelley C.ShelleyBasedEraMary st
  O.LedgerStateAlonzo st -> ledgerStateEventMapShelley C.ShelleyBasedEraAlonzo st
  O.LedgerStateBabbage st -> ledgerStateEventMapShelley C.ShelleyBasedEraBabbage st
  O.LedgerStateConway st -> ledgerStateEventMapShelley C.ShelleyBasedEraConway st
  where
    ledgerStateEventMapByron :: O.LedgerState Byron.ByronBlock -> EventMap Unspent
    ledgerStateEventMapByron st = Map.fromList $ map byronTxOutToEvent $ Map.toList $ Byron.unUTxO $ Byron.cvsUtxo $ cvs
      where
        cvs = Byron.byronLedgerState st :: Byron.ChainValidationState
        slotNo = coerce @Byron.SlotNumber @C.SlotNo $ Byron.cvsLastSlot cvs

        byronTxOutToEvent :: (Byron.CompactTxIn, Byron.CompactTxOut) -> (C.TxIn, Txo Unspent)
        byronTxOutToEvent (k, v) = (txIn, event)
          where
            txIn = C.fromByronTxIn $ Byron.fromCompactTxIn k :: C.TxIn
            txOut = fromByronTxOut $ Byron.fromCompactTxOut v
            event = unspentTxo slotNo dummyBlockHeaderHash 0 0 txIn $ unsafeCastToCurrentEra txOut

    ledgerStateEventMapShelley
      :: forall proto lEra era
       . ( O.StandardCrypto ~ Ledger.EraCrypto lEra
         , C.ShelleyLedgerEra era ~ lEra
         , C.IsCardanoEra era
         )
      => C.ShelleyBasedEra era
      -> O.LedgerState (O.ShelleyBlock proto lEra)
      -> EventMap Unspent
    ledgerStateEventMapShelley era st = st
      & O.shelleyLedgerState
      & Ledger.nesEs
      & Ledger.esLState
      & Ledger.lsUTxOState
      & Ledger.utxosUtxo
      & coerce
      & Map.toList
      & map ledgerToCardanoApi
      & Map.fromList
      where
        ledgerToCardanoApi :: (Ledger.TxIn O.StandardCrypto, Ledger.TxOut lEra) -> (C.TxIn, Txo Unspent)
        ledgerToCardanoApi (k, v) = (txIn, event)
          where
            txIn = C.fromShelleyTxIn k
            txOut = C.fromShelleyTxOut era v
            event = mkEvent' txIn $ unsafeCastToCurrentEra txOut

    mkEvent' = unspentTxo 0 dummyBlockHeaderHash 0 0

    dummyBlockHeaderHash :: C.Hash C.BlockHeader
    dummyBlockHeaderHash = fromString "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

-- copy-paste from cardano-api/cardano-api/internal/Cardano/Api/TxBody.hs::731
fromByronTxOut :: Byron.TxOut -> C.TxOut ctx C.ByronEra
fromByronTxOut (Byron.TxOut addr value) =
  C.TxOut
    (C.AddressInEra C.ByronAddressInAnyEra (C.ByronAddress addr))
    (C.TxOutAdaOnly C.AdaOnlyInByronEra (C.fromByronLovelace value))
     C.TxOutDatumNone C.ReferenceScriptNone

storeStateFile :: FilePath -> SlotNoBhh -> State Utxo -> IO FilePath
storeStateFile prefix slotNoBhh state =
  StateFile.store prefix slotNoBhh $ \path -> BS.writeFile path $ C.serialiseToCBOR state

-- * UTXO state

parseState :: FilePath -> IO (State Utxo)
parseState path = do
  bs <- BS.readFile path
  case C.deserialiseFromCBOR AsUtxoState bs of
    Left decoderError -> E.throwIO decoderError
    Right eventMap -> return eventMap

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
instance C.ToCBOR TxoPrim where
  toCBOR (Txo{..}) =
       toCBOR slotNo
    <> toCBOR blockHeaderHash
    <> toCBOR blockNo
    <> toCBOR txIndexInBlock
    <> toCBOR txIn
    <> toCBOR value
    <> toCBOR address
    <> toCBOR datumHash

instance C.FromCBOR TxoPrim where
  fromCBOR = Txo
    <$> fromCBOR
    <*> fromCBOR
    <*> fromCBOR
    <*> fromCBOR
    <*> fromCBOR
    <*> fromCBOR
    <*> fromCBOR
    <*> fromCBOR

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
