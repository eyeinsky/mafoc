{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DataKinds #-}
module Mafoc.Indexers.Mamba where

import Data.Time (UTCTime)
import Data.Map qualified as M
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple (NamedParam((:=)))
import Database.SQLite.Simple.ToField qualified as SQL
import Data.Set qualified as Set
import Data.ByteString qualified as BS
import Data.Text qualified as TS
import Data.Text.Encoding qualified as TS
import Control.Exception qualified as E
import Control.Concurrent qualified as IO
import Data.Aeson qualified as A
import Servant
 (Get, JSON, (:>), QueryParam, (:<|>)((:<|>)), Capture, FromHttpApiData(parseQueryParam, parseUrlPiece), Handler)

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Cardano.Slotting.Time qualified as Slotting
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.CLI qualified as O
import Mafoc.Core
  ( DbPathAndTableName
  , Indexer (Event, Runtime, State, checkpoint, description, parseCli, toEvents)
  , LocalChainsyncConfig
  , localNodeConnection
  , NodeConfig
  , defaultTableName
  , getCheckpointSqlite
  , initialize
  , initializeLocalChainsync
  , interval
  , persistMany
  , setCheckpointSqlite
  , sqliteOpen
  , sqliteInitCheckpoints
  , loadLatestTrace
  , LedgerEra, slotEra
  , BatchState(BatchState, BatchEmpty, slotNoBhh, NoProgress, chainPointAtStart)
  , IndexerHttpApi(server, API), mkParam, andFilters, query1
  , TxIndexInBlock
  )
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.Exceptions qualified as E
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Indexers.MintBurn qualified as MintBurn
import Mafoc.Indexers.Utxo qualified as Utxo
import Mafoc.Indexers.Datum qualified as Datum
import Mafoc.StateFile qualified as StateFile
import Mafoc.LedgerState qualified as LedgerState
import Mafoc.FieldNameAfterUnderscore (FieldNameAfterUnderscore(FieldNameAfterUnderscore))


data Mamba = Mamba
  { chainsyncConfig :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  , stateFilePrefix :: FilePath
  }
  deriving (Show)

instance Indexer Mamba where
  description = "All mamba indexers"

  parseCli =
    Mamba
      <$> O.mkCommonLocalChainsyncConfig O.commonNodeConnectionAndConfig
      <*> O.commonDbPathAndTableName
      <*> O.stateFilePrefix "mamba"

  data Event Mamba = Event
    { mintBurnEvents :: [Event MintBurn.MintBurn]
    , utxoEvents :: [Event Utxo.Utxo]
    , newEpoch :: Maybe (M.Map C.PoolId C.Lovelace, Event EpochNonce.EpochNonce)
    , datumEvents :: [Event Datum.Datum]
    }
    deriving (Show)

  data Runtime Mamba = Runtime
    { sqlConnection :: SQL.Connection
    , tableMintBurn :: String
    , tableUtxo :: String
    , tableEpochStakepoolSize :: String
    , tableEpochNonce :: String
    , tableDatum :: String
    , ledgerCfg :: LedgerState.ExtLedgerCfg_
    , ledgerStateFile :: FilePath
    , utxoStateFile :: FilePath
    , eraHistory :: C.EraHistory C.CardanoMode
    , systemStart :: C.SystemStart
    }

  data State Mamba = State
    { extLedgerState :: LedgerState.ExtLedgerState_
    , maybeEpochNo :: Maybe C.EpochNo
    , utxoState :: State Utxo.Utxo
    , blockNo :: C.BlockNo -- field just for the GetCurrentSyncedBlock API query
    }

  toEvents Runtime{ledgerCfg} State{extLedgerState, maybeEpochNo, utxoState} blockInMode@(C.BlockInMode (C.Block _ txs) _) = (state', [event])
    where
      extLedgerState' = LedgerState.applyBlock ledgerCfg extLedgerState blockInMode :: LedgerState.ExtLedgerState_
      maybeEpochNo' = LedgerState.getEpochNo $ O.ledgerState extLedgerState'

      utxoState' :: State Utxo.Utxo
      utxoEvents :: [Event Utxo.Utxo]
      (utxoState', utxoEvents) = Utxo.toEventsPrim utxoState Utxo.onUtxoDefault blockInMode
      datumEvents = Datum.toEventsPrim (#slotNo blockInMode) txs

      mintBurnEvents = MintBurn.toEventsPrim (\_ _ -> True) blockInMode :: [Event MintBurn.MintBurn]

      newEpochEvent = case EpochResolution.resolve maybeEpochNo maybeEpochNo' of
        EpochResolution.New epochNo -> let
          epochNonce = LedgerState.getEpochNonce extLedgerState'
          in case LedgerState.getEpochNoAndStakeMap (O.ledgerState extLedgerState') of
             Just (_epochNo, stakeMap) -> Just (stakeMap, EpochNonce.Event epochNo epochNonce (#blockNo blockInMode) (#blockHeaderHash blockInMode) (#slotNo blockInMode))
             Nothing -> undefined -- TODO
        EpochResolution.SameOrAbsent -> Nothing
        EpochResolution.AssumptionBroken exception -> E.throw exception

      event = Event mintBurnEvents utxoEvents newEpochEvent datumEvents

      state' = State extLedgerState' maybeEpochNo' utxoState' $ #blockNo blockInMode

  initialize cli@Mamba{chainsyncConfig, dbPathAndTableName, stateFilePrefix} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
        ledgerStateFile = stateFilePrefix <> "_ledgerState"
        utxoStateFile = stateFilePrefix <> "_utxo"

    ((ledgerCfg, extLedgerState), ledgerStateCp) <- loadLatestTrace ledgerStateFile (LedgerState.init_ nodeConfig) (LedgerState.load nodeConfig) trace
    (utxoState, utxoCp) <- StateFile.loadLatest utxoStateFile
      Utxo.parseState
      (Utxo.initialState <$> LedgerState.getGenesisConfig (#nodeConfig chainsyncConfig))

    let (dbPath, tablePrefix) = defaultTableName "mamba" dbPathAndTableName
        tableMintBurn = tablePrefix <> "_mintburn"
        tableUtxo = tablePrefix <> "_utxo"
        tableEpochStakepoolSize = tablePrefix <> "_epoch_sdd"
        tableEpochNonce = tablePrefix <> "_epoch_nonce"
        tableDatum = tablePrefix <> "_datums"
    sqlConnection <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlConnection
    maybeCheckpointCp <- getCheckpointSqlite sqlConnection "mamba"
    let checkpointCp = fromMaybe C.ChainPointAtGenesis maybeCheckpointCp

    case Set.toList $ Set.fromList [checkpointCp, utxoCp, ledgerStateCp] of
      [] -> E.throwIO $ E.The_impossible_happened "The set can't be empty"
      _ : _ : _ -> E.throwIO $
        E.ChainPoints_don't_match [("checkpoints table", maybeCheckpointCp), ("utxo state", Just utxoCp), ("ledger state", Just ledgerStateCp)]
      [stateCp] -> do
        Utxo.sqliteInit sqlConnection tableUtxo
        MintBurn.sqliteInit sqlConnection tableMintBurn
        EpochStakepoolSize.sqliteInit sqlConnection tableEpochStakepoolSize
        EpochNonce.sqliteInit sqlConnection tableEpochNonce
        Datum.sqliteInit sqlConnection tableDatum

        localChainsyncRuntime <- do
          networkId <- #getNetworkId nodeConfig
          localChainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace $ show cli
          let cliCp = fst $ interval localChainsyncRuntime'
          if cliCp <= stateCp
            then return $ localChainsyncRuntime' {interval = (stateCp, snd $ interval localChainsyncRuntime') }
            else E.throwIO $ E.TextException $
              "Startingpoint specified on the command line is later than the starting point found in indexer state: "
              <> TS.pack (show cliCp) <> " vs " <> TS.pack (show stateCp)

        let lnc = localNodeConnection localChainsyncRuntime
        eraHistory <- either E.throwShow pure =<< C.queryNodeLocalState lnc Nothing (C.QueryEraHistory C.CardanoModeIsMultiEra)
        systemStart <- either E.throwShow pure =<< C.queryNodeLocalState lnc Nothing C.QuerySystemStart
        let state = State extLedgerState (LedgerState.getEpochNo $ O.ledgerState extLedgerState) utxoState 0
            runtime = Runtime{sqlConnection, tableMintBurn, tableUtxo, tableEpochStakepoolSize, tableEpochNonce, tableDatum, ledgerCfg, ledgerStateFile, utxoStateFile, eraHistory, systemStart}
        return (state, localChainsyncRuntime, runtime)

  persistMany Runtime{sqlConnection, tableMintBurn, tableUtxo, tableEpochStakepoolSize, tableEpochNonce, tableDatum, ledgerCfg} events = do
    MintBurn.persistManySqlite sqlConnection tableMintBurn (mintBurnEvents =<< events)
    Utxo.persistManySqlite sqlConnection tableUtxo (utxoEvents =<< events)
    persistMany (Datum.Runtime sqlConnection tableDatum) (datumEvents =<< events)

    let epochEvents = mapMaybe newEpoch events
        essEvents = map (\(essMap, EpochNonce.Event{EpochNonce.epochNo}) -> EpochStakepoolSize.Event epochNo essMap) epochEvents
    persistMany (EpochNonce.Runtime sqlConnection tableEpochNonce ledgerCfg) $ map snd epochEvents
    persistMany (EpochStakepoolSize.Runtime sqlConnection tableEpochStakepoolSize ledgerCfg) essEvents

  checkpoint Runtime{sqlConnection, ledgerCfg, utxoStateFile, ledgerStateFile} State{extLedgerState, utxoState} slotNoBhh = do
    void $ LedgerState.store ledgerStateFile slotNoBhh ledgerCfg extLedgerState -- epochstakepoolsize, epochnonce
    void $ Utxo.storeStateFile utxoStateFile slotNoBhh utxoState
    setCheckpointSqlite sqlConnection "mamba" slotNoBhh

-- * Mamba's REST API

deriving newtype instance FromHttpApiData C.EpochNo
deriving newtype instance FromHttpApiData C.SlotNo

instance FromHttpApiData C.PolicyId where
  parseQueryParam = deserialise C.deserialiseFromRawBytesHex C.AsPolicyId

instance FromHttpApiData C.AssetName where
  parseQueryParam = deserialise C.deserialiseFromRawBytesHex C.AsAssetName

instance FromHttpApiData (C.Address C.ShelleyAddr) where
  parseUrlPiece = first (TS.pack . show) . C.deserialiseFromBech32 C.AsShelleyAddress

instance FromHttpApiData C.TxId where
  parseQueryParam = deserialise C.deserialiseFromRawBytesHex C.AsTxId

deserialise :: Show err => (C.AsType a -> BS.ByteString -> Either err a) -> C.AsType a -> TS.Text -> Either TS.Text a
deserialise with asType = deserialiseBs with asType . TS.encodeUtf8

deserialiseBs :: Show err => (C.AsType a -> BS.ByteString -> Either err a) -> C.AsType a -> BS.ByteString -> Either TS.Text a
deserialiseBs with asType = first (TS.pack . show) . with asType

-- * GetCurrentSyncedBlock

data GetCurrentSyncedBlock = GetCurrentSyncedBlock
  -- optional:
  { getCurrentSyncedBlock_blockNo :: C.BlockNo
  , getCurrentSyncedBlock_blockTimestamp :: UTCTime
  , getCurrentSyncedBlock_blockHeaderHash :: C.Hash C.BlockHeader
  , getCurrentSyncedBlock_slotNo :: C.SlotNo
  , getCurrentSyncedBlock_epochNo :: Maybe C.EpochNo
  , getCurrentSyncedBlock_era :: LedgerEra
  -- , getCurrentSyncedBlock_nodeTip :: C.ChainTip -- TODO
  }
  deriving Generic
  deriving A.ToJSON via FieldNameAfterUnderscore GetCurrentSyncedBlock

-- * GetUtxosFromAddress

data GetUtxosFromAddress = GetUtxosFromAddress
  -- required:
  { getUtxosFromAddress_blockHeaderHash :: C.Hash C.BlockHeader
  , getUtxosFromAddress_slotNo :: C.SlotNo
  , getUtxosFromAddress_blockNo :: C.BlockNo
  , getUtxosFromAddress_txIndexInBlock :: TxIndexInBlock
  , getUtxosFromAddress_txId :: C.TxId
  , getUtxosFromAddress_txIx :: C.TxIx
  , getUtxosFromAddress_txInputs :: [C.TxIn]
  , getUtxosFromAddress_value :: C.Value -- {"lovelace":123, {"somePolicyId":{"someAssetName":345}}}, ADA is represented as "lovelace", and not as empty string
  -- optional:
  , getUtxosFromAddress_datum :: Maybe (C.Hash C.ScriptData)
  , getUtxosFromAddress_datumHash :: Maybe (C.Hash C.ScriptData)
  , getUtxosFromAddress_spentBy :: Maybe (C.SlotNo, C.TxId)
  }
  deriving Generic
  deriving A.ToJSON via FieldNameAfterUnderscore GetUtxosFromAddress

instance SQL.ToField (C.Address C.ShelleyAddr) where
  toField = SQL.SQLBlob . C.serialiseToRawBytes

-- * GetBurnTokensEvents

data GetBurnTokensEvents = GetBurnTokensEvents
  -- required:
  { getBurnTokensEvents_blockHeaderHash :: C.Hash C.BlockHeader
  , getBurnTokensEvents_slotNo :: C.SlotNo
  , getBurnTokensEvents_blockNo :: C.BlockNo
  , getBurnTokensEvents_txId :: C.TxId
  , getBurnTokensEvents_redeemer :: C.ScriptData
  , getBurnTokensEvents_burntAmount :: C.Quantity
  , getBurnTokensEvents_isStable :: Bool
  -- optional: -
  }
  deriving Generic
  deriving A.ToJSON via FieldNameAfterUnderscore GetBurnTokensEvents

-- * GetNonceByEpoch

data GetNonceByEpoch = GetNonceByEpoch
  -- required:
  { getNonceByEpoch_blockHeaderHash :: C.Hash C.BlockHeader
  , getNonceByEpoch_blockNo :: C.BlockNo
  , getNonceByEpoch_epochNo :: C.EpochNo
  , getNonceByEpoch_slotNo :: C.SlotNo
  , getNonceByEpoch_nonce :: Ledger.Nonce
  -- optional: -
  }
  deriving Generic
  deriving A.ToJSON via FieldNameAfterUnderscore GetNonceByEpoch

-- * GetEpochActiveStakeDistribution

data GetEpochActiveStakeDistribution = GetEpochActiveStakeDistribution
  { getEpochActiveStakeDistribution_epochNo :: C.EpochNo
  , getEpochActiveStakeDistribution_slotNo :: C.SlotNo
  , getEpochActiveStakeDistribution_blockHeaderHash :: C.Hash C.BlockHeader
  , getEpochActiveStakeDistribution_blockNo :: C.BlockNo
  , getEpochActiveStakeDistribution_poolSizes :: [(C.PoolId, C.Lovelace)]
  }
  deriving Generic
  deriving A.ToJSON via FieldNameAfterUnderscore GetEpochActiveStakeDistribution

instance IndexerHttpApi Mamba where

  type API Mamba =
         "stub" :> Get '[JSON] ()

    :<|> "getCurrentSyncedBlock"
           :> Get '[JSON] (Maybe GetCurrentSyncedBlock)

    :<|> "getUtxosFromAddress"
           :> Capture "ShelleyAddress" (C.Address C.ShelleyAddr)
           :> QueryParam "createdAtOrAfterSlotNo" C.SlotNo
           :> QueryParam "unspentBeforeSlotNo" C.SlotNo
           :> Get '[JSON] [GetUtxosFromAddress]

    :<|> "getBurnTokensEvents"
           :> Capture "policyId" C.PolicyId
           :> QueryParam "assetName" C.AssetName
           :> QueryParam "createdBeforeSlotNo" C.SlotNo
           :> QueryParam "createdAfterTx" C.TxId
           :> Get '[JSON] [GetBurnTokensEvents]

    :<|> "getNonceByEpoch"
           :> Capture "epochNo" C.EpochNo
           :> Get '[JSON] (Maybe GetNonceByEpoch)

    :<|> "getEpochActiveStakeDistribution"
           :> Capture "epochNo" C.EpochNo
           :> Get '[JSON] (Maybe GetEpochActiveStakeDistribution)

  server Runtime{sqlConnection, tableMintBurn, tableUtxo, tableEpochStakepoolSize, tableEpochNonce, tableDatum, eraHistory, systemStart} mvar =
         return ()
    :<|> getCurrentSyncedBlock
    :<|> getUtxosFromAddress
    :<|> getBurnTokensEvents
    :<|> getNonceByEpoch
    :<|> getEpochActiveStakeDistribution
    where
      getCurrentSyncedBlock :: Handler (Maybe GetCurrentSyncedBlock)
      getCurrentSyncedBlock = do
        (State{maybeEpochNo, blockNo}, batchState) <- liftIO $ IO.readMVar mvar
        let fromSlotNoBhh (slotNo, bhh) = do
              (relativeTime, _) <- either (liftIO . E.throwShow) return (C.getProgress slotNo eraHistory)
              let blockTime = Slotting.fromRelativeTime systemStart relativeTime
              return $ GetCurrentSyncedBlock blockNo blockTime bhh slotNo maybeEpochNo (slotEra slotNo)
        case batchState of
          BatchState{slotNoBhh} -> Just <$> fromSlotNoBhh slotNoBhh
          BatchEmpty{slotNoBhh} -> Just <$> fromSlotNoBhh slotNoBhh
          NoProgress{chainPointAtStart} -> case chainPointAtStart of
            C.ChainPoint slotNo bhh -> Just <$> fromSlotNoBhh (slotNo, bhh)
            C.ChainPointAtGenesis -> return $ Nothing

      getUtxosFromAddress :: C.Address C.ShelleyAddr -> Maybe C.SlotNo -> Maybe C.SlotNo -> Handler [GetUtxosFromAddress]
      getUtxosFromAddress shelleyAddress createdAtOrAfterSlotNo unspentBeforeSlotNo = do
        if | Just atOrAfter <- createdAtOrAfterSlotNo, Just before <- unspentBeforeSlotNo
             -> when (not $ atOrAfter < before) $ fail "createdAtOrAfterSlotNo must be earlier than unspentBeforeSlotNo"
           | otherwise -> return ()
        map fromSql <$> liftIO (SQL.queryNamed sqlConnection query params)
        where
          fromSql ((slotNo, bhh, blockNo, txIndexInBlock, txId, txIx, value, spentBy, spentAt) SQL.:. (maybeDatum, maybeDatumHash)) = let
            spentByAt = Just (spentAt, spentBy)
            in GetUtxosFromAddress bhh slotNo blockNo txIndexInBlock txId txIx [] value maybeDatum maybeDatumHash spentByAt

          query = " SELECT slot_no, block_header_hash, block_no         \
                  \      , tx_index, tx_id, tx_ix                       \
                  \      , value, spent_by, spent_at, datum_hash, datum \
                  \ FROM " <> fromString tableUtxo  <> " u              \
             \ LEFT JOIN " <> fromString tableDatum <> " d ON u.datum_hash = datum.hash \
                 \ WHERE " <> andFilters filters
          (params, filters) = unzip $ (":address" := shelleyAddress, ":address = address") : optionalParams
          optionalParams = catMaybes
            [ mkParam "slot_no >= :atOrAfter" ":atOrAfter" <$> createdAtOrAfterSlotNo
            , mkParam "spent_at < :before" ":before" <$> unspentBeforeSlotNo
            ]

      getBurnTokensEvents
        :: C.PolicyId
        -> Maybe C.AssetName
        -> Maybe C.SlotNo
        -> Maybe C.TxId
        -> Handler [GetBurnTokensEvents]
      getBurnTokensEvents policyId maybeAssetName maybeCreatedBeforeSlotNo maybeCreatedAfterTx =
        map fromSql <$> liftIO (SQL.queryNamed sqlConnection query params)
        where
          fromSql (bhh, blockNo, quantity, redeemer, slotNo, txId) =
            GetBurnTokensEvents bhh slotNo blockNo txId redeemer quantity True
          query = "SELECT block_header_hash, block_no, quantity, redeemer, slot_no, tx_id \
                  \  FROM " <> table
               <> " WHERE " <> andFilters filters
          table = fromString tableMintBurn

          (params, filters) = unzip conditions
          conditions = (":policyId" := policyId, ":policyId = policyId") : optionalParams
          optionalParams = catMaybes
            [ mkParam "slot_no < :slotNo" ":slotNo" <$> maybeCreatedBeforeSlotNo
            , mkParam "asset_name = :assetName" ":assetName" <$> maybeAssetName
            , mkParam
                ("slot_no > (SELECT slot_no FROM " <> table <> " WHERE tx_id = :afterTxId)")
                ":afterTx"
                <$> maybeCreatedAfterTx
            ]

      getNonceByEpoch :: C.EpochNo -> Handler (Maybe GetNonceByEpoch)
      getNonceByEpoch epochNo = fmap fromSql . listToMaybe <$> liftIO (query1 sqlConnection query epochNo)
        where
          fromSql (nonce, blockNo, bhh, slotNo) = GetNonceByEpoch bhh blockNo epochNo nonce slotNo
          query = "SELECT nonce, block_no, block_header_hash, slot_no FROM "
               <> fromString tableEpochNonce
               <> " WHERE epoch_no = ?"

      getEpochActiveStakeDistribution :: C.EpochNo -> Handler (Maybe GetEpochActiveStakeDistribution)
      getEpochActiveStakeDistribution epochNo = do
        stakepoolSizes <- liftIO $ query1 sqlConnection ess epochNo
        maybeFirstBlock <- listToMaybe <$> liftIO (query1 sqlConnection block epochNo)
        return $ case maybeFirstBlock of
          Just (slotNo, bhh, blockNo) -> Just $ GetEpochActiveStakeDistribution epochNo slotNo bhh blockNo stakepoolSizes
          Nothing -> Nothing
        where
          ess = "SELECT pool_id, lovelace FROM "<> fromString tableEpochStakepoolSize <>" WHERE epoch_no = ?"
          block = "SELECT slot_no, block_header_hash, block_no FROM "<> fromString tableEpochNonce <>" WHERE epoch_no = ?"
