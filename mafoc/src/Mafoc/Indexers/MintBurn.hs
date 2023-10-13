{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}

{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.
-}
module Mafoc.Indexers.MintBurn where

import Control.Monad (guard)
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import Data.List qualified as List
import Data.Map qualified as Map

import Cardano.Ledger.Babbage.Tx qualified as LB
import Cardano.Ledger.Alonzo.Scripts.Data qualified as LA
import Cardano.Ledger.Alonzo.Tx qualified as LA
import Cardano.Ledger.Alonzo.TxWits qualified as LA
import Cardano.Ledger.Mary.Value qualified as LM
import Data.ByteString.Short qualified as Short
import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Ouroboros.Consensus.Shelley.Eras qualified as OEra
import Cardano.Ledger.Mary.Value qualified as LA
import Cardano.Ledger.Api.Scripts.Data qualified as Ledger.Api
import Cardano.Ledger.Core qualified as Ledger
import Cardano.Ledger.Conway.TxBody qualified as LC

import Mafoc.CLI qualified as Opt
import Mafoc.Core (
  DbPathAndTableName,
  Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
  LocalChainsyncConfig_,
  defaultTableName,
  initializeLocalChainsync_,
  initializeSqlite,
  setCheckpointSqlite,
  modifyStartingPoint,
  TxIndexInBlock,
 )

{- | Configuration data type which does double-duty as the tag for the
 indexer.
-}
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
    , assetFilter :: C.PolicyId -> C.AssetName -> Bool
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
    }
    deriving Show

  data State MintBurn = EmptyState

  toEvents Runtime{assetFilter} _state blockInMode = (EmptyState, toEventsPrim assetFilter blockInMode)

  initialize MintBurn{chainsync, dbPathAndTableName, maybePolicyIdAndAssetName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace
    let (dbPath, tableName) = defaultTableName "mintburn" dbPathAndTableName
    (sqlCon, checkpointedChainPoint) <- initializeSqlite dbPath tableName
    sqliteInit sqlCon tableName
    let chainsyncRuntime' = modifyStartingPoint chainsyncRuntime (\cliChainPoint -> max checkpointedChainPoint cliChainPoint)
        assetFilter = case maybePolicyIdAndAssetName of
          Just (policyId, Just assetName) -> \policyId' assetName' -> policyId' == policyId && assetName' == assetName
          Just (policyId, Nothing)        -> \policyId' _          -> policyId' == policyId
          Nothing                         -> \_         _          -> True
    return (EmptyState, chainsyncRuntime', Runtime sqlCon tableName assetFilter)

  persistMany Runtime{sqlConnection, tableName} events = persistManySqlite sqlConnection tableName events

  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh

-- | Separate from class for reuse: this way we don't need to add
-- dummy runtime and state when we want to call create MintBurn events
-- outside of this indexer.
toEventsPrim :: (C.PolicyId -> C.AssetName -> Bool) -> C.BlockInMode C.CardanoMode -> [Event MintBurn]
toEventsPrim assetFilter blockInMode@(C.BlockInMode (C.Block _ txs) _) = do
  (bx, C.Tx txb _) <- zip [0 ..] txs
  (policyId, assetName, quantity, maybeRedeemer) <- case txb of
    C.ShelleyTxBody era shelleyTx _ _ _ _ -> case era of
      C.ShelleyBasedEraShelley -> []
      C.ShelleyBasedEraAllegra -> []
      C.ShelleyBasedEraMary -> []
      C.ShelleyBasedEraAlonzo -> getPolicyData txb $ LA.atbMint shelleyTx
      C.ShelleyBasedEraBabbage -> getPolicyData txb $ LB.btbMint shelleyTx
      C.ShelleyBasedEraConway -> getPolicyData txb $ LC.ctbMint shelleyTx
    _byronTxBody -> [] -- ByronTxBody is not exported but as it's the only other data constructor then _ matches it.
  guard $ assetFilter policyId assetName -- TODO optimize away
  pure $ Event
    (#slotNo blockInMode) (#blockHeaderHash blockInMode) (#blockNo blockInMode)
    (#calculateTxId txb) bx
    policyId assetName quantity maybeRedeemer


-- * SQLite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = liftIO $ do
  let tableName' = fromString tableName
  SQL.execute_ c $
    " CREATE TABLE IF NOT EXISTS " <> tableName' <>
    "   ( slot_no           INT  NOT NULL \
    \   , block_header_hash INT  NOT NULL \
    \   , block_no          INT  NOT NULL \
    \   , tx_index_in_block INT  NOT NULL \
    \   , tx_id             BLOB NOT NULL \
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
           \ , tx_index_in_block, tx_id             \
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

-- * Get policy data

getPolicyData
  :: forall era
   . (Ledger.Era (C.ShelleyLedgerEra era), OEra.EraCrypto (C.ShelleyLedgerEra era) ~ OEra.StandardCrypto)
  => C.TxBody era
  -> LM.MultiAsset OEra.StandardCrypto
  -> [(C.PolicyId, C.AssetName, C.Quantity, Maybe (C.ScriptData, C.Hash C.ScriptData))]
getPolicyData txb m = do
  let txRedeemers :: C.TxBody era -> [(LA.RdmrPtr, LB.Data (C.ShelleyLedgerEra era))]
      txRedeemers (C.ShelleyTxBody _ _ _ txScriptData _ _) = case txScriptData of
        C.TxBodyScriptData _proof _datum (LA.Redeemers redeemers) -> Map.toList $ fmap fst redeemers
        C.TxBodyNoScriptData -> mempty
      txRedeemers _ = mempty
      findRedeemerByIndex ix (LA.RdmrPtr _ w, _) = w == ix
      findRedeemer ix = snd <$> List.find (findRedeemerByIndex ix) (txRedeemers txb)
      toAssetRedeemer r = (fromAlonzoData r, C.ScriptDataHash $ Ledger.Api.hashData r)
  (ix, (policyId', assetName, quantity)) <- zip [0 ..] $ LA.flattenMultiAsset m
  pure
    ( fromMaryPolicyID policyId'
    , fromMaryAssetName assetName
    , C.Quantity quantity
    , toAssetRedeemer <$> findRedeemer ix
    )

  where
    -- Copy-paste:
    fromMaryPolicyID :: LM.PolicyID OEra.StandardCrypto -> C.PolicyId
    fromMaryPolicyID (LM.PolicyID sh) = C.PolicyId (C.fromShelleyScriptHash sh) -- from cardano-api:src/Cardano/Api/Value.hs
    fromMaryAssetName :: LM.AssetName -> C.AssetName
    fromMaryAssetName (LM.AssetName n) = C.AssetName $ Short.fromShort n -- from cardano-api:src/Cardano/Api/Value.hs
    fromAlonzoData :: LA.Data ledgerera -> C.ScriptData
    fromAlonzoData = C.fromPlutusData . LA.getPlutusData -- from cardano-api:src/Cardano/Api/ScriptData.hs
