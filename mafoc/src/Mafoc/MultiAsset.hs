module Mafoc.MultiAsset where

import Data.Map qualified as Map
import Control.Monad (guard)
import Data.List qualified as List
import Data.ByteString.Short qualified as Short
import Data.ByteString qualified as BS

import Crypto.Hash (Digest, hash)
import Crypto.Hash.Algorithms (Blake2b_160)
import Data.ByteArray (convert)

import Cardano.Api qualified as C
import Cardano.Api.SerialiseBech32 qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Ledger.Alonzo.Scripts.Data qualified as LA
import Cardano.Ledger.Alonzo.Tx qualified as LA
import Cardano.Ledger.Alonzo.TxWits qualified as LA
import Cardano.Ledger.Mary.Value qualified as LM
import Cardano.Ledger.Alonzo.Scripts qualified as LA
import Ouroboros.Consensus.Shelley.Eras qualified as OEra
import Cardano.Ledger.Core qualified as Ledger
import Cardano.Ledger.Api.Scripts.Data qualified as Ledger.Api
import Cardano.Ledger.Conway.TxBody qualified as LC
import Cardano.Ledger.Babbage.Tx qualified as LB

import Mafoc.Upstream (TxIndexInBlock)

getEvents
  :: (C.PolicyId -> [()]) -> (C.AssetName -> [()]) -> C.BlockInMode mode
  -> [(C.TxId, TxIndexInBlock, C.PolicyId, C.AssetName, C.Quantity, Maybe (C.ScriptData, C.Hash C.ScriptData))]
getEvents guardPolicy guardAssetName (C.BlockInMode (C.Block _ txs) _) = do
  (bx, tx@(C.Tx (txb :: C.TxBody era) _)) <- zip [0 ..] txs
  let mints = fromMaybe [] $ txMintList txb
      redeemers' = maybe [] (fill 0 . List.sort . mapMaybe (\case (LA.RdmrPtr LA.Mint ix, r) -> Just (ix, r); _ -> Nothing) . Map.toList) $ txRedeemers txb
  ((ledgerPolicyId, nameQuantities), maybeRedeemer) <- zip mints redeemers'
  let policyId = fromMaryPolicyID ledgerPolicyId
  guardPolicy policyId
  (ledgerAssetName, quantity) <- Map.toList nameQuantities
  let assetName = fromMaryAssetName ledgerAssetName
  guardAssetName assetName
  pure
    ( #calculateTxId tx :: C.TxId
    , bx
    , policyId
    , assetName
    , C.Quantity quantity
    , maybeRedeemer
    )

getEvents'
  :: (C.PolicyId -> C.AssetName -> [()]) -> C.BlockInMode mode
  -> [(C.TxId, TxIndexInBlock, C.PolicyId, C.AssetName, C.Quantity, Maybe (C.ScriptData, C.Hash C.ScriptData))]
getEvents' guardAssetId (C.BlockInMode (C.Block _ txs) _) = do
  (bx, tx@(C.Tx (txb :: C.TxBody era) _)) <- zip [0 ..] txs
  let mints = fromMaybe [] $ txMintList txb
      redeemers' = maybe [] (fill 0 . List.sort . mapMaybe (\case (LA.RdmrPtr LA.Mint ix, r) -> Just (ix, r); _ -> Nothing) . Map.toList) $ txRedeemers txb
  ((ledgerPolicyId, nameQuantities), maybeRedeemer) <- zip mints redeemers'
  let policyId = fromMaryPolicyID ledgerPolicyId
  (ledgerAssetName, quantity) <- Map.toList nameQuantities
  let assetName = fromMaryAssetName ledgerAssetName
  guardAssetId policyId assetName
  pure
    ( #calculateTxId tx :: C.TxId
    , bx
    , policyId
    , assetName
    , C.Quantity quantity
    , maybeRedeemer
    )

fill :: Word64 -> [(Word64, a)] -> [Maybe a]
fill n xs = case xs of
  (ix, b) : xs' -> case compare n ix of
    LT -> Nothing : fill (n + 1) xs
    EQ -> Just b : fill (n + 1) xs'
    GT -> error $ "this should never ever happen " <> show (n, ix)
  [] -> repeat Nothing

restrict :: (Alternative f, Eq a) => a -> a -> f ()
restrict a b = guard (a == b)

pass :: Alternative f => a -> f ()
pass = const $ pure ()

txRedeemers :: C.TxBody era -> Maybe (Map.Map LA.RdmrPtr (C.ScriptData, C.Hash C.ScriptData))
txRedeemers txb = case txb of
  C.ShelleyTxBody era _shelleyTx _ txScriptData _ _ -> case era of
    C.ShelleyBasedEraAlonzo -> redeemers txScriptData
    C.ShelleyBasedEraBabbage -> redeemers txScriptData
    C.ShelleyBasedEraConway -> redeemers txScriptData
    _ -> Nothing
  _ -> Nothing

redeemers
  :: forall era
   . ( Ledger.EraCrypto (C.ShelleyLedgerEra era) ~ OEra.StandardCrypto
     , Ledger.Era (C.ShelleyLedgerEra era))
  => C.TxBodyScriptData era -> Maybe (Map.Map LA.RdmrPtr (C.ScriptData, C.Hash C.ScriptData))
redeemers txScriptData = fmap cardanoRedeemer <$> scriptDataRedeemers txScriptData

cardanoRedeemer
  :: (Ledger.EraCrypto era ~ OEra.StandardCrypto, Ledger.Era era)
  => Ledger.Api.Data era -> (C.ScriptData, C.Hash C.ScriptData)
cardanoRedeemer ledgerRedeemer = (fromAlonzoData ledgerRedeemer, C.ScriptDataHash $ Ledger.Api.hashData ledgerRedeemer)

scriptDataRedeemers
  :: Ledger.Era (C.ShelleyLedgerEra era)
  => C.TxBodyScriptData era -> Maybe (Map.Map LA.RdmrPtr (Ledger.Api.Data (C.ShelleyLedgerEra era)))
scriptDataRedeemers txScriptData = case txScriptData of
  C.TxBodyScriptData _proof _datum (LA.Redeemers redeemers') -> Just $ fmap fst redeemers'
  C.TxBodyNoScriptData -> Nothing

txMintList :: C.TxBody era -> Maybe [(LM.PolicyID OEra.StandardCrypto, Map.Map LM.AssetName Integer)]
txMintList txb = Map.toList . coerce <$> txMultiAsset txb


txMultiAsset :: C.TxBody era -> Maybe (LM.MultiAsset OEra.StandardCrypto)
txMultiAsset = \case
  C.ShelleyTxBody era shelleyTx _ _ _ _ -> case era of
    C.ShelleyBasedEraShelley -> Nothing
    C.ShelleyBasedEraAllegra -> Nothing
    C.ShelleyBasedEraMary -> Nothing
    C.ShelleyBasedEraAlonzo -> Just $ LA.atbMint shelleyTx
    C.ShelleyBasedEraBabbage -> Just $ LB.btbMint shelleyTx
    C.ShelleyBasedEraConway -> Just $ LC.ctbMint shelleyTx
  _byronTxBody -> Nothing

-- * Asset fingerprint
--
-- CIP 14 - User-Facing Asset Fingerprint, https://cips.cardano.org/cips/cip14/

newtype AssetFingerprint = AssetFingerprint BS.ByteString
  deriving (Eq, Show)

instance C.SerialiseAsRawBytes AssetFingerprint where
  serialiseToRawBytes (AssetFingerprint bs) = bs
  deserialiseFromRawBytes _ bs = Right (AssetFingerprint bs)

instance C.HasTypeProxy AssetFingerprint where
  data AsType AssetFingerprint = AsAssetFingerprint
  proxyToAsType _ = AsAssetFingerprint

instance C.SerialiseAsBech32 AssetFingerprint where
  bech32PrefixFor _ = "asset"
  bech32PrefixesPermitted AsAssetFingerprint = ["asset"]

assetFingerprint :: C.PolicyId -> C.AssetName -> AssetFingerprint
assetFingerprint policyId assetName = AssetFingerprint digestBs
  where
    bs0 = C.serialiseToRawBytes policyId
    bs1 = C.serialiseToRawBytes assetName
    digest = hash (bs0 <> bs1) :: Digest Blake2b_160
    digestBs = convert digest :: BS.ByteString

-- * Copy-paste

fromMaryPolicyID :: LM.PolicyID OEra.StandardCrypto -> C.PolicyId
fromMaryPolicyID (LM.PolicyID sh) = C.PolicyId (C.fromShelleyScriptHash sh) -- from cardano-api:src/Cardano/Api/Value.hs
fromMaryAssetName :: LM.AssetName -> C.AssetName
fromMaryAssetName (LM.AssetName n) = C.AssetName $ Short.fromShort n -- from cardano-api:src/Cardano/Api/Value.hs
fromAlonzoData :: LA.Data ledgerera -> C.ScriptData
fromAlonzoData = C.fromPlutusData . LA.getPlutusData -- from cardano-api:src/Cardano/Api/ScriptData.hs
