{- | Conventional string formats used in Cardano. -}

{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Upstream.Formats where

import Data.Text qualified as TS
import Data.Aeson.Types qualified as A

import Cardano.Api qualified as C

import Mafoc.Upstream (SlotNoBhh)

-- * SlotNoBhhString

newtype SlotNoBhhString = SlotNoBhhString SlotNoBhh
  deriving newtype (Eq, Ord)

instance A.ToJSON SlotNoBhhString where
  toJSON = A.String . renderSlotNoBhh

instance A.ToJSONKey SlotNoBhhString where
  toJSONKey = A.toJSONKeyText renderSlotNoBhh

renderSlotNoBhh :: SlotNoBhhString -> TS.Text
renderSlotNoBhh (SlotNoBhhString (slotNo, bhh)) = (TS.pack $ show $ coerce @_ @Word64 slotNo) <> ":" <> C.serialiseToRawBytesHexText bhh

-- * AssetIdString

-- | Newtype of AssetId which renders as "policyId.assetName"
newtype AssetIdString = AssetIdString C.AssetId
  deriving newtype (Eq, Ord)

instance A.ToJSON AssetIdString where
  toJSON = A.toJSON . renderAssetId . coerce
instance A.ToJSONKey AssetIdString where
  toJSONKey = A.toJSONKeyText (renderAssetId . coerce)


-- copy-paste from cardano-api:Cardano.Api.Value
renderPolicyId :: C.PolicyId -> TS.Text
renderPolicyId (C.PolicyId scriptHash) = C.serialiseToRawBytesHexText scriptHash

renderAssetId :: C.AssetId -> TS.Text
renderAssetId C.AdaAssetId = "lovelace"
renderAssetId (C.AssetId polId (C.AssetName "")) = renderPolicyId polId
renderAssetId (C.AssetId polId assetName) =
  renderPolicyId polId <> "." <> C.serialiseToRawBytesHexText assetName
