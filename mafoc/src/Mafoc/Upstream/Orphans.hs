{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE DerivingVia #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Upstream.Orphans where

import Cardano.Binary (fromCBOR, toCBOR)
import Cardano.Api qualified as C

import Marconi.ChainIndex.Orphans ()

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
