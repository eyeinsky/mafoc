{-# LANGUAGE DerivingVia #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Upstream.Orphans where

import Database.SQLite.Simple.FromField qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import Prettyprinter (Pretty (pretty), (<+>))
import Data.Aeson qualified as Aeson
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString.Lazy (toStrict)
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.Ok qualified as SQL
import Codec.Serialise (Serialise (decode, encode))

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Binary qualified as CBOR
import Cardano.Ledger.Shelley.API qualified as Ledger
import Codec.CBOR.Read qualified as CBOR


-- * C.Hash C.BlockHeader

deriving via C.UsingRawBytes (C.Hash C.BlockHeader) instance C.ToCBOR (C.Hash C.BlockHeader)
deriving via C.UsingRawBytes (C.Hash C.BlockHeader) instance C.FromCBOR (C.Hash C.BlockHeader)

instance Pretty (C.Hash C.BlockHeader) where
  pretty hash = "BlockHash" <+> pretty (C.serialiseToRawBytesHexText hash)

instance SQL.ToField (C.Hash C.BlockHeader) where
  toField f = SQL.toField $ C.serialiseToRawBytes f

instance SQL.FromField (C.Hash C.BlockHeader) where
  fromField = fromFieldViaRawBytes (C.proxyToAsType Proxy)

-- * C.TxIn

instance C.ToCBOR C.TxIn where toCBOR (C.TxIn txId txIx) = CBOR.toCBOR (txId, txIx)
instance C.FromCBOR C.TxIn where fromCBOR = uncurry C.TxIn <$> CBOR.fromCBOR

instance SQL.ToRow C.TxIn where
  toRow (C.TxIn txid txix) = SQL.toRow (txid, txix)

instance SQL.FromRow C.TxIn where
  fromRow = C.TxIn <$> SQL.field <*> SQL.field

-- ** C.TxId

deriving via C.UsingRawBytes C.TxId instance C.ToCBOR C.TxId
deriving via C.UsingRawBytes C.TxId instance C.FromCBOR C.TxId

instance SQL.FromField C.TxId where
  fromField = fromFieldViaRawBytes C.AsTxId
instance SQL.ToField C.TxId where
  toField = SQL.SQLBlob . C.serialiseToRawBytes

-- ** C.TxIx

deriving newtype instance C.ToCBOR C.TxIx
deriving newtype instance C.FromCBOR C.TxIx

instance SQL.FromField C.TxIx where
  fromField = fmap C.TxIx . SQL.fromField
instance SQL.ToField C.TxIx where
  toField (C.TxIx i) = SQL.SQLInteger $ fromIntegral i

-- * C.Value

instance C.ToCBOR C.Value where
  toCBOR = CBOR.toCBOR . C.valueToList
instance C.FromCBOR C.Value where
  fromCBOR = C.valueFromList <$> CBOR.fromCBOR

instance SQL.ToField C.Value where
  toField = SQL.SQLBlob . toStrict . Aeson.encode

instance SQL.FromField C.Value where
  fromField f =
    SQL.fromField f
      >>= either
        (const $ SQL.returnError SQL.ConversionFailed f "Cannot deserialise value.")
        pure
        . Aeson.eitherDecode

-- ** C.AssetId

instance C.ToCBOR C.AssetId where
  toCBOR =
    CBOR.toCBOR . \case
      C.AdaAssetId -> Nothing
      C.AssetId policyId assetName -> Just (policyId, assetName)
instance C.FromCBOR C.AssetId where
  fromCBOR = maybeToAssetId <$> CBOR.fromCBOR
    where
      maybeToAssetId = \case
        Nothing -> C.AdaAssetId
        Just (policyId, assetName) -> C.AssetId policyId assetName

-- ** C.Quantity

deriving newtype instance C.ToCBOR C.Quantity
deriving newtype instance C.FromCBOR C.Quantity
deriving newtype instance SQL.ToField C.Quantity
deriving newtype instance SQL.FromField C.Quantity

deriving newtype instance SQL.ToField C.Lovelace
deriving newtype instance SQL.FromField C.Lovelace

-- ** C.AssetName

deriving newtype instance C.ToCBOR C.AssetName
deriving newtype instance C.FromCBOR C.AssetName
deriving newtype instance SQL.ToField C.AssetName
deriving newtype instance SQL.FromField C.AssetName

-- ** C.PolicyId

deriving via C.UsingRawBytes C.PolicyId instance C.ToCBOR C.PolicyId
deriving via C.UsingRawBytes C.PolicyId instance C.FromCBOR C.PolicyId
instance SQL.ToField C.PolicyId where -- C.PolicyId is a newtype over C.ScriptHash but no ToField available for it.
  toField = SQL.toField . C.serialiseToRawBytes
instance SQL.FromField C.PolicyId where
  fromField = fromFieldViaRawBytes C.AsPolicyId

-- * C.AddressAny

deriving via C.UsingRawBytes C.AddressAny instance C.ToCBOR C.AddressAny
deriving via C.UsingRawBytes C.AddressAny instance C.FromCBOR C.AddressAny

-- * C.SlotNo

deriving newtype instance Real C.SlotNo
deriving newtype instance Integral C.SlotNo

instance Pretty C.SlotNo where
  pretty (C.SlotNo n) = "Slot" <+> pretty n

deriving newtype instance SQL.ToField C.SlotNo
deriving newtype instance SQL.FromField C.SlotNo

-- * C.ChainPoint

instance Pretty C.ChainTip where
  pretty C.ChainTipAtGenesis = "ChainTipAtGenesis"
  pretty (C.ChainTip sn ha bn) = "ChainTip(" <> pretty sn <> "," <+> pretty ha <> "," <+> pretty bn <> ")"

instance Pretty C.ChainPoint where
  pretty C.ChainPointAtGenesis = "ChainPointAtGenesis"
  pretty (C.ChainPoint sn ha) = "ChainPoint(" <> pretty sn <> "," <+> pretty ha <> ")"

instance SQL.FromRow C.ChainPoint where
  fromRow = C.ChainPoint <$> SQL.field <*> SQL.field

-- * C.BlockNo

instance Pretty C.BlockNo where
  pretty (C.BlockNo bn) = "BlockNo" <+> pretty bn

deriving newtype instance SQL.ToField C.BlockNo
deriving newtype instance SQL.FromField C.BlockNo

-- * C.AddressAny

instance SQL.FromField C.AddressAny where
  fromField = fromFieldViaRawBytes C.AsAddressAny

instance SQL.ToField C.AddressAny where
  toField = SQL.SQLBlob . C.serialiseToRawBytes

instance FromJSON C.AddressAny where
  parseJSON (Aeson.String v) =
    maybe
      mempty
      pure
      $ C.deserialiseAddress C.AsAddressAny v
  parseJSON _ = mempty

instance ToJSON C.AddressAny where
  toJSON = Aeson.String . C.serialiseAddress

-- * C.Hash C.ScriptData

instance SQL.FromField (C.Hash C.ScriptData) where
  fromField = fromFieldViaRawBytes (C.AsHash C.AsScriptData)

instance SQL.ToField (C.Hash C.ScriptData) where
  toField = SQL.SQLBlob . C.serialiseToRawBytes

-- * C.ScriptData

deriving via C.UsingRawBytes (C.Hash C.ScriptData) instance C.ToCBOR (C.Hash C.ScriptData)
deriving via C.UsingRawBytes (C.Hash C.ScriptData) instance C.FromCBOR (C.Hash C.ScriptData)

instance Serialise C.ScriptData where
  encode = CBOR.toCBOR
  decode = CBOR.fromCBOR

instance SQL.FromField C.ScriptData where
  fromField = fromFieldViaCbor' C.AsScriptData

instance SQL.ToField C.ScriptData where
  toField = SQL.SQLBlob . C.serialiseToCBOR

instance FromJSON C.ScriptData where
  parseJSON =
    either (fail . show) (pure . C.getScriptData)
      . C.scriptDataFromJson C.ScriptDataJsonDetailedSchema

mapLeft :: (a -> b) -> Either a c -> Either b c
mapLeft f (Left v) = Left $ f v
mapLeft _ (Right v) = Right v

instance ToJSON C.ScriptData where
  toJSON = C.scriptDataToJson C.ScriptDataJsonDetailedSchema . C.unsafeHashableScriptData

-- * C.ScriptInAnyLang

instance SQL.ToField C.ScriptInAnyLang where
  toField = SQL.SQLBlob . toStrict . Aeson.encode

instance SQL.FromField C.ScriptInAnyLang where
  fromField f =
    SQL.fromField f
      >>= either
        (const $ SQL.returnError SQL.ConversionFailed f "Cannot deserialise value.")
        pure
        . Aeson.eitherDecode

-- * C.ScriptHash

instance SQL.ToField C.ScriptHash where
  toField = SQL.SQLBlob . C.serialiseToRawBytesHex

instance SQL.FromField C.ScriptHash where
  fromField f =
    SQL.fromField f
      >>= either
        (const $ SQL.returnError SQL.ConversionFailed f "Cannot deserialise scriptDataHash.")
        pure
        . C.deserialiseFromRawBytesHex C.AsScriptHash

-- * Ledger.Nonce

instance SQL.ToField Ledger.Nonce where
  toField = SQL.SQLBlob . CBOR.toStrictByteString . CBOR.toCBOR

instance SQL.FromField Ledger.Nonce where
  fromField = fromFieldViaCbor "Ledger.Nonce"

-- * ToField/FromField

deriving newtype instance SQL.ToField C.EpochNo
deriving newtype instance SQL.FromField C.EpochNo

instance SQL.FromField C.PoolId where
  fromField = fromFieldViaRawBytes (C.AsHash C.AsStakePoolKey)

instance SQL.ToField C.PoolId where
  toField = SQL.toField . C.serialiseToRawBytes

-- * Helpers

-- | Helper to deserialize via SerialiseAsRawBytes instance
fromFieldViaRawBytes :: (C.SerialiseAsRawBytes a) => C.AsType a -> SQL.Field -> SQL.Ok a
fromFieldViaRawBytes as f = either (const err) pure . C.deserialiseFromRawBytes as =<< SQL.fromField f
  where
    err = SQL.returnError SQL.ConversionFailed f "can't deserialise via SerialiseAsRawBytes"

-- | Helper to deserialize via SerialiseAsRawBytes instance
fromFieldViaCbor :: CBOR.FromCBOR a => String -> SQL.Field -> SQL.Ok a
fromFieldViaCbor str f = either (const err) (pure . snd) . CBOR.deserialiseFromBytes CBOR.fromCBOR =<< SQL.fromField f
  where
    err = SQL.returnError SQL.ConversionFailed f $ "can't deserialise " <> str <> " using FromCBOR"

fromFieldViaCbor' :: C.SerialiseAsCBOR a => C.AsType a -> SQL.Field -> SQL.Ok a
fromFieldViaCbor' as f = either (const err) pure . C.deserialiseFromCBOR as =<< SQL.fromField f
  where
    err = SQL.returnError SQL.ConversionFailed f $ "can't deserialise via CBOR"
