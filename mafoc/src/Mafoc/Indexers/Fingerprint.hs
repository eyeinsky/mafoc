{-# LANGUAGE RecordWildCards #-}
module Mafoc.Indexers.Fingerprint where

import Data.Aeson qualified as A
import Data.ByteString.Lazy.Char8 qualified as BL8
import Control.Monad (guard)

import Cardano.Api qualified as C

import Mafoc.Core
  ( Indexer, Event, Runtime, State, TxIndexInBlock, checkpoint, persistMany, toEvents, initialize, parseCli, description
  , stateless
  , LocalChainsyncConfig_, initializeLocalChainsync_)
import Mafoc.MultiAsset (AssetFingerprint, assetFingerprint, pass, restrict, getEvents, getEvents')
import Mafoc.CLI qualified as O

data Fingerprint = Fingerprint
  { chainsync :: LocalChainsyncConfig_
  , maybeAssetFilter :: Maybe (Either (C.PolicyId, Maybe C.AssetName) AssetFingerprint)
  } deriving Show

instance Indexer Fingerprint where

  description = "Index multi-asset fingerprints"

  parseCli = Fingerprint
    <$> O.commonLocalChainsyncConfig
    <*> (Just <$> assetFilter <|> pure Nothing)
    where
      assetFilter = Left <$> O.commonAssetId
                <|> Right <$> O.commonAssetFingerprint

  data Runtime Fingerprint = Runtime
    { toEvents_ :: C.BlockInMode C.CardanoMode -> [Event Fingerprint] }

  newtype State Fingerprint = State ()

  data Event Fingerprint = Event
    { fingerprint :: AssetFingerprint
    , policyId :: C.PolicyId
    , assetName :: C.AssetName
    , slotNo :: C.SlotNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    }

  initialize cli@Fingerprint{chainsync, maybeAssetFilter} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace $ show cli

    let assetFilter = case maybeAssetFilter of
          Nothing -> \blockInMode -> map (mk blockInMode) $ getEvents pass pass blockInMode
          Just e -> case e of
            Left assetId -> mkToEvents assetId
            Right fingerprint -> \blockInMode -> let
              guard' policyId assetName = guard $ assetFingerprint policyId assetName == fingerprint
              in map (mk blockInMode) $ getEvents' guard' blockInMode

    return (stateless, chainsyncRuntime, Runtime assetFilter)

  toEvents Runtime{toEvents_} _state blockInMode = (stateless, toEvents_ blockInMode)
  persistMany _runtime events = mapM_ (BL8.putStrLn . A.encode) events
  checkpoint _ _ _ = pure ()

instance A.ToJSON (Event Fingerprint) where
  toJSON Event{..} = A.object
    [ "fingerprint" A..= C.serialiseToBech32 fingerprint
    , "policyId" A..= policyId
    , "assetName" A..= assetName
    , "slotNo" A..= slotNo
    , "blockHeaderHash" A..= blockHeaderHash
    ]

mkToEvents :: (C.PolicyId, Maybe C.AssetName) -> C.BlockInMode C.CardanoMode -> [Event Fingerprint]
mkToEvents = \case
  (policyId, Just assetName) -> \blockInMode ->
    map (mk blockInMode) $ getEvents (restrict policyId) (restrict assetName) blockInMode
  (policyId, Nothing) -> \blockInMode ->
    map (mk blockInMode) $ getEvents (restrict policyId) pass blockInMode

mk
  :: C.BlockInMode C.CardanoMode
  -> (C.TxId, TxIndexInBlock, C.PolicyId, C.AssetName, C.Quantity, Maybe (C.ScriptData, C.Hash C.ScriptData))
  -> Event Fingerprint
mk blockInMode (_txId, _bx, policyId, assetName, _quantity, _maybeRedeemer) =
  Event{..}
  where
    slotNo = #slotNo blockInMode
    blockHeaderHash = #blockHeaderHash blockInMode
    fingerprint = assetFingerprint policyId assetName
