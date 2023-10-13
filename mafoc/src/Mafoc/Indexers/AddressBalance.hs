{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Mafoc.Indexers.AddressBalance where

import System.IO qualified as IO
import Data.Map.Strict qualified as Map
import Data.Map.Merge.Strict qualified as Map
import Data.List.NonEmpty qualified as NE
import Data.Aeson qualified as A
import Data.Aeson ((.=))
import Data.Aeson.Key qualified as A
import Data.ByteString.Lazy.Char8 qualified as BL8

import Cardano.Api qualified as C
import Mafoc.CLI qualified as O
import Mafoc.Core (
  Indexer (Event, Runtime, State, checkpoint, description, parseCli, persistMany, toEvents),
  LocalChainsyncConfig_,
  initialize,
  initializeLocalChainsync_,
  SlotNoBhh,
  SlotNoBhhString(SlotNoBhhString),
  AssetIdString(AssetIdString),
  CurrentEra
 )
import Mafoc.Utxo qualified as Utxo
import Mafoc.Upstream (txoAddressAny, toAddressAny)
import Mafoc.Exceptions qualified as E

data AddressBalance = AddressBalance
  { chainsync :: LocalChainsyncConfig_
  , addresses :: NE.NonEmpty (C.Address C.ShelleyAddr)
  , ignoreMissingUtxos :: Bool
  }
  deriving (Show)

instance Indexer AddressBalance where
  description = "Index balance changes for a set of addresses"

  parseCli =
    AddressBalance
      <$> O.commonLocalChainsyncConfig
      <*> O.some_ O.commonAddress
      <*> O.commonIgnoreMissingUtxos

  data Runtime AddressBalance = Runtime
    { match :: C.AddressAny -> Maybe C.AddressAny
    , onUtxo :: Utxo.OnUtxo (C.TxOut C.CtxTx CurrentEra) (C.TxIn, C.TxOut C.CtxTx CurrentEra)
    }

  data Event AddressBalance = Event
    -- when
    { chainPoint :: SlotNoBhh
    , blockNo :: C.BlockNo
    , txId :: C.TxId
    -- what
    , balance :: Map.Map C.AddressAny (Map.Map C.AssetId (C.Quantity, C.Quantity))
    , changes :: Map.Map C.AddressAny
                   ( [C.Value] -- spends
                   , [C.Value] -- deposits
                   )
    }
    deriving (Show)

  data State AddressBalance = State Utxo.UtxoMap

  toEvents
    Runtime{match, onUtxo = Utxo.OnUtxo{found, missing, toResult}}
    (State utxoStateStart)
    blockInMode@(C.BlockInMode (C.Block _bh txs :: C.Block era) _eim) = (State utxoStateEnd, reverse events)

    where
      (utxoStateEnd, events) = foldl stepTx (utxoStateStart, []) txs

      stepTx :: (Utxo.UtxoMap, [Event AddressBalance]) -> C.Tx era -> (Utxo.UtxoMap, [Event AddressBalance])
      stepTx (utxosBefore, events') tx = ( utxosAfter, maybe events' (:events') maybeEvent)
        where
          txId = #calculateTxId tx :: C.TxId
          slotNoBhh = #slotNoBhh blockInMode
          (spentTxIns, newTxos) = Utxo.txoEvent tx

          (maybeSpentTxos, remainingUtxos) = Utxo.spendTxos ([], utxosBefore) spentTxIns $
            \txIn maybeTxo -> case maybeTxo of
              Just txo ->  found txIn txId slotNoBhh txo
              Nothing -> missing txIn txId slotNoBhh
          spentTxos = toResult maybeSpentTxos

          createdTxos' = Utxo.addTxId txId $ flip mapMaybe newTxos $ \(txIx, txo) -> case match $ txoAddressAny txo of
            Just address -> Just (txIx, (address, txo))
            Nothing -> Nothing

          createdTxos :: Utxo.UtxoList
          createdTxos = Utxo.unsafeCastEra $ map (\(txIn, (_address, txo)) -> (txIn, txo)) createdTxos'

          utxosAfter = remainingUtxos <> Map.fromList createdTxos

          maybeEvent = if null spentTxos && null createdTxos
            then Nothing -- No event when no txos for requested addresses were spent or created
            else let
              toMap :: forall era' . [(C.AddressAny, C.TxOut C.CtxTx era')] -> Map.Map C.AddressAny [C.Value]
              toMap txos = Map.fromListWith (<>) $ map (\(a, C.TxOut _ value _ _) -> (a, [C.txOutValueToValue value])) txos :: Map.Map C.AddressAny [C.Value]

              deposits = fmap (\xs -> ([], xs)) $ toMap $ map snd createdTxos'
              spends = fmap (\xs -> (xs, [])) $ toMap $ map (\(_txIn, txo) -> (txoAddressAny txo, txo)) spentTxos

              toAddressValueMap = Map.fromListWith (<>) . map txoValue . Map.elems

              balance = Map.merge
                (Map.mapMissing $ \_k v -> mergeBeforeAfter v mempty)
                (Map.mapMissing $ \_k v -> mergeBeforeAfter mempty v)
                (Map.zipWithMatched $ \_k b a -> mergeBeforeAfter b a)
                (toAddressValueMap utxosBefore)
                (toAddressValueMap utxosAfter)

              in Just $ Event
                   (#slotNoBhh blockInMode)
                   (#blockNo blockInMode)
                   txId
                   balance
                   (Map.unionWith (<>) deposits spends)

  initialize AddressBalance{chainsync, addresses, ignoreMissingUtxos} trace = do
    lcr <- initializeLocalChainsync_ chainsync trace
    IO.hSetBuffering IO.stdout IO.LineBuffering
    let runtime = Runtime
          { match = Utxo.addressListFilter addresses
          , onUtxo = if ignoreMissingUtxos
            then Utxo.OnUtxo { found =   \txIn _ _ txo -> Just (txIn, txo)
                             , missing = \_ _ _ -> Nothing
                             , toResult = catMaybes
                             }
            else Utxo.OnUtxo { found =   \txIn _ _             txo -> (txIn, txo)
                             , missing = \txIn _ (slotNo, bhh)     -> E.throw $ E.UTxO_not_found txIn slotNo bhh
                             , toResult = id
                             }
          }
    return (State mempty, lcr, runtime)

  persistMany _runtime events = mapM_ (BL8.putStrLn . A.encode) events

  checkpoint _runtime _state _slotNoBhh = return ()

instance A.ToJSON (Event AddressBalance) where
  toJSON Event{..} = A.object
    [ "blockNo" .= blockNo
    , "chainPoint" .= SlotNoBhhString chainPoint
    , "txId" .= txId
    , "balance" .= addressMapToObject (Map.mapKeys AssetIdString <$> balance)
    , "changes" .= addressMapToObject (fmap spendsDeposits changes)
    ]
    where
      spendsDeposits :: ([C.Value], [C.Value]) -> A.Value
      spendsDeposits (spends, deposits) = A.object $ catMaybes
        [ if null spends then Nothing else Just $ "spends" .= spends
        , if null deposits then Nothing else Just $ "deposits" .= deposits
        ]

      addressMapToObject :: A.ToJSON v => Map.Map C.AddressAny v -> A.Value
      addressMapToObject = A.object . map (\(k, v) -> A.fromText (C.serialiseAddress k) .= A.toJSON v) . Map.toList

txoValue :: C.TxOut ctx era -> (C.AddressAny, C.Value)
txoValue (C.TxOut address value _TxOutDatum _ReferenceScript) = (toAddressAny address, C.txOutValueToValue value)


-- | Merge balance before and after @Value@s by setting non-existent @AssetId@ to zero.
mergeBeforeAfter :: C.Value -> C.Value -> Map.Map C.AssetId (C.Quantity, C.Quantity)
mergeBeforeAfter before after = Map.merge
  (Map.mapMissing $ \_k v -> (v, mempty))
  (Map.mapMissing $ \_k v -> (mempty, v))
  (Map.zipWithMatched $ \_k b a -> (b, a))
  before' after'
  where
    before' = Map.fromList $ C.valueToList before
    after' = Map.fromList $ C.valueToList after
