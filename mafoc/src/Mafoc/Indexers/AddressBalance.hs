{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}

module Mafoc.Indexers.AddressBalance where

import System.IO qualified as IO
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, mapMaybe)
import Data.List.NonEmpty qualified as NE
import Data.Aeson qualified as A
import Data.Aeson ((.=))
import Data.Aeson.Key qualified as A
import Data.ByteString.Lazy.Char8 qualified as BL8

import Marconi.ChainIndex.Orphans () -- ToJSON C.AddressAny
import Cardano.Api qualified as C
import Mafoc.CLI qualified as O
import Mafoc.Core (
  Indexer (Event, Runtime, State, checkpoint, description, parseCli, persistMany, toEvents),
  LocalChainsyncConfig_,
  initialize,
  initializeLocalChainsync_,
 )
import Mafoc.Utxo qualified as Utxo
import Mafoc.Upstream (txoAddressAny, toAddressAny)

data AddressBalance = AddressBalance
  { chainsync :: LocalChainsyncConfig_
  , addresses :: NE.NonEmpty (C.Address C.ShelleyAddr)
  }
  deriving (Show)

instance Indexer AddressBalance where
  description = "Index address balance changes"

  parseCli =
    AddressBalance
      <$> O.commonLocalChainsyncConfig
      <*> O.some_ O.commonAddress

  data Runtime AddressBalance = Runtime { match :: C.AddressAny -> Maybe C.AddressAny }

  data Event AddressBalance = Event
    -- when
    { chainPoint :: C.ChainPoint
    , blockNo :: C.BlockNo
    , txId :: C.TxId
    -- what
    , balanceBefore :: Map.Map C.AddressAny C.Value
    , balanceAfter :: Map.Map C.AddressAny C.Value
    , changes :: Map.Map C.AddressAny
                   ( [C.Value] -- spends
                   , [C.Value] -- deposits
                   )
    }
    deriving (Show)

  data State AddressBalance = State Utxo.UtxoMap

  toEvents Runtime{match} (State utxoStateStart) blockInMode@(C.BlockInMode (C.Block _bh txs :: C.Block era) _eim) = (State utxoStateEnd, reverse events)
    where
      (utxoStateEnd, events) = foldl stepTx (utxoStateStart, []) txs

      stepTx :: (Utxo.UtxoMap, [Event AddressBalance]) -> C.Tx era -> (Utxo.UtxoMap, [Event AddressBalance])
      stepTx (utxosBefore, events') tx = ( utxosAfter, maybe events' (:events') maybeEvent)
        where
          txId = #calculateTxId tx :: C.TxId
          (spentTxIns, newTxos) = Utxo.txoEvent tx

          (maybeSpentTxos, remainingUtxos) = Utxo.spendTxos ([], utxosBefore) spentTxIns $
            \txIn maybeTxo -> case maybeTxo of
              Just txo -> Just (txIn, txo)
              Nothing -> Nothing
          spentTxos = catMaybes maybeSpentTxos

          createdTxos' = Utxo.addTxId txId $ flip mapMaybe newTxos $ \(txIx, txo) -> case match $ txoAddressAny txo of
            Just address -> Just (txIx, (address, txo))
            Nothing -> Nothing

          createdTxos :: Utxo.UtxoList
          createdTxos = Utxo.unsafeCastEra $ map (\(txIn, (_address, txo)) -> (txIn, txo)) createdTxos'

          utxosAfter = remainingUtxos <> Map.fromList createdTxos

          maybeEvent = if null spentTxos && null createdTxos
            then Nothing -- No event when no txos for requested addresses were spent or created
            else let
              toMap txos = Map.fromListWith (<>) $ map (\(a, C.TxOut _ value _ _) -> (a, [C.txOutValueToValue value])) txos :: Map.Map C.AddressAny [C.Value]

              deposits = fmap (\xs -> ([], xs)) $ toMap $ map snd createdTxos'
              spends = fmap (\xs -> (xs, [])) $ toMap $ map (\(_txIn, txo) -> (txoAddressAny txo, txo)) spentTxos

              in Just $ Event
                   (#chainPoint blockInMode)
                   (#blockNo blockInMode)
                   txId
                   (Map.fromListWith (<>) $ map txoValue $ Map.elems utxosBefore)
                   (Map.fromListWith (<>) $ map txoValue $ Map.elems utxosAfter)
                   (Map.unionWith (<>) deposits spends)

  initialize AddressBalance{chainsync, addresses} _trace = do
    lcr <- initializeLocalChainsync_ chainsync
    IO.hSetBuffering IO.stdout IO.LineBuffering
    return (State mempty, lcr, Runtime $ Utxo.addressListFilter addresses)

  persistMany _runtime events = mapM_ (BL8.putStrLn . A.encode) events

  checkpoint _runtime _state _slotNoBhh = return ()

instance A.ToJSON (Event AddressBalance) where
  toJSON Event{..} = A.object
    [ "blockNo" .= blockNo
    , "chainPoint" .= chainPoint
    , "txId" .= txId
    , "balanceBefore" .= addressMapToObject balanceBefore
    , "changes" .= addressMapToObject (fmap spendsDeposits changes)
    , "balanceAfter" .= addressMapToObject balanceAfter
    ]
    where
      spendsDeposits :: ([C.Value], [C.Value]) -> A.Value
      spendsDeposits (spends, deposits) = A.object $ catMaybes
        [ if null spends then Nothing else Just $ "spends" .= spends
        , if null deposits then Nothing else Just $ "deposits" .= deposits
        ]

      addressToKey :: C.AddressAny -> A.Key
      addressToKey = A.fromText . C.serialiseAddress

      addressMapToObject :: A.ToJSON v => Map.Map C.AddressAny v -> A.Value
      addressMapToObject = A.object . map (\(k, v) -> addressToKey k .= A.toJSON v) . Map.toList


txoValue :: C.TxOut ctx era -> (C.AddressAny, C.Value)
txoValue (C.TxOut address value _TxOutDatum _ReferenceScript) = (toAddressAny address, C.txOutValueToValue value)

-- instance A.ToJSONKey C.AddressAny where
--   toJSONKey = undefined -- default instance goes through ToJSON C.AddressAny
