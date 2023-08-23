{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Utxo where

import Data.Map qualified as Map
import Data.List qualified as L
import Data.List.NonEmpty qualified as NE

import Cardano.Api qualified as C
import Marconi.ChainIndex.Types qualified as Marconi
import Mafoc.Exceptions qualified as E

type UtxoMapEra era = Map.Map C.TxIn (C.TxOut C.CtxTx era)
type UtxoMap = UtxoMapEra Marconi.CurrentEra
type UtxoListEra era = [(C.TxIn, C.TxOut C.CtxTx era)]
type UtxoList = UtxoListEra Marconi.CurrentEra

-- | Spend a list of txIns from the utxo map in init.
spendTxos
  :: forall spend k v
   . (Ord k)
  => ([spend], Map.Map k v)
  -> [k]
  -> (k -> Maybe v -> spend)
  -> ([spend], Map.Map k v)
spendTxos init_ txIns mkSpent = foldl step init_ txIns
  where
    step :: ([spend], Map.Map k v) -> k -> ([spend], Map.Map k v)
    step (spentTxos, utxoMap') txIn = Map.alterF popUtxo txIn utxoMap'
      where
        popUtxo found = (mkSpent txIn found : spentTxos, Nothing)

addTxId :: C.TxId -> [(C.TxIx, a)] -> [(C.TxIn, a)]
addTxId txId list = map (\(ix, a) -> (C.TxIn txId ix, a)) list

unsafeCastEra
  :: (C.IsCardanoEra era)
  => [(a, C.TxOut C.CtxTx era)]
  -> [(a, C.TxOut C.CtxTx Marconi.CurrentEra)]
unsafeCastEra list = case traverse (traverse castToCurrentEra) list of
  Right a -> a
  Left _ -> E.throw $ E.The_impossible_happened "It should always be possible to cast `TxOut ctx era` to CurrentEra"

castToCurrentEra
  :: (C.IsCardanoEra fromEra, C.EraCast f)
  => f fromEra
  -> Either C.EraCastError (f Marconi.CurrentEra)
castToCurrentEra = C.eraCast Marconi.CurrentEra

addressListFilter :: NE.NonEmpty (C.Address C.ShelleyAddr) -> C.AddressAny -> Maybe C.AddressAny
addressListFilter list addr = if addr `elem` fmap C.AddressShelley list
  then Just addr
  else Nothing

-- * TxoEvent

type TxoEvent era = ([C.TxIn], [(C.TxIx, C.TxOut C.CtxTx era)])

-- | Pick spent and created txo from a transaction
txoEvent :: forall era . C.Tx era -> TxoEvent era
txoEvent (C.Tx (C.TxBody txbc@C.TxBodyContent{}) _) = (spent, created)
  where
    regularTxOuts = C.txOuts txbc

    (spent, created) = if isValid (C.txScriptValidity txbc)
      -- Transaction is valid, we take the "regular" txIns and txOuts
      then (map fst $ C.txIns txbc, addTxIx regularTxOuts)
      -- Transaction is invalid, we spend the input collateral and return rest to return collateral
      else let
        spentCollateral = case C.txInsCollateral txbc of
          C.TxInsCollateralNone     -> []
          C.TxInsCollateral _ txIns -> txIns
        returnCollateral = case C.txReturnCollateral txbc of
            C.TxReturnCollateralNone     -> []
            C.TxReturnCollateral _ txOut -> [(C.TxIx $ L.genericLength regularTxOuts, txOut)]
        in (spentCollateral, returnCollateral)

    isValid :: C.TxScriptValidity era -> Bool
    isValid = \case
      C.TxScriptValidityNone -> True
      C.TxScriptValidity _ scriptValidity -> case scriptValidity of
        C.ScriptValid   -> True
        C.ScriptInvalid -> False

    -- Add TxIx to TxOut's, these are 0-indexed
    addTxIx :: [a] -> [(C.TxIx, a)]
    addTxIx = zip [C.TxIx 0 ..]
