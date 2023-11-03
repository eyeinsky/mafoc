{-# LANGUAGE PatternSynonyms #-}
module Mafoc.Utxo where

import Data.Map qualified as Map
import Data.List qualified as L
import Data.List.NonEmpty qualified as NE

import Cardano.Api qualified as C

import Cardano.Api.Address qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Api.TxIn qualified as C
import Cardano.Api.Value qualified as C

import Cardano.Chain.Block qualified as Byron
import Cardano.Chain.Common qualified as Byron
import Cardano.Chain.Genesis qualified as Byron
import Cardano.Chain.Slotting qualified as Byron
import Cardano.Chain.UTxO qualified as Byron
import Ouroboros.Consensus.Byron.Ledger qualified as Byron
import Cardano.Ledger.Core qualified as Ledger
import Cardano.Ledger.Shelley.API qualified as Ledger

import Cardano.Crypto qualified as Crypto

import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O
import Ouroboros.Consensus.Shelley.Ledger qualified as O

import Mafoc.LedgerState (ExtLedgerState_)
import Mafoc.Exceptions qualified as E
import Mafoc.Upstream (SlotNoBhh, CurrentEra, pattern CurrentEra)

type UtxoMapEra era = Map.Map C.TxIn (C.TxOut C.CtxTx era)
type UtxoMap = UtxoMapEra CurrentEra
type UtxoListEra era = [(C.TxIn, C.TxOut C.CtxTx era)]
type UtxoList = UtxoListEra CurrentEra


-- | Utxo resolving strategy
data OnUtxo elem result = forall a . OnUtxo
  { missing  :: C.TxIn -> C.TxId -> SlotNoBhh -> a
  , found    :: C.TxIn -> C.TxId -> SlotNoBhh -> elem -> a
  , toResult :: [a] -> [result]
  }

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
        popUtxo found_ = (mkSpent txIn found_ : spentTxos, Nothing)

--

addTxId :: C.TxId -> [(C.TxIx, a)] -> [(C.TxIn, a)]
addTxId txId list = map (\(ix, a) -> (C.TxIn txId ix, a)) list

unsafeCastEra
  :: (C.IsCardanoEra era)
  => [(a, C.TxOut C.CtxTx era)]
  -> [(a, C.TxOut C.CtxTx CurrentEra)]
unsafeCastEra list = case traverse (traverse castToCurrentEra) list of
  Right a -> a
  Left _ -> E.throw castToCurrentEraFailedException

castToCurrentEra
  :: (C.IsCardanoEra fromEra, C.EraCast f)
  => f fromEra
  -> Either C.EraCastError (f CurrentEra)
castToCurrentEra = C.eraCast CurrentEra

unsafeCastToCurrentEra
  :: (C.IsCardanoEra fromEra, C.EraCast f)
  => f fromEra -> f CurrentEra
unsafeCastToCurrentEra e = either (E.throw castToCurrentEraFailedException) id $ castToCurrentEra e

castToCurrentEraFailedException :: E.MafocIOException
castToCurrentEraFailedException = E.The_impossible_happened "It should always be possible to cast `TxOut ctx era` to CurrentEra"

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

-- * Genesis

byronGenesisUtxoFromConfig :: C.GenesisConfig -> (C.Hash C.BlockHeader, [(C.TxIn, C.TxOut ctx CurrentEra)])
byronGenesisUtxoFromConfig (C.GenesisCardano _ byronConfig _ _ _) = ( hash, byronTxOuts )
  where
    genesisHash = Byron.configGenesisHash byronConfig :: Byron.GenesisHash
    hash = C.HeaderHash $ Crypto.abstractHashToShort (Byron.unGenesisHash genesisHash)

    byronTxOuts :: [(C.TxIn, C.TxOut ctx0 CurrentEra)]
    byronTxOuts = byronGenesisAddressBalances byronConfig
      & map byronGenesisBalanceToTxOut
      & map (\(txIn, txOut) -> (txIn, unsafeCastToCurrentEra txOut))

    byronGenesisAddressBalances :: Byron.Config -> [(Byron.Address, Byron.Lovelace)]
    byronGenesisAddressBalances config =
        avvmBalances <> nonAvvmBalances
      where
        networkMagic :: Byron.NetworkMagic
        networkMagic = Byron.makeNetworkMagic (Byron.configProtocolMagic config)
        f = Byron.makeRedeemAddress networkMagic . Crypto.fromCompactRedeemVerificationKey

        avvmBalances :: [(Byron.Address, Byron.Lovelace)]
        avvmBalances = map (\(a, b) -> (f a, b)) $ Map.toList (Byron.unGenesisAvvmBalances $ Byron.configAvvmDistr config)

        nonAvvmBalances :: [(Byron.Address, Byron.Lovelace)]
        nonAvvmBalances =
          Map.toList $ Byron.unGenesisNonAvvmBalances (Byron.configNonAvvmBalances config)

    byronGenesisBalanceToTxOut :: (Byron.Address, Byron.Lovelace) -> (C.TxIn, C.TxOut ctx C.ByronEra)
    byronGenesisBalanceToTxOut (address, value) = (txIn, txOut)
      where
        txIn = C.fromByronTxIn $ Byron.TxInUtxo (addressHash address) 0
        txOut = fromByronTxOut $ Byron.TxOut address value

        addressHash :: Byron.Address -> Crypto.Hash Byron.Tx
        addressHash =
          fromMaybe (E.throw $ E.The_impossible_happened "Hashing addresses from Byron genesis file shouldn't fail")
            . Crypto.abstractHashFromBytes . Crypto.abstractHashToBytes . Crypto.serializeCborHash

genesisUtxoFromLedgerState :: forall ctx . ExtLedgerState_ -> Map.Map C.TxIn (C.TxIn, C.TxOut ctx CurrentEra)
genesisUtxoFromLedgerState extLedgerState = case O.ledgerState extLedgerState of
  O.LedgerStateByron (st :: O.LedgerState Byron.ByronBlock) -> ledgerStateEventMapByron st
  O.LedgerStateShelley st -> ledgerStateEventMapShelley C.ShelleyBasedEraShelley st
  O.LedgerStateAllegra st -> ledgerStateEventMapShelley C.ShelleyBasedEraAllegra st
  O.LedgerStateMary st -> ledgerStateEventMapShelley C.ShelleyBasedEraMary st
  O.LedgerStateAlonzo st -> ledgerStateEventMapShelley C.ShelleyBasedEraAlonzo st
  O.LedgerStateBabbage st -> ledgerStateEventMapShelley C.ShelleyBasedEraBabbage st
  O.LedgerStateConway st -> ledgerStateEventMapShelley C.ShelleyBasedEraConway st
  where
    ledgerStateEventMapByron :: O.LedgerState Byron.ByronBlock -> Map.Map C.TxIn (C.TxIn, C.TxOut ctx CurrentEra)
    ledgerStateEventMapByron st = Map.fromList $ map byronTxOutToEvent $ Map.toList $ Byron.unUTxO $ Byron.cvsUtxo $ cvs
      where
        cvs = Byron.byronLedgerState st :: Byron.ChainValidationState
        _slotNo = coerce @Byron.SlotNumber @C.SlotNo $ Byron.cvsLastSlot cvs -- TODO: get slot no from shelly as well

        byronTxOutToEvent :: (Byron.CompactTxIn, Byron.CompactTxOut) -> (C.TxIn, (C.TxIn, C.TxOut ctx CurrentEra))
        byronTxOutToEvent (k, v) = (txIn, event)
          where
            txIn = C.fromByronTxIn $ Byron.fromCompactTxIn k :: C.TxIn
            txOut = fromByronTxOut $ Byron.fromCompactTxOut v
            event = (txIn, unsafeCastToCurrentEra txOut)

    ledgerStateEventMapShelley
      :: forall proto lEra era
       . ( O.StandardCrypto ~ Ledger.EraCrypto lEra
         , C.ShelleyLedgerEra era ~ lEra
         , C.IsCardanoEra era
         )
      => C.ShelleyBasedEra era
      -> O.LedgerState (O.ShelleyBlock proto lEra)
      -> Map.Map C.TxIn (C.TxIn, C.TxOut ctx CurrentEra)
    ledgerStateEventMapShelley era st = st
      & O.shelleyLedgerState
      & Ledger.nesEs
      & Ledger.esLState
      & Ledger.lsUTxOState
      & Ledger.utxosUtxo
      & coerce
      & Map.toList
      & map ledgerToCardanoApi
      & Map.fromList
      where
        ledgerToCardanoApi :: (Ledger.TxIn O.StandardCrypto, Ledger.TxOut lEra) -> (C.TxIn, (C.TxIn, C.TxOut ctx CurrentEra))
        ledgerToCardanoApi (k, v) = (txIn, event)
          where
            txIn = C.fromShelleyTxIn k
            txOut = C.fromShelleyTxOut era v
            event = (txIn, unsafeCastToCurrentEra txOut)

-- copy-paste from cardano-api/cardano-api/internal/Cardano/Api/TxBody.hs::731
fromByronTxOut :: Byron.TxOut -> C.TxOut ctx C.ByronEra
fromByronTxOut (Byron.TxOut addr value) =
  C.TxOut
    (C.AddressInEra C.ByronAddressInAnyEra (C.ByronAddress addr))
    (C.TxOutAdaOnly C.AdaOnlyInByronEra (C.fromByronLovelace value))
     C.TxOutDatumNone C.ReferenceScriptNone
