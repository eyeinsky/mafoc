{-# LANGUAGE RecordWildCards #-}
module Mafoc.Indexers.Deposit where

import System.IO qualified as IO
import Data.ByteString.Lazy.Char8 qualified as BL8
import Data.Aeson qualified as A
import Data.Aeson ((.=))
import Data.List.NonEmpty qualified as NE

import Cardano.Api qualified as C
import Mafoc.Core (Indexer(Event, State, Runtime, description, parseCli, initialize, toEvents, persistMany, checkpoint), LocalChainsyncConfig_, initializeLocalChainsync_, stateless)
import Mafoc.CLI qualified as O
import Mafoc.Utxo qualified as Utxo
import Mafoc.Upstream (toAddressAny)

data Deposit = Deposit
  { chainsync :: LocalChainsyncConfig_
  , addresses :: NE.NonEmpty (C.Address C.ShelleyAddr)
  }
  deriving Show

instance Indexer Deposit where

  data Event Deposit = Event
    { chainPoint :: C.ChainPoint
    , blockNo :: C.BlockNo
    , txId :: C.TxId
    , assets :: NE.NonEmpty (C.AddressAny, C.Value)
    }

  newtype State Deposit = Stateless ()

  data Runtime Deposit = Runtime { match :: C.AddressAny -> Maybe C.AddressAny }

  description = "Index deposits for a set of addresses"

  parseCli =
    Deposit
      <$> O.commonLocalChainsyncConfig
      <*> O.some_ O.commonAddress

  initialize cli@Deposit{chainsync, addresses} trace = do
    lcr <- initializeLocalChainsync_ chainsync trace $ show cli
    IO.hSetBuffering IO.stdout IO.LineBuffering
    return (stateless, lcr, Runtime $ Utxo.addressListFilter addresses)

  toEvents Runtime{match} state bim@(C.BlockInMode (C.Block _ txs) _) = (state, mapMaybe toEvent txs)
    where
      toEvent :: forall era . C.Tx era -> Maybe (Event Deposit)
      toEvent tx@(C.Tx (C.TxBody txbc) _) = Event (#chainPoint bim) (#blockNo bim) (#calculateTxId tx) <$> maybeList
        where
          maybeList :: Maybe (NE.NonEmpty (C.AddressAny, C.Value))
          maybeList = NE.nonEmpty $ mapMaybe maybePair $ C.txOuts txbc

          maybePair :: C.TxOut ctx era -> Maybe (C.AddressAny, C.Value)
          maybePair (C.TxOut address value _ _) = case match (toAddressAny address) of
            Just addressAny -> Just (addressAny, C.txOutValueToValue value)
            Nothing -> Nothing

  persistMany _runtime events = BL8.putStrLn $ A.encode events

  checkpoint _runtime _state _slotNoBhh = return ()

instance A.ToJSON (Event Deposit) where
  toJSON Event{..} = A.object
    [ "blockNo" .= blockNo
    , "chainPoint" .= chainPoint
    , "txId" .= txId
    , "assets" .= assets
    ]
