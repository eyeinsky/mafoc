{-# OPTIONS_GHC -Wno-missing-import-lists #-}
module Mafoc.Questions.IntraBlockSpends where

import Data.Set qualified as Set
import Prettyprinter (Pretty (pretty), (<+>))
import Data.Text.IO qualified as TS

import Cardano.Api qualified as C
import Mafoc.Core
import Mafoc.CLI qualified as O
import Mafoc.Logging (renderDefault)

data IntraBlockSpends = IntraBlockSpends
  { chainsyncConfig :: LocalChainsyncConfig_
  }
  deriving Show

instance Indexer IntraBlockSpends where
  parseCli = IntraBlockSpends
    <$> O.commonLocalChainsyncConfig

  data Runtime IntraBlockSpends = Runtime
  data Event IntraBlockSpends = Event
    { slotNoBhh :: SlotNoBhh
    , txCount :: Int
    , intraBlockSpends :: Int
    }
  newtype State IntraBlockSpends = Stateless ()

  description = "Report the number of intra-block spends: where transaction spends UTxOs from the same block it itself is in."

  initialize IntraBlockSpends{chainsyncConfig} trace = do
    traceInfo trace "Only blocks with intra-block spends are reported"
    lcr <- initializeLocalChainsync_ chainsyncConfig trace
    return (stateless, lcr, Runtime)

  toEvents _runtime _state blockInMode@(C.BlockInMode (C.Block _ txs) _) = (_state, events)
    where
      txSpentTxIds :: C.Tx era -> [C.TxId]
      txSpentTxIds (C.Tx (C.TxBody (C.TxBodyContent{C.txIns})) _) = map ((\case C.TxIn txId _ -> txId) . fst) txIns

      txIdsInBlock = Set.fromList $ map #calculateTxId txs :: Set.Set C.TxId

      count = sum $ map (\tx -> length $ filter id $ map (`Set.member` txIdsInBlock) $ txSpentTxIds tx) txs
      events = if count /= 0
        then [Event (#slotNoBhh blockInMode) (length txs) count]
        else []

  persistMany _runtime events = do
    forM_ events $ \(Event (slotNo, bhh) txCount count) -> let
      msg = pretty (C.ChainPoint slotNo bhh)
        <+> "intra-spends:" <+> pretty count
        <+> "(from total of" <+> pretty txCount <+> "transactions in block)"
      in TS.putStrLn $ renderDefault msg

  checkpoint _runtime _state slotNoBhh =
    TS.putStrLn $ renderDefault $ "Have indexed up until" <+> pretty slotNoBhh
