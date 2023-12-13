{-# LANGUAGE DerivingVia #-}
module Mafoc.Exceptions
  ( module Mafoc.Exceptions
  , throw
  , throwIO
  ) where

import Control.Exception (Exception, throw, throwIO)
import Data.Text qualified as TS
import Codec.CBOR.Read (DeserialiseFailure)
import Prettyprinter (Pretty(pretty))
import Cardano.Api qualified as C

data CardanoAssumptionBroken
  = Epoch_difference_other_than_0_or_1
    { epochNo :: C.EpochNo
    , followingEpochNo :: C.EpochNo
    }
  | Epoch_number_disappears { previouslyExistingEpochNo :: C.EpochNo }
  | UTxO_not_found { attemptedToSpend :: C.TxIn
                   , slotNo :: C.SlotNo
                   , bhh :: C.Hash C.BlockHeader
                   }
  | Block_number_ahead_of_tip
    { blockNumberInBlock :: C.BlockNo
    , blockNumberInChainTip :: C.ChainTip
    }
  deriving stock Show
  deriving anyclass Exception
  deriving Pretty via Showing CardanoAssumptionBroken

data MafocIOException
  = Can't_deserialise_LedgerState_from_CBOR FilePath DeserialiseFailure
  | Can't_parse_chain_point_from_LedgerState_file_name FilePath String
  -- | Impossible
  | The_impossible_happened { whatItWas :: TS.Text }
  -- | Header-DB
  | SlotNo_not_found C.SlotNo
  | No_headerDb_specified { whyIsItNeeded :: TS.Text }
  | Can't_find_previous_ChainPoint_to_slot C.SlotNo
  -- | ChainPoints from multiple state sources
  | ChainPoints_don't_match [(String, Maybe C.ChainPoint)]
  -- | GHC.Stats.getRTSStats: GC stats not enabled. Use `+RTS -T -RTS' to enable them.
  | RTS_GC_stats_not_enabled
  -- | For stateful indexers we can only start from chain points for
  -- which we have a state. Thus, if a user requests to start indexing
  -- from a later chain point then we throw this exception. (If the
  -- user requests to start from an /earlier/ chain point then we
  -- silently skip forward and start indexing from the state file's
  -- chain point.)
  | No_state_for_requested_starting_point
    { requested :: C.ChainPoint -- ^ Requested ChainPoint to start indexing from
    , fromState ::  C.ChainPoint -- ^ ChainPoint for latest state file we have available
    }
  -- | HTTP API was requested from an indexer which doesn't have an API defined.
  | Indexer_has_no_HTTP_API
  -- | Indexers that require state (utxo, epochstakepoolsizes,
  -- epochnonce, addressbalance) to extract their events can't index
  -- the chain in parallel as state is folded sequentially from
  -- genesis. Thus, starting from the middle is not possible
  -- because we don't have a state for that point.
  | Stateful_indexer_can't_be_run_with_parallelilsm

  -- | Every chain point later than slot zero must have a previous chain point.
  | Previous_ChainPoint_for_slot_not_found C.SlotNo

  -- | An escape-hatch exception: an exception where we don't bother
  -- with structure, but just explain in text. Perhaps we'll find a
  -- category later.
  | TextException TS.Text
  deriving stock Show
  deriving anyclass Exception
  deriving Pretty via Showing MafocIOException

throwShow :: Show a => a -> IO b
throwShow = throwIO . TextException . TS.pack . show

newtype Showing a = Showing a
instance Show a => Pretty (Showing a) where pretty (Showing a) = pretty (show a)
