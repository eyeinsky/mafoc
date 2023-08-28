{-# LANGUAGE DerivingStrategies #-}
module Mafoc.Exceptions
  ( module Mafoc.Exceptions
  , throw
  , throwIO
  ) where

import Control.Exception (Exception, throw, throwIO)
import Data.Text qualified as TS
import Codec.CBOR.Read (DeserialiseFailure)
import Cardano.Api qualified as C

data CardanoAssumptionBroken
  = Epoch_difference_other_than_0_or_1
    { epochNo :: C.EpochNo
    , followingEpochNo :: C.EpochNo
    }
  | Epoch_number_disappears { previouslyExistingEpochNo :: C.EpochNo }
  | UTxO_not_found { attemptedToSpend :: C.TxIn }
  | Block_number_ahead_of_tip
    { blockNumberInBlock :: C.BlockNo
    , blockNumberInChainTip :: C.ChainTip
    }
  deriving stock Show
  deriving anyclass Exception

data MafocIOException
  = Can't_deserialise_LedgerState_from_CBOR FilePath DeserialiseFailure
  | Can't_parse_chain_point_from_LedgerState_file_name FilePath String
  -- | Impossible
  | The_impossible_happened { whatItWas :: TS.Text }
  -- | Header-DB
  | SlotNo_not_found C.SlotNo
  | No_headerDb_specified
  | Can't_find_previous_ChainPoint_to_slot C.SlotNo
  -- | ChainPoints from multiple state sources
  | ChainPoints_don't_match [(String, Maybe C.ChainPoint)]
  deriving stock Show
  deriving anyclass Exception

-- | An escape-hatch exception: an exception where we don't bother
-- with structure, but just explain in text. Perhaps we'll find a
-- category later.
data TextException = TextException TS.Text
  deriving stock Show
  deriving anyclass Exception
