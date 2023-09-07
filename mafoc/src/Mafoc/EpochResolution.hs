module Mafoc.EpochResolution where

import Cardano.Api qualified as C
import Mafoc.Exceptions qualified as E

data EpochResolution
  = New C.EpochNo
  | SameOrAbsent
  | AssumptionBroken E.CardanoAssumptionBroken

resolve :: Maybe C.EpochNo -> Maybe C.EpochNo -> EpochResolution
resolve maybePreviousEpoch maybeCurrentEpoch = case maybePreviousEpoch of
  Just previousEpochNo -> case maybeCurrentEpoch of
    Just currentEpochNo ->
      let epochDiff = currentEpochNo - previousEpochNo
      in case epochDiff of
          1 -> New currentEpochNo
          0 -> SameOrAbsent
          _ -> AssumptionBroken (E.Epoch_difference_other_than_0_or_1 previousEpochNo currentEpochNo)
    Nothing -> AssumptionBroken (E.Epoch_number_disappears previousEpochNo)
  Nothing -> case maybeCurrentEpoch of
    Just currentEpochNo -> New currentEpochNo
    Nothing -> SameOrAbsent
