{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module RollbackRingBuffer where

import Control.Exception qualified as IO
import Control.Monad.IO.Class (liftIO)
import Data.Coerce (coerce)
import Data.List qualified as L
import Data.String (fromString)
import Data.Word (Word64)
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS


import Hedgehog ((===))
import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)

import Mafoc.RollbackRingBuffer (rollbackRingBuffer)
import Mafoc.Upstream (SlotNoBhh, minusNaturalMaybe)
import Spec.Helpers (classifier, footnotes)

tests :: TestTree
tests = testGroup "RollbackRingBuffer"
  [ testPropertyNamed
    "Rollback ring buffer buffers, overflows and handles rollbacks as expected"
    "prop_rollbackRingBuffer" prop_rollbackRingBuffer
  ]

-- | Generates random number of events and a random security
-- param. Event type is SlotNo so it's easy to derive a ChainPoint and
-- ChainTip from it.
prop_rollbackRingBuffer :: H.Property
prop_rollbackRingBuffer = H.property $ do
  (events, securityParam, numberOfEvents) <- H.forAll genEvents

  let hasRollForwards = any (\case CS.RollForward{} -> True; _ -> False) events
      hasRollbacks = any (\case CS.RollBackward{} -> True; _ -> False) events

  let stream = mapM_ S.yield events
  overflow <- liftIO $ IO.try $ S.toList_ $ rollbackRingBuffer securityParam tipDiff eventSlotNoBhh stream
  let expected = snd <$> toExpected securityParam events :: Either CS.RollbackException [C.SlotNo]

  classifier "A"
    [ ("Test cases with only \"roll forwards\"", hasRollForwards && not hasRollbacks)
    , ("Test cases with only rollbacks", not hasRollForwards && hasRollbacks)
    , ("Test cases with both", hasRollForwards && hasRollbacks)
    , ("Empty event set", not hasRollForwards && not hasRollbacks)
    ]

  classifier "B"
    [ ("numberOfEvents > securityParam", numberOfEvents > securityParam)
    , ("numberOfEvents <= securityParam", numberOfEvents <= securityParam)
    ]

  let bufferOverflowsP = \case Right (_ : _) -> True; _ -> False
      bufferOverflows = bufferOverflowsP overflow
  classifier "C"
    [ ("Buffer overflowed", bufferOverflows)
    , ("Buffer didn't overflow", not bufferOverflows)
    ]

  footnotes
    [ ("securityParam", show securityParam)
    , ("numberOfEvents", show numberOfEvents)
    , ("events", showEvents events)
    , ("sucess", show $ expected == overflow)
    ]
  expected === overflow

-- | Generate a list of chainsync events, both "roll forwards" and
-- rollbacks. Our event type is C.SlotNo.
genEvents :: H.Gen ([CS.ChainSyncEvent C.SlotNo], Natural, Natural)
genEvents = do
  let n = 5
  securityParam :: Natural <- Gen.integral $ Range.linear 0 $ 2 * n
  numberOfEvents :: Natural <- Gen.integral $ Range.linear 0 $ 3 * n
  let loop _ acc 0 = return acc
      loop prevEvent acc countdown = do
        step <- Gen.frequency
          [ (1, Gen.integral $ Range.linear (-5) 0) -- roll backward
          , (8, Gen.integral $ Range.linear 1 5)    -- roll forward
          ]
        tipStep <- Gen.integral $ Range.linear 0 5  -- tip distance from current event
        let event = prevEvent + step
            tip' = C.ChainTip (fromInteger $ event + tipStep) dummyBhh 0
            chainSyncEvent = if prevEvent < event
              then CS.RollForward (fromInteger event) tip'
              else CS.RollBackward (C.ChainPoint (fromInteger event) dummyBhh) tip'
        loop event (chainSyncEvent : acc) (countdown - 1)

  let start' = (maxBound - minBound) `div` 4 :: Word64 -- we start from the middle Word64, so that rollbacks wont wrap
      start = fromIntegral start' :: Integer
  events <- reverse <$> loop start [] numberOfEvents
  return (events, securityParam, numberOfEvents)

-- | List-based test oracle for the ring buffer.
toExpected :: Natural -> [CS.ChainSyncEvent C.SlotNo] -> Either CS.RollbackException ([C.SlotNo], [C.SlotNo])
toExpected securityParam list = case foldl step (Right ([], [])) list of
  Left e                   -> Left e
  Right (buffer, overflow) -> Right (reverse buffer, reverse overflow)
  where
  step :: Either CS.RollbackException ([C.SlotNo], [C.SlotNo])
    -> CS.ChainSyncEvent C.SlotNo
    -> Either CS.RollbackException ([C.SlotNo], [C.SlotNo])
  step e@(Left{}) _ = e
  step (Right (buffer, overflow)) csEvent = case csEvent of
    CS.RollForward event ct
      | tipDiff event ct > securityParam -> Right ([], (event : buffer) <> overflow)
      | length buffer == fromIntegral securityParam -> case (buffer, securityParam) of
          ([], 0) -> Right ([], event : overflow)
          _       -> flush ct (event : init buffer, last buffer : overflow)
      | length buffer < fromIntegral securityParam -> flush ct (event : buffer, overflow)
      | otherwise -> error "buffer larger than security param"
    CS.RollBackward cp ct -> case cp of
      C.ChainPointAtGenesis -> Right ([], overflow)
      C.ChainPoint slotNo bhh -> let
        (_, remaining) = L.break ((slotNo, bhh) ==) $ map eventSlotNoBhh buffer
        in case remaining of
             [] -> Left $ CS.RollbackLocationNotFound (C.ChainPoint slotNo dummyBhh) ct
             _  -> Right (map fst remaining, overflow)

  flush ct (buffer, overflow) = Right $ let
    (unstable, stable) = span (\event -> tipDiff event ct < securityParam) buffer
    in (unstable, stable <> overflow)

showEvents :: [CS.ChainSyncEvent C.SlotNo] -> String
showEvents = concat . map (\case
  CS.RollForward e ct   -> "\n - forward " <> show e <> " " <> showCtSlot ct
  CS.RollBackward cp ct -> "\n - backward " <> showCpSlot cp <> " " <> showCtSlot ct)
  where
    showCtSlot :: C.ChainTip -> String
    showCtSlot = \case
      C.ChainTip slotNo _ _ -> show slotNo
      C.ChainTipAtGenesis   -> "genesis"
    showCpSlot :: C.ChainPoint -> String
    showCpSlot = \case
      C.ChainPoint slotNo _ -> show slotNo
      C.ChainPointAtGenesis -> "genesis"

-- * SlotNo

eventSlotNoBhh :: C.SlotNo -> SlotNoBhh
eventSlotNoBhh event = (event, dummyBhh)

tipDiff :: C.SlotNo -> C.ChainTip -> Natural
tipDiff event (C.ChainTip slotNo _ _) = let
  eventSlotNo = fromIntegral (coerce event :: Word64) :: Natural
  tipSlot = fromIntegral (coerce slotNo :: Word64) :: Natural
  in case tipSlot `minusNaturalMaybe` eventSlotNo of
  Just n  -> n
  Nothing -> error $ "Subtraction with a larger natural! " <> show (tipSlot, eventSlotNo, eventSlotNo - tipSlot)
tipDiff (C.SlotNo 0) C.ChainTipAtGenesis = 0
tipDiff slotNo C.ChainTipAtGenesis = error $ "Tip can't be at genesis when event slot number is " <> show slotNo

dummyBhh :: C.Hash C.BlockHeader
dummyBhh = fromString "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
