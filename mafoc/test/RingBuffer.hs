{-# LANGUAGE OverloadedStrings #-}
module RingBuffer where

import Control.Monad (replicateM)

import Hedgehog ((===))
import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)

import Mafoc.RingBuffer (RingBuffer, flushN, new, peek, pushMany, unpushWhile)
import Spec.Helpers (footnotes)


tests :: TestTree
tests = testGroup "RingBuffer"
  [ testPropertyNamed
    "Ring buffer overflows as expected"
    "prop_ringBuffer" prop_ringBuffer
  ]

prop_ringBuffer :: H.Property
prop_ringBuffer = H.property $ do

  -- pushMany: manual
  do
    rb <- liftIO $ new 2
    (rb', overflow) <- liftIO $ pushMany [1, 2, 3, 4 :: Int] rb
    overflow === [1, 2]
    (_rb'', overflow') <- liftIO $ flushN 1 rb'
    overflow' === [3]

  -- pushMany: buffer overflow matches the oldest elements in the list
  do
    (numberOfItems, items, ringBufferSize, ringBuffer, overflow1) <- mkRingBufferItems
    let expectedOverflowSize1
          | numberOfItems < ringBufferSize = 0
          | otherwise = numberOfItems - ringBufferSize
        (expectedOverflow1, expectedRemaining1) = splitAt (fromIntegral expectedOverflowSize1) items
    footnotes
      [ ("expectedOverflowSize1", show expectedOverflowSize1)
      , ("expectedOverflow1", show expectedOverflow1)
      , ("overflow1", show overflow1)
      , ("expectedRemaining1", show expectedRemaining1)
      ]
    overflow1 === expectedOverflow1

    -- flushN: popping n elements from the buffer matches the n oldest elements remaining in the buffer
    howManyToPop :: Natural <- H.forAll $ Gen.integral $ Range.linear 0 $ 3 * ringBufferSize
    let expectedRemainingSize1 = numberOfItems - expectedOverflowSize1
        expectedOverflowSize2 = min howManyToPop expectedRemainingSize1
        expectedOverflow2 = take (fromIntegral expectedOverflowSize2) expectedRemaining1
    (_, overflow2) <- liftIO $ flushN howManyToPop ringBuffer
    footnotes
      [ ("howManyToPop", show howManyToPop)
      , ("expectedRemainingSize1", show expectedRemainingSize1)
      , ("expectedOverflowSize2", show expectedOverflowSize2)
      , ("expectedOverflow2", show expectedOverflow2)
      , ("overflow2", show overflow2)
      ]
    overflow2 === expectedOverflow2

  -- peek
  do
    (numberOfItems, items, ringBufferSize, ringBuffer, _overflow) <- mkRingBufferItems
    let expectedOverflowSize
          | numberOfItems < ringBufferSize = 0
          | otherwise = numberOfItems - ringBufferSize
    oldestItem <- liftIO $ peek ringBuffer
    let expectedOldestItem = listToMaybe $ drop (fromIntegral expectedOverflowSize) items
    case expectedOldestItem of
      Just _ -> H.classify "RingBuffer has at least one element" True
      _      -> H.classify "RingBuffer is empty" True
    oldestItem === expectedOldestItem

  -- unpushWhile: Unpush newest items from buffer while predicate matches
  do
    (_numberOfItems, items, ringBufferSize, ringBuffer, _overflow) <- mkRingBufferItems
    int :: Int <- H.forAll $ Gen.integral $ Range.linearBounded
    let predicate item = item > int
    (_, gotten) <- liftIO $ unpushWhile predicate ringBuffer
    let expected = take (fromIntegral ringBufferSize) $ takeWhile predicate $ reverse items
    gotten === expected

mkRingBufferItems :: H.PropertyT IO (Natural, [Int], Natural, RingBuffer Int, [Int])
mkRingBufferItems = do
  ringBufferSize :: Natural <- H.forAll $ Gen.integral $ Range.linear 1 10
  numberOfItems :: Natural <- H.forAll $ Gen.integral $ Range.linear 0 $ 2 * ringBufferSize
  items :: [Int] <- replicateM (fromIntegral numberOfItems) $ H.forAll $ Gen.integral Range.linearBounded
  (ringBuffer, overflow) <- liftIO $ pushMany items =<< new ringBufferSize
  return (numberOfItems, items, ringBufferSize, ringBuffer, overflow)
