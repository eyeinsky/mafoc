{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module RollbackRingBuffer where

import Control.Exception qualified as IO
import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.String (fromString)
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Hedgehog qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)

import Mafoc.RollbackRingBuffer (Event (RollBackward, RollForward), RollbackException (RollbackLocationNotFound),
                                 rollbackRingBuffer)

tests :: TestTree
tests = testGroup "RollbackRingBuffer"
  [ testPropertyNamed "prop_rollback_buffer_has_size" "bufferOverflowsExpectedly" bufferOverflowsExpectedly
  , testPropertyNamed "prop_rollback_ringbuffer_comprehensive_test" "rollbackRingBufferComprehensiveTest" rollbackRingBufferComprehensiveTest
  ]

-- Helper type for a `rollbackRingBuffer` where point and event are both the same (`Int`)
type IntStream = S.Stream (S.Of (Event Int Int)) IO ()
type IntBuffer = S.Stream (S.Of (Event Int Int)) IO () -> S.Stream (S.Of Int) IO ()

bufferOverflowsExpectedly :: H.Property
bufferOverflowsExpectedly = H.property $ do
  bufferSize   :: Natural <- H.forAll $ Gen.integral (Range.linear 0 20)
  overflowSize :: Int     <- H.forAll $ Gen.integral (Range.linear 0 20)
  let intRollbackRingBuffer = rollbackRingBuffer bufferSize :: IntBuffer
      bufferSizeInt = fromEnum bufferSize
      overflow :: [Int]
      overflow = [0 .. overflowSize - 1] -- These will eventually be emitted from the FIFO ringbuffer.
      source :: IntStream
      source = do
        forM_ overflow $ (S.yield . mkEvent)
        forM_ [overflowSize .. overflowSize + bufferSizeInt - 1] (S.yield . mkEvent)
  result <- liftIO $ S.toList_ $ intRollbackRingBuffer source -- Consume `source` by rollback ringbuffer.
  overflow H.=== result


-- | Generates rollbackRingBuffers of size @bufferSize@, then inserts
-- @nEvents@ (from zero to nEvents minus one), then rolls back to
-- @rollbackPoint@.
rollbackRingBufferComprehensiveTest :: H.Property
rollbackRingBufferComprehensiveTest = H.property $ do
  let n = 20
  bufferSize    :: Natural <- H.forAll $ Gen.integral (Range.linear 0 $ 2 * n)
  nEvents       :: Natural <- H.forAll $ Gen.integral (Range.linear 0 $ 2 * n)
  rollbackPoint :: Natural <- H.forAll $ Gen.integral (Range.linear 0 n)

  let intRollbackRingBuffer = rollbackRingBuffer bufferSize :: IntBuffer

      events :: [Int]
      events = [0 .. fromEnum nEvents - 1]

      source :: IntStream
      source = do
        forM_ events (S.yield . mkEvent)
        S.yield $ RollBackward $ fromEnum rollbackPoint

  liftIO (IO.try $ S.toList_ $ intRollbackRingBuffer source) >>= \case
    Left (RollbackLocationNotFound (p :: Int)) -> if
      | rollbackPoint >= nEvents -> do
          H.label $ "Rollback point '" <> fromString (show p) <> "' not in generated events"
          H.success
      | rollbackPoint < nEvents - bufferSize -> do
          H.label $ "Rollback point '" <> fromString (show p) <> "' not in buffer anymore"
          H.success
      | otherwise -> fail $ "Rollback point '" <> fromString (show p) <> "' was not found, but none of the valid conditions for it matched !!"

    Right overflow -> if nEvents <= bufferSize
      then do
        H.label "All events fit to buffer"
        H.assert $ rollbackPoint <= nEvents && null overflow
      else do
        H.label "Event for rollback point still in buffer"
        H.assert $ let
          min' = nEvents - bufferSize
          max' = nEvents - 1
          in min' <= rollbackPoint && rollbackPoint <= max'
          && overflow == take (fromEnum $ nEvents - bufferSize) events

-- * Helpers

-- | Make an event where payload and point are the same.
mkEvent :: a -> Event a a
mkEvent a = RollForward a a
