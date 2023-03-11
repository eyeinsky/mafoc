{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE MultiWayIf       #-}
module Mafoc.RollbackRingBuffer where

import Control.Exception qualified as IO
import Control.Monad.Primitive (PrimState)
import Control.Monad.Trans.Class (lift)
import Data.Typeable (Typeable)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

data Event a p
  = RollForward a p p
  | RollBackward p p

data RollbackException e
  = RollbackLocationNotFound e

instance (Show e, Typeable e) => IO.Exception (RollbackException e)
deriving instance Show e => Show (RollbackException e)

rollbackRingBuffer :: (Ord p, Show p, Typeable p) => Natural -> S.Stream (S.Of (Event a p)) IO r -> S.Stream (S.Of a) IO r
rollbackRingBuffer bufferSizeNat eventStream = case bufferSizeNat of
  0 -> throwOnRollBacward eventStream
  _ -> let bufferSize = fromEnum bufferSizeNat :: Int
    in do
    vector :: VG.Mutable V.Vector (PrimState IO) (a, p) <- lift (VGM.new bufferSize)
    fill 0 0 eventStream bufferSize vector

-- | Fill phase, don't yield anything. Consume ChainSyncEvents and
-- roll back to an earlier location in the vector in case of rollback.
fill
  :: (Eq p, Show p, Typeable p)
  => Int -> Int
  -> S.Stream (S.Of (Event a p)) IO r
  -> Int
  -> VG.Mutable V.Vector (PrimState IO) (a, p)
  -> S.Stream (S.Of a) IO r
fill i j source bufferSize vector = streamPassReturn source $ \event source' -> case event of
  RollForward a ep _cp -> do
    lift $ VGM.unsafeWrite vector i (a, ep)
    let i' = (i + 1) `rem` bufferSize
        j' = j + 1
    if j' == bufferSize
      then fillYield i' source' bufferSize vector
      else fill i' j' source' bufferSize vector

  RollBackward p _ -> rewind p i j source' bufferSize vector

-- | Fill & yield phase. Buffer is full in the beginning, but will
-- need to be refilled when a rollback occurs.
fillYield :: (Eq p, Show p, Typeable p) => Int -> S.Stream (S.Of (Event a p)) IO r -> Int -> VG.Mutable V.Vector (PrimState IO) (a, p) -> S.Stream (S.Of a) IO r
fillYield i source bufferSize vector = streamPassReturn source $ \event source' -> case event of
  RollForward a ep _cp -> do
    (a', _) <- lift $ VGM.exchange vector i (a, ep)
    S.yield a'
    let i' = (i + 1) `rem` bufferSize
    fillYield i' source' bufferSize vector
  RollBackward rp _ -> rewind rp i bufferSize source' bufferSize vector



rewind :: (Eq p, Show p, Typeable p) => p -> Int -> Int -> S.Stream (S.Of (Event a p)) IO r -> Int -> VG.Mutable V.Vector (PrimState IO) (a, p) -> S.Stream (S.Of a) IO r
rewind p i j source' bufferSize vector = do
  frozen :: V.Vector (a, b) <- lift $ VG.unsafeFreeze vector
  let maybeFound = findRollbackIndex p i j frozen bufferSize
  case maybeFound of
    Just foundAtIndex -> let
      (i', j') = calculateRewind foundAtIndex bufferSize i j
      in fill i' j' source' bufferSize vector
    _ -> lift $ IO.throwIO $ RollbackLocationNotFound p

-- | Find index for the rollback point.
findRollbackIndex :: Eq p => p -> Int -> Int -> V.Vector (a, p) -> Int -> Maybe Int
findRollbackIndex p i j frozen bufferSize = let
  findIn = VG.findIndex ((p ==) . snd)
  in if j == 0
  -- nowhere to search in
  then Nothing
  -- j > 0, somewhere to search in
  else
    if i > 0
    then
      if j > i
      -- two slices
      then let
        prefix = VG.basicUnsafeSlice 0 i frozen
        ji = j - i
        suffixStart = bufferSize - ji
        suffix = VG.basicUnsafeSlice suffixStart ji frozen
        in case findIn prefix of
             Just iPrefix -> Just iPrefix
             _ -> case findIn suffix of
                    Just iSuffix -> Just (iSuffix + suffixStart)
                    _            -> Nothing
      -- one slice, j <= i
      else findIn $ VG.basicUnsafeSlice (i - j) j frozen
    -- one slice, i = 0
    else let sliceStart = bufferSize - j
      in fmap (sliceStart +) $ findIn $ VG.basicUnsafeSlice sliceStart j frozen

-- | Calculate current and filled for found rollback point at @foundAtIndex@.
calculateRewind :: Int -> Int -> Int -> Int -> (Int, Int)
calculateRewind foundAtIndex vectorSize i j = let
  i' = foundAtIndex + 1
  j' = j - (if i > i' then i - i' else i + (j - i'))
  in (i' `rem` vectorSize, j')

-- * Helpers

ignoreRollbacks :: Monad m => S.Stream (S.Of (Event a p)) m r -> S.Stream (S.Of a) m r
ignoreRollbacks = S.mapMaybe (\case RollForward e _ _ -> Just e; _ -> Nothing)

throwOnRollBacward :: (Show p, Typeable p) => S.Stream (S.Of (Event a p)) IO r -> S.Stream (S.Of a) IO r
throwOnRollBacward = S.mapM $ \event -> case event of
  RollBackward rp _cp   -> IO.throwIO $ RollbackLocationNotFound rp
  RollForward a _ep _cp -> return a

streamPassReturn :: Monad m => S.Stream (S.Of a) m r -> (a -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r) -> S.Stream (S.Of b) m r
streamPassReturn source f = lift (S.next source) >>= \case
  Left r                 -> pure r
  Right (event, source') -> f event source'
