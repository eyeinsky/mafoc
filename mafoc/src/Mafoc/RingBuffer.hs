{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE DerivingStrategies #-}

module Mafoc.RingBuffer
  ( RingBuffer(ringBufferFill),  new, peek, reset

  -- * Add elements
  , push, push_, pushMany

  -- * Remove oldest elements
  , pop, pop_, flush, flush_, flushN, flushWhile, flushWhile_

  -- * Remove newest elements
  , unpushWhile, unpushWhile_
  ) where

import Control.Arrow (second)
import Control.Exception (Exception, throwIO)
import Control.Monad (foldM)
import Control.Monad.Primitive (PrimState)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM

import Numeric.Natural (Natural)

-- * RingBuffer

data RingBuffer a = RingBuffer
  { ringBufferVector :: VG.Mutable V.Vector (PrimState IO) a
  , _ringBufferSize  :: Natural
  , _ringBufferIndex :: Int
  , ringBufferFill   :: Natural
  }

data RingBufferException
  = Fill_larger_than_size
  deriving Show
  deriving anyclass Exception

new :: Natural -> IO (RingBuffer a)
new size = do
  vector :: VG.Mutable V.Vector (PrimState IO) a <- VGM.new $ fromIntegral size
  return $ RingBuffer vector size 0 0

-- * Add items

-- | Push @a@ to buffer, possibly returning carry and the new ringbuffer.
push :: a -> RingBuffer a -> IO (RingBuffer a, Maybe a)
push a (RingBuffer vector size index fill) = case fill `compare` size of
  LT -> do
    VGM.unsafeWrite vector index a
    let rb' = RingBuffer vector size (next index) (fill + 1)
    return (rb', Nothing)
  EQ -> do
    a' <- VGM.exchange vector index a
    let rb' = RingBuffer vector size (next index) fill
    return (rb', Just a')
  GT -> throwIO Fill_larger_than_size

  where
    next i = (i + 1) `rem` fromIntegral size

push_ :: a -> RingBuffer a -> IO (RingBuffer a)
push_ a rb = fst <$> push a rb

-- | Push many @as@ to @ringBuffer@, returning overflow.
pushMany :: [a] -> RingBuffer a -> IO (RingBuffer a, [a])
pushMany as ringBuffer = second reverse <$> foldM step (ringBuffer, []) as
  where
    step (rb, overflow) a = do
      (rb', ma) <- push a rb
      return (rb', maybe overflow (: overflow) ma)

-- * Remove older items

-- | Pop oldest item.
pop :: RingBuffer a -> IO (RingBuffer a, Maybe a)
pop rb@(RingBuffer vector size index fill) = case fill of
  0 -> return (rb, Nothing)
  _ -> do
    Just a <- peek rb
    let rb' = RingBuffer vector size index (fill - 1)
    return (rb', Just a)

pop_ :: RingBuffer a -> IO (RingBuffer a)
pop_ rb = fst <$> pop rb

flush :: RingBuffer a -> IO (RingBuffer a, [a])
flush rb = second reverse <$> go rb []
  where
    go rb' as = do
      (rb'', ma) <- pop rb'
      maybe (return (rb'', as)) (go rb'' . (:as)) ma

flush_ :: RingBuffer a -> IO (RingBuffer a)
flush_ rb = fst <$> flush rb

-- | Pop @n@ oldest items, or less if buffer fill is smaller.
flushN :: Natural -> RingBuffer a -> IO (RingBuffer a, [a])
flushN 0 rb = return (rb, [])
flushN n rb = second reverse <$> go n rb []
  where
    go 0 rb' as = return (rb', as)
    go n' rb' as = do
      (rb'', ma) <- pop rb'
      case ma of
        Just a -> go (n' - 1) rb'' (a : as)
        _      -> return (rb'', as)

flushWhile :: (a -> Bool) -> RingBuffer a -> IO (RingBuffer a, [a])
flushWhile predicate rb = second reverse <$> go rb []
  where
    go rb' as = do
      ma <- peek rb'
      if | Just a <- ma, predicate a -> do
             (rb'', _) <- pop rb'
             go rb'' (a : as)
         | otherwise -> return (rb', as)

flushWhile_ :: (a -> Bool) -> RingBuffer a -> IO (RingBuffer a)
flushWhile_ predicate rb = fst <$> flushWhile predicate rb

reset :: RingBuffer a -> RingBuffer a
reset (RingBuffer vector size _index _fill) = (RingBuffer vector size 0 0)

-- * Remove newer items

-- | Pop newest item added to ringbuffer.
unpush :: RingBuffer a -> IO (RingBuffer a, Maybe a)
unpush rb@(RingBuffer vector size _index fill) = case indexLatest rb of
  Just index' -> do
    a <- VGM.read vector index'
    return (RingBuffer vector size index' (fill - 1), Just a)
  _ -> return (rb, Nothing)

-- | Unpush newest items from buffer while predicate matches.
unpushWhile :: (a -> Bool) -> RingBuffer a -> IO (RingBuffer a, [a])
unpushWhile predicate rb = second reverse <$> go rb []
  where
    go rb' as = do
      ma <- peekLatest rb'
      if | Just a <- ma, predicate a -> do
             (rb'', _) <- unpush rb'
             go rb'' (a : as)
         | otherwise -> return (rb', as)

unpushWhile_ :: (a -> Bool) -> RingBuffer a -> IO (RingBuffer a)
unpushWhile_ predicate rb = fst <$> unpushWhile predicate rb

-- * Access items in buffer without modification

peek :: RingBuffer a -> IO (Maybe a)
peek rb = traverse (VGM.read $ ringBufferVector rb) (peekIndex rb)

peekLatest :: RingBuffer a -> IO (Maybe a)
peekLatest rb = traverse (VGM.read $ ringBufferVector rb) (indexLatest rb)

-- | Index of the oldest item
peekIndex :: RingBuffer a -> Maybe Int
peekIndex (RingBuffer _vector size index fill) = case fill of
  0 -> Nothing
  _ -> Just $ (index - fromIntegral fill) `mod` fromIntegral size

-- | Index of the latest item added to the buffer
indexLatest :: RingBuffer a -> Maybe Int
indexLatest (RingBuffer _vector size index fill) = case fill of
  0 -> Nothing
  _ -> Just $ (index - 1) `mod` fromIntegral size
