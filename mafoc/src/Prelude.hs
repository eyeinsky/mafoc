{-# OPTIONS_GHC -Wno-missing-import-lists #-}
module Prelude
  ( module Prelude
  , module Export
  ) where

-- * Re-export

import BasePrelude as Export

import Data.Coerce as Export (Coercible, coerce)
import Data.Functor as Export (void, ($>), (<&>))
import Data.Maybe as Export (fromMaybe, mapMaybe, listToMaybe, catMaybes)
import Data.Proxy as Export (Proxy (Proxy))
import Control.Monad as Export (when)
import Data.Function as Export ((&), on)
import Data.String as Export (IsString, fromString)
import Data.Word as Export (Word32, Word64)
import GHC.Generics as Export (Generic)
import Numeric.Natural as Export (Natural)
import Data.Char as Export (isDigit, toLower, toUpper)
import Text.Read as Export (readEither, readMaybe)
import Data.Bifunctor as Export (first, second)
import Data.Scientific as Export

import Control.Applicative as Export ((<|>), Alternative)
import Control.Monad.Trans.Class as Export (MonadTrans, lift)
import Control.Monad.IO.Class as Export (MonadIO, liftIO)
import Control.Monad as Export (foldM, forM, forM_)

-- * Local use

import Data.Time qualified as Time

-- | Helper to print a value with label.
labelPrint :: Show a => String -> a -> IO ()
labelPrint label a = putStrLn $ label <> ": " <> show a

-- | Measure time an IO action took.
timeIO :: IO a -> IO (a, Time.NominalDiffTime)
timeIO action = do
  t0 <- Time.getCurrentTime
  a <- action
  t1 <- Time.getCurrentTime
  return (a, Time.diffUTCTime t1 t0)

todo :: a
todo = undefined
