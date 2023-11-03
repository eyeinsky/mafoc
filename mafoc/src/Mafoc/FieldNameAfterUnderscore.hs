{-# LANGUAGE UndecidableInstances #-}
-- TODO Are UndecidableInstances unavoidable? The A.GToJSON' constraints require it.

module Mafoc.FieldNameAfterUnderscore (FieldNameAfterUnderscore(FieldNameAfterUnderscore)) where

import GHC.Generics (Rep)
import Data.Aeson qualified as A

newtype FieldNameAfterUnderscore a = FieldNameAfterUnderscore a

fieldNameAfterUnderscore :: A.Options
fieldNameAfterUnderscore = A.defaultOptions { A.fieldLabelModifier = drop 1 . dropWhile (/= '_') }

instance ( Generic a, A.ToJSON a
         , A.GToJSON' A.Value A.Zero (Rep a)
         , A.GToJSON' A.Encoding A.Zero (Rep a) )
  => A.ToJSON (FieldNameAfterUnderscore a) where

  toJSON (FieldNameAfterUnderscore a) = A.genericToJSON fieldNameAfterUnderscore a
  toEncoding (FieldNameAfterUnderscore a) = A.genericToEncoding fieldNameAfterUnderscore a
