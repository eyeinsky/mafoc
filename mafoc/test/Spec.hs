{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import RingBuffer qualified
import RollbackRingBuffer qualified
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main = defaultMain $ testGroup "Mafoc"
  [ RingBuffer.tests
  , RollbackRingBuffer.tests ]
