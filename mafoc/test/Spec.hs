{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import RollbackRingBuffer qualified
import Test.Tasty (defaultMain, testGroup)

main :: IO ()
main = defaultMain $ testGroup "Mafoc"
  [ RollbackRingBuffer.tests ]
