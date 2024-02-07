module Mafoc.Cmds.Eras where

import Data.List qualified as L
import Mafoc.Upstream

allEras :: [LedgerEra]
allEras = [minBound .. maxBound]

printEras :: IO ()
printEras = putStrLn $ unlines
  [ "Eras: " <> list <> "."
  , ""
  , "Currently available eras. Use these on the commandline for interval definitions, e.g \"alonzo-babbage\" will index from start of Alonzo to end of Babbage."
  ]
  where
    list = L.intercalate ", " $ map (map toLower . show) allEras
