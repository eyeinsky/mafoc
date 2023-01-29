module Mafoc.Indexer.Class where


class Indexer a where

  -- | Runtime configuration for an indexer.
  data Runtime a :: *

  -- | Initialize an indexer and return its runtime configuration. E.g
  -- open the destination to where data is persisted, etc.
  initialize :: a -> IO (Runtime a)

  -- | Run the indexer.
  run :: Runtime a -> IO ()
