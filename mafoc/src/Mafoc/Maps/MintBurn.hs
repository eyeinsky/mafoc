{-# LANGUAGE LambdaCase #-}

{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.

-}

module Mafoc.Maps.MintBurn where

import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as C
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

import Mafoc.Helpers (fromChainSyncEvent, getSecurityParam, loopM)
import Mafoc.RollbackRingBuffer (rollbackRingBuffer)

-- | Setup and start the streamer
indexer :: FilePath -> C.NetworkId -> FilePath -> Maybe C.ChainPoint -> Maybe C.SlotNo -> Either Natural FilePath -> IO ()
indexer socketPath networkId dbPath start _end kOrNodeConfig = do
  let localNodeCon = CS.mkLocalNodeConnectInfo networkId socketPath
      start' = fromMaybe C.ChainPointAtGenesis start
  k <- either pure getSecurityParam kOrNodeConfig
  sqlCon <- SQL.open dbPath
  S.effects $ streamer sqlCon localNodeCon start' k

streamer :: SQL.Connection -> C.LocalNodeConnectInfo C.CardanoMode -> C.ChainPoint -> Natural -> S.Stream (S.Of Marconi.MintBurn.TxMintEvent) IO r
streamer sqlCon lnCon chainPoint k = C.blocks lnCon chainPoint
  & fromChainSyncEvent
  & rollbackRingBuffer k
  & S.mapMaybe Marconi.MintBurn.toUpdate
  & \source -> do
      liftIO (Marconi.MintBurn.sqliteInit sqlCon)
      loopM source $ \event -> do
        liftIO $ Marconi.MintBurn.sqliteInsert sqlCon [event]
        S.yield event
