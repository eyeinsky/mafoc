{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Maps.BlockTxCount where

import Control.Exception qualified as IO
import Control.Monad.IO.Class (liftIO)
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Logging (logging)
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Streaming qualified as CS
import Mafoc.Helpers qualified as Mafoc
import Mafoc.RollbackRingBuffer qualified as Mafoc

-- * Block transaction count indexer

data BlockTxCount = BlockTxCount
  { blockTxCountChunkSize :: Int
  }

type Row = (Word64, C.Hash C.BlockHeader, Word64, Int)

-- | Count transactions for every block.
blockTxCount
  :: SQL.Connection
  -> BlockTxCount
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
blockTxCount sqlCon config source = do
  liftIO sqliteCreateTable
  source
    & S.chunksOf (blockTxCountChunkSize config)
    & S.mapped (\chunk -> do
                   sqliteInsert . map toRow =<< S.toList_ chunk
                   pure chunk
               )
    & S.concats

  where
    toRow :: C.BlockInMode C.CardanoMode -> Row
    toRow (C.BlockInMode (C.Block (C.BlockHeader slotNo hash blockNo) txs) _) = (coerce slotNo, hash, coerce blockNo, length txs)

    sqliteCreateTable :: IO ()
    sqliteCreateTable = SQL.execute_ sqlCon
      "CREATE TABLE IF NOT EXISTS slot_blocks (slotNo INT NOT NULL, blockHeaderHash BLOB NOT NULL, blockNo INT NOT NULL, txCount INT NOT NULL)"

    sqliteInsert :: [Row] -> IO ()
    sqliteInsert rows = SQL.executeMany sqlCon "INSERT INTO slot_blocks (slotNo, blockHeaderHash, blockNo, txCount) VALUES (?, ?, ?, ?)" rows

lastCp :: SQL.Connection -> IO (Maybe C.ChainPoint)
lastCp sqlCon = do
  rows :: [Row] <- SQL.query_ sqlCon "SELECT slotNo, blockHeaderHash, blockNo, txCount FROM slot_blocks ORDER BY slotNo DESC LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                        -> Nothing

indexer :: FilePath -> String -> FilePath -> Maybe C.ChainPoint -> Maybe C.SlotNo -> IO ()
indexer socketPath dbPath nodeConfig cpFromCli maybeEnd = do
  sqlCon <- SQL.open dbPath
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  let localNodeConnectInfo = CS.mkConnectInfo env socketPath
      _con = CS.mkLocalNodeConnectInfo networkId socketPath
  k <- liftIO $ Mafoc.getSecurityParam nodeConfig
  c <- defaultConfigStdout
  cpFromDb :: Either SQL.SQLError (Maybe C.ChainPoint) <- IO.try (lastCp sqlCon)
  let (startCp, msg) = if
        | Right (Just cp) <- cpFromDb -> (cp, "Resume from database: " <> show cp)
        | Just cp <- cpFromCli        -> (cp, "Start from CLI argument: " <> show cp)
        | otherwise                   -> (C.ChainPointAtGenesis, "Start from genesis")
  putStrLn msg
  withTrace c "mafoc" $ \trace ->
    let
      stream = CS.blocks localNodeConnectInfo startCp
        & logging trace
        & Mafoc.fromChainSyncEvent
        & S.drop 1
        & Mafoc.rollbackRingBuffer k
        & blockTxCount sqlCon (BlockTxCount 3000)

    in S.effects $ case maybeEnd of
      Just endSlotNo -> S.takeWhile ((<= endSlotNo) . CS.bimSlotNo) stream
      _              -> stream
  where
    networkId = C.Mainnet
