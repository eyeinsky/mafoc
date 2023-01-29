{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Maps.BlockBasics where

import Cardano.Api qualified as C
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt

import Cardano.Streaming.Helpers qualified as CS
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Streaming qualified as CS
import Mafoc.Helpers (fromChainSyncEvent, getIndexerBookmarkSqlite, getSecurityParam, sqliteCreateBookmarsks)
import Mafoc.Indexer.Class (Indexer (Runtime, initialize, run))
import Mafoc.RollbackRingBuffer (rollbackRingBuffer)

-- * Block transaction count indexer

data BlockBasics = BlockBasics
  { chunkSize                 :: Int
  , socketPath                :: String
  , dbPath                    :: String
  , securityParamOrNodeConfig :: Either Natural FilePath
  , startingPointOverride     :: Maybe C.ChainPoint
  , end                       :: Maybe C.SlotNo
  , networkId                 :: C.NetworkId
  } deriving (Show)

parseCli :: Opt.ParserInfo BlockBasics
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "blockbasics"
  <> Opt.header "blockbasics - Index block header hash and number of transactions per block"
  where
    cli :: Opt.Parser BlockBasics
    cli = BlockBasics
      <$> Opt.option Opt.auto (Opt.longOpt "chunk-size" "Size of buffer to be inserted into sqlite" <> Opt.internal)
      <*> Opt.commonSocketPath
      <*> Opt.commonDbPath
      <*> Opt.commonSecurityParamEither
      <*> Opt.commonMaybeChainPointStart
      <*> Opt.commonMaybeUntilSlot
      <*> Opt.commonNetworkId

instance Indexer BlockBasics where
  data Runtime BlockBasics = Runtime
    { sqlConnection       :: SQL.Connection
    , startingPoint       :: C.ChainPoint
    , localNodeConnection :: C.LocalNodeConnectInfo C.CardanoMode
    , securityParam       :: Natural
    , cliConfig           :: BlockBasics
    }

  initialize config = do
    c <- SQL.open $ dbPath config
    sqliteInit c
    sqliteCreateBookmarsks c
    startingPoint <- case startingPointOverride config of
      Just cp -> return cp
      _       -> return . fromMaybe C.ChainPointAtGenesis =<< getIndexerBookmarkSqlite c "blockbasics"
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (Runtime c startingPoint localNodeCon k config)

  run (Runtime{sqlConnection, localNodeConnection, startingPoint, securityParam, cliConfig}) =
    S.effects $ streamer sqlConnection localNodeConnection startingPoint securityParam (chunkSize cliConfig)

type Row = (Word64, C.Hash C.BlockHeader, Int)

-- | Count transactions for every block.
streamer
  :: SQL.Connection -> C.LocalNodeConnectInfo C.CardanoMode -> C.ChainPoint -> Natural -> Int
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
streamer sqlCon lnCon startingPoint k chunkSize = do
  CS.blocks lnCon startingPoint
    & fromChainSyncEvent
    & rollbackRingBuffer k
    & S.chunksOf chunkSize
    & S.mapped (\chunk -> do
                   sqliteInsert . map toRow =<< S.toList_ chunk
                   pure chunk
               )
    & S.concats

  where
    toRow :: C.BlockInMode C.CardanoMode -> Row
    toRow (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _) txs) _) = (coerce slotNo, hash, length txs)

    sqliteInsert :: [Row] -> IO ()
    sqliteInsert rows = SQL.executeMany sqlCon "INSERT INTO block_basics (slot_no, block_header_hash, tx_count) VALUES (?, ?, ?)" rows

sqliteInit :: SQL.Connection -> IO ()
sqliteInit sqlCon = SQL.execute_ sqlCon
  "CREATE TABLE IF NOT EXISTS block_basics (slot_no INT NOT NULL, block_header_hash BLOB NOT NULL, tx_count INT NOT NULL)"

lastCp :: SQL.Connection -> IO (Maybe C.ChainPoint)
lastCp sqlCon = do
  rows :: [Row] <- SQL.query_ sqlCon "SELECT slot_no, block_header_hash, tx_count FROM block_basics ORDER BY slot_no DESC LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                      -> Nothing
