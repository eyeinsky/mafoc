{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Maps.BlockBasics where

import Data.Coerce (coerce)
import Data.String (IsString (fromString))
import Data.Word (Word32)

import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C

import Cardano.Streaming qualified as CS
import Mafoc.Core (BlockSourceConfig (BlockSourceConfig), DbPathAndTableName,
                   Indexer (Event, Runtime, State, initialize, persist, toEvent), Interval, defaultTableName,
                   findIntervalToBeIndexed, getSecurityParam, sqliteInitBookmarks)

-- * Block transaction count indexer

data BlockBasics = BlockBasics
  { chunkSize                 :: Int
  , socketPath                :: String
  , dbPathAndTableName        :: DbPathAndTableName
  , securityParamOrNodeConfig :: Either Natural FilePath
  , interval                  :: Interval
  , networkId                 :: C.NetworkId
  , logging                   :: Bool
  , pipelineSize              :: Word32
  } deriving (Show)

parseCli :: Opt.ParserInfo BlockBasics
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "blockbasics"
  <> Opt.header "blockbasics - Index block header hash and number of transactions per block"
  where
    cli :: Opt.Parser BlockBasics
    cli = BlockBasics
      <$> Opt.commonChunkSize
      <*> Opt.commonSocketPath
      <*> Opt.commonDbPathAndTableName
      <*> Opt.commonSecurityParamEither
      <*> Opt.commonInterval
      <*> Opt.commonNetworkId
      <*> Opt.commonQuiet
      <*> Opt.commonPipelineSize

instance Indexer BlockBasics where

  type Event BlockBasics = Row

  data State BlockBasics = EmptyState

  data Runtime BlockBasics = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }

  toEvent a _ = Just (EmptyState, blockToRow a)

  persist Runtime{sqlConnection, tableName} event = sqliteInsert sqlConnection tableName event

  initialize config = do
    let (dbPath, tableName) = defaultTableName "blockbasics" $ dbPathAndTableName config
    c <- SQL.open dbPath
    sqliteInit c tableName
    sqliteInitBookmarks c
    interval' <- findIntervalToBeIndexed (interval config) c tableName
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (EmptyState, BlockSourceConfig localNodeCon interval' k (logging config) (pipelineSize config), Runtime c tableName)

type Row = (Word64, C.Hash C.BlockHeader, Int)

blockToRow :: C.BlockInMode C.CardanoMode -> Row
blockToRow (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _) txs) _) = (coerce slotNo, hash, length txs)

sqliteInsert :: SQL.Connection -> String -> Row -> IO ()
sqliteInsert sqlCon tableName row = SQL.executeMany sqlCon template [row]
  where
    template = "INSERT INTO " <> fromString tableName <> " (slot_no, block_header_hash, tx_count) VALUES (?, ?, ?)"

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit sqlCon tableName = SQL.execute_ sqlCon $
  "CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " (slot_no INT NOT NULL, block_header_hash BLOB NOT NULL, tx_count INT NOT NULL)"

lastCp :: SQL.Connection -> IO (Maybe C.ChainPoint)
lastCp sqlCon = do
  rows :: [Row] <- SQL.query_ sqlCon "SELECT slot_no, block_header_hash, tx_count FROM block_basics ORDER BY slot_no DESC LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                      -> Nothing
