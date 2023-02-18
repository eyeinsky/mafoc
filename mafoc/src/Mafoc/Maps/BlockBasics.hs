{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Maps.BlockBasics where

import Data.Coerce (coerce)
import Data.Function ((&))
import Data.String (IsString (fromString))

import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming.Helpers qualified as CS

import Cardano.Streaming qualified as CS
import Mafoc.Helpers (DbPathAndTableName, Interval (Interval), defaultTableName, findIntervalToBeIndexed,
                      fromChainSyncEvent, getSecurityParam, sqliteCreateBookmarsks, takeUpTo)
import Mafoc.Indexer.Class (Indexer (Runtime, initialize, run))
import Mafoc.RollbackRingBuffer (Event (RollBackward), rollbackRingBuffer)

-- * Block transaction count indexer

data BlockBasics = BlockBasics
  { chunkSize                 :: Int
  , socketPath                :: String
  , dbPathAndTableName        :: DbPathAndTableName
  , securityParamOrNodeConfig :: Either Natural FilePath
  , interval                  :: Interval
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
      <*> Opt.commonDbPathAndTableName
      <*> Opt.commonSecurityParamEither
      <*> Opt.commonInterval
      <*> Opt.commonNetworkId

instance Indexer BlockBasics where
  data Runtime BlockBasics = Runtime
    { sqlConnection       :: SQL.Connection
    , tableName           :: String
    , interval_           :: Interval
    , localNodeConnection :: C.LocalNodeConnectInfo C.CardanoMode
    , securityParam       :: Natural
    , cliConfig           :: BlockBasics
    }

  initialize config = do
    let (dbPath, tableName) = defaultTableName "blockbasics" $ dbPathAndTableName config
    c <- SQL.open dbPath
    sqliteInit c tableName
    sqliteCreateBookmarsks c
    interval' <- findIntervalToBeIndexed (interval config) c tableName
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (Runtime c tableName interval' localNodeCon k config)

  run (Runtime{sqlConnection, tableName, localNodeConnection, interval_, securityParam, cliConfig}) =
    S.effects $ streamer sqlConnection tableName localNodeConnection interval_ securityParam (chunkSize cliConfig)

type Row = (Word64, C.Hash C.BlockHeader, Int)

-- | Count transactions for every block.
streamer
  :: SQL.Connection -> String -> C.LocalNodeConnectInfo C.CardanoMode -> Interval -> Natural -> Int
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
streamer sqlCon tableName lnCon (Interval from upTo) k chunkSize = do
  CS.blocks lnCon from
    & fromChainSyncEvent
    & skipFirstGenesis from
    & rollbackRingBuffer k
    & takeUpTo upTo
    & case chunkSize of
        1 -> S.chain (\e -> sqliteInsert [blockToRow e])
        _ -> \source -> source
          & S.chunksOf chunkSize
          & S.mapped (\chunk -> do
                         sqliteInsert . map blockToRow =<< S.toList_ chunk
                         pure chunk
                     )
          & S.concats

  where
    blockToRow :: C.BlockInMode C.CardanoMode -> Row
    blockToRow (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _) txs) _) = (coerce slotNo, hash, length txs)

    sqliteInsert :: [Row] -> IO ()
    sqliteInsert rows = SQL.executeMany sqlCon template rows
      where
        template = "INSERT INTO " <> fromString tableName <> " (slot_no, block_header_hash, tx_count) VALUES (?, ?, ?)"

skipFirstGenesis
  :: Monad m
  => C.ChainPoint
  -> S.Stream (S.Of (Event a (C.ChainPoint, b))) m r
  -> S.Stream (S.Of (Event a (C.ChainPoint, b))) m r
skipFirstGenesis cp source = case cp of
  C.ChainPointAtGenesis -> S.lift (S.next source) >>= \case
    Left r -> pure r
    Right (event, source') -> case event of
      RollBackward (C.ChainPointAtGenesis, _) -> source'
      e                                       -> S.yield e >> source'
  _ -> source

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit sqlCon tableName = SQL.execute_ sqlCon $
  "CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " (slot_no INT NOT NULL, block_header_hash BLOB NOT NULL, tx_count INT NOT NULL)"

lastCp :: SQL.Connection -> IO (Maybe C.ChainPoint)
lastCp sqlCon = do
  rows :: [Row] <- SQL.query_ sqlCon "SELECT slot_no, block_header_hash, tx_count FROM block_basics ORDER BY slot_no DESC LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                      -> Nothing
