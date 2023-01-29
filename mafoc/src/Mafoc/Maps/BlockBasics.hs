{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Maps.BlockBasics where

import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
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
import Mafoc.Helpers (blockSlotNo, fromChainSyncEvent, getIndexerBookmarkSqlite, getSecurityParam,
                      sqliteCreateBookmarsks)
import Mafoc.Indexer.Class (Indexer (Runtime, initialize, run))
import Mafoc.RollbackRingBuffer (Event (RollBackward), rollbackRingBuffer)

-- * Block transaction count indexer

data BlockBasics = BlockBasics
  { chunkSize                 :: Int
  , socketPath                :: String
  , dbPath                    :: String
  , securityParamOrNodeConfig :: Either Natural FilePath
  , startingPointOverride     :: Maybe C.ChainPoint
  , untilSlot                 :: Maybe C.SlotNo
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
    , maybeEndingPoint    :: Maybe C.SlotNo
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
    return (Runtime c startingPoint (untilSlot config) localNodeCon k config)

  run (Runtime{sqlConnection, localNodeConnection, startingPoint, maybeEndingPoint, securityParam, cliConfig}) =
    S.effects $ streamer sqlConnection localNodeConnection startingPoint maybeEndingPoint securityParam (chunkSize cliConfig)

type Row = (Word64, C.Hash C.BlockHeader, Int)

-- | Count transactions for every block.
streamer
  :: SQL.Connection -> C.LocalNodeConnectInfo C.CardanoMode -> C.ChainPoint -> Maybe C.SlotNo -> Natural -> Int
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
streamer sqlCon lnCon startingPoint maybeUntil k chunkSize = do
  CS.blocks lnCon startingPoint
    & fromChainSyncEvent
    & skipFirstGenesis startingPoint
    & rollbackRingBuffer k
    & maybe id (\slotNo -> S.takeWhile (\blk -> blockSlotNo blk < slotNo)) maybeUntil
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
    sqliteInsert rows = SQL.executeMany sqlCon "INSERT INTO block_basics (slot_no, block_header_hash, tx_count) VALUES (?, ?, ?)" rows

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

sqliteInit :: SQL.Connection -> IO ()
sqliteInit sqlCon = SQL.execute_ sqlCon
  "CREATE TABLE IF NOT EXISTS block_basics (slot_no INT NOT NULL, block_header_hash BLOB NOT NULL, tx_count INT NOT NULL)"

lastCp :: SQL.Connection -> IO (Maybe C.ChainPoint)
lastCp sqlCon = do
  rows :: [Row] <- SQL.query_ sqlCon "SELECT slot_no, block_header_hash, tx_count FROM block_basics ORDER BY slot_no DESC LIMIT 1"
  pure $ case rows of
    ((slotNo, hash,_) : _) -> Just (C.ChainPoint (coerce slotNo) hash)
    _                      -> Nothing
