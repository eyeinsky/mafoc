{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Helpers where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Maybe (fromMaybe)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Mafoc.RollbackRingBuffer (Event (RollBackward, RollForward))
import Marconi.ChainIndex.Indexers.MintBurn ()

-- * Interval

data UpTo
  = SlotNo C.SlotNo
  | Infinity
  | CurrentTip
  deriving (Show)

data Interval = Interval
  { from :: C.ChainPoint
  , upTo :: UpTo
  } deriving (Show)

chainPointLaterThanFrom :: C.ChainPoint -> Interval -> Bool
chainPointLaterThanFrom cp (Interval from' _) = from' <= cp

takeUpTo :: UpTo -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO () -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
takeUpTo upTo' source = case upTo' of
  SlotNo slotNo -> S.takeWhile (\blk -> blockSlotNo blk < slotNo) source
  Infinity -> source
  CurrentTip -> S.lift (S.next source) >>= \case
    Left r -> return r
    Right (event, source') -> let
      slotNo = blockSlotNo event
      in do
      S.yield event
      S.takeWhile (\blk -> blockSlotNo blk < slotNo) source'

-- * Additions to cardano-api

getSecurityParam :: FilePath -> IO Natural
getSecurityParam nodeConfig = do
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure $ fromIntegral $ C.envSecurityParam env

-- | Convert event from @ChainSyncEvent@ to @Event@.
fromChainSyncEvent
  :: Monad m
  => S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) m r
  -> S.Stream (S.Of (Event (C.BlockInMode mode) (C.ChainPoint, C.ChainTip))) m r
fromChainSyncEvent = S.map $ \e -> case e of
  CS.RollForward a ct   -> RollForward a (blockChainPoint a, ct)
  CS.RollBackward cp ct -> RollBackward (cp, ct)

instance Ord C.ChainTip where
  compare C.ChainTipAtGenesis C.ChainTipAtGenesis = EQ
  compare C.ChainTipAtGenesis _                   = LT
  compare _ C.ChainTipAtGenesis                   = GT
  compare (C.ChainTip a _ _) (C.ChainTip b _ _)   = compare a b

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

blockSlotNo :: C.BlockInMode mode -> C.SlotNo
blockSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) _) _) = slotNo

-- * Sqlite

sqliteCreateBookmarsks :: SQL.Connection -> IO ()
sqliteCreateBookmarsks c = do
  SQL.execute_ c "CREATE TABLE IF NOT EXISTS bookmarks (indexer TEXT NOT NULL, slot_no INT NOT NULL, block_header_hash BLOB NOT NULL)"

-- | Get bookmark (the place where we left off) for an indexer with @name@
getIndexerBookmarkSqlite :: SQL.Connection -> String -> IO (Maybe C.ChainPoint)
getIndexerBookmarkSqlite c name = do
  list <- SQL.query c "SELECT indexer, slot_no, block_header_hash FROM bookmarks WHERE indexer = ?" (SQL.Only name)
  case list of
    [(slotNo, blockHeaderHash)] -> return $ Just $ C.ChainPoint slotNo blockHeaderHash
    []                          -> return Nothing
    _                           -> error "getIndexerBookmark: this should never happen!!"


findIntervalToBeIndexed :: Interval -> SQL.Connection -> String -> IO Interval
findIntervalToBeIndexed cliInterval sqlCon name = do
  maybe notFound found <$> getIndexerBookmarkSqlite sqlCon name
  where
    notFound = cliInterval
    found bookmark = if chainPointLaterThanFrom bookmark cliInterval
      then cliInterval { from = bookmark }
      else cliInterval

-- ** Database path and table(s)

data DbPathAndTableName = DbPathAndTableName FilePath (Maybe String)
  deriving (Show)

defaultTableName :: String -> DbPathAndTableName -> (FilePath, String)
defaultTableName defaultName (DbPathAndTableName dbPath maybeName) = (dbPath, fromMaybe defaultName maybeName)

-- * Streaming

-- | Consume a stream @source@ in a loop and run effect @f@ on it.
loopM :: (MonadTrans t1, Monad m, Monad (t1 m)) => S.Stream (S.Of t2) m b -> (t2 -> t1 m a) -> t1 m b
loopM source f = loop source
  where
    loop source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (event, source'') -> do
        _ <- f event
        loop source''

-- | Helper to create loops
streamFold :: Monad m => (a -> b -> m b) -> b -> S.Stream (S.Of a) m r -> S.Stream (S.Of a) m r
streamFold f acc_ source_ = loop acc_ source_
  where
    loop acc source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        acc' <- lift $ f e acc
        S.yield e
        loop acc' source'
