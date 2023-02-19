{-# LANGUAGE AllowAmbiguousTypes    #-}
{-# LANGUAGE LambdaCase             #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE TypeFamilyDependencies #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Core where

import Control.Monad.Trans.Class (MonadTrans, lift)
import Data.Function ((&))
import Data.Maybe (fromMaybe)
import Data.Text qualified as TS
import Data.Word (Word32)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.BM.Data.Trace (Trace)
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Mafoc.RollbackRingBuffer qualified as RB
import Marconi.ChainIndex.Indexers.MintBurn ()
import Marconi.ChainIndex.Logging qualified as Marconi

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

takeUpTo
  :: UpTo
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (cp, C.ChainTip))) IO ()
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (cp, C.ChainTip))) IO ()
takeUpTo upTo' source = case upTo' of
  SlotNo slotNo -> flip S.takeWhile source $ \case
    RB.RollForward blk _ -> blockSlotNo blk <= slotNo
    _                    -> True
  Infinity -> source
  CurrentTip -> S.lift (S.next source) >>= \case
    Left r -> return r
    Right (event, source') -> do
      lift $ putStrLn $ "Indexing up to current tip, which is: " <> show (getTipPoint event) -- TODO replace me with a better logger
      S.yield event -- We can always yield the current event, as that
                    -- is the source for the upper bound anyway.
      flip S.takeWhile source' $ \case
        RB.RollForward blk _ -> blockChainPoint blk <= getTipPoint event
        _                    -> True

  where
    getTipPoint :: RB.Event a (b, C.ChainTip) -> C.ChainPoint
    getTipPoint = \case
      RB.RollForward _ (_, ct) -> C.chainTipToChainPoint ct
      RB.RollBackward (_, ct)  -> C.chainTipToChainPoint ct

-- * Additions to cardano-api

getSecurityParam :: FilePath -> IO Natural
getSecurityParam nodeConfig = do
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure $ fromIntegral $ C.envSecurityParam env

-- | Convert event from @ChainSyncEvent@ to @Event@.
fromChainSyncEvent
  :: Monad m
  => S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode mode))) m r
  -> S.Stream (S.Of (RB.Event (C.BlockInMode mode) (C.ChainPoint, C.ChainTip))) m r
fromChainSyncEvent = S.map $ \e -> case e of
  CS.RollForward a ct   -> RB.RollForward a (blockChainPoint a, ct)
  CS.RollBackward cp ct -> RB.RollBackward (cp, ct)

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

sqliteInitBookmarks :: SQL.Connection -> IO ()
sqliteInitBookmarks c = do
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
streamFold :: Monad m => (a -> b -> m (b, c)) -> b -> S.Stream (S.Of a) m r -> S.Stream (S.Of c) m r
streamFold f acc_ source_ = loop acc_ source_
  where
    loop acc source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        (acc', e') <- lift $ f e acc
        S.yield e'
        loop acc' source'

-- * Indexer class

class Indexer a where

  -- | The @a@ itself doubles as cli configuration, no need for the following:
  -- type Config a = r | r -> a

  -- | Runtime configuration.
  data Runtime a

  -- | Event type.
  type Event a

  -- | The fold state. For map type indexers where no fold state needs
  -- to be maintained, the state is some form of empty (i.e defined to
  -- a data type with no fields).
  data State a

  -- | Fold a block into an event and produce a new state.
  toEvent :: C.BlockInMode C.CardanoMode -> State a -> Maybe (State a, Event a)

  -- | Initialize an indexer and return its runtime configuration. E.g
  -- open the destination to where data is persisted, etc.
  initialize :: a -> IO (State a, BlockSourceConfig, Runtime a)

  -- | Write event to persistent storage.
  persist :: Runtime a -> Event a -> IO ()

-- | Static configuration for block source
data BlockSourceConfig = BlockSourceConfig
  { localNodeConnection :: C.LocalNodeConnectInfo C.CardanoMode
  , interval            :: Interval
  , securityParam       :: Natural
  , logging             :: Bool
  , pipelineSize        :: Word32
  }

blockSource :: BlockSourceConfig -> Trace IO TS.Text -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
blockSource cc trace = blocks' (localNodeConnection cc) from'
  & (if logging cc then Marconi.logging trace else id)
  & fromChainSyncEvent
  & takeUpTo upTo'
  & S.drop 1 -- The very first event from local chainsync is always a
             -- rewind. We skip this because we don't have anywhere to
             -- rollback to anyway.
  & RB.rollbackRingBuffer (securityParam cc)
  where
    Interval from' upTo' = interval cc
    blocks'
      :: C.LocalNodeConnectInfo C.CardanoMode
      -> C.ChainPoint
      -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
    blocks' = let
      pipelineSize' = pipelineSize cc
      in if pipelineSize' > 1
      then CS.blocks
      else CS.blocksPipelined pipelineSize'

runIndexer :: forall a . Indexer a => a -> IO ()
runIndexer cli = do
  (initialState, cc, runtimeConfig) <- initialize cli
  let
    f :: C.BlockInMode C.CardanoMode -> State a -> IO (State a, Maybe (Event a))
    f blk s = return $ maybe (s, Nothing) (\(s', e') -> (s', Just e')) $ toEvent blk s

  c <- defaultConfigStdout
  withTrace c "mafoc" $ \trace -> do
    S.effects
      $ (blockSource cc trace :: S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ())
      & streamFold f initialState
      & S.mapMaybe id
      & S.chain (persist runtimeConfig)
