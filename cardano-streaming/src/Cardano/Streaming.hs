{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Cardano.Streaming (
  withChainSyncEventStream,
  withChainSyncEventEpochNoStream,
  CS.ChainSyncEvent (..),
  CS.ChainSyncEventException (..),
  --
  CS.mkConnectInfo,
  CS.mkLocalNodeConnectInfo

  -- * Stream blocks and ledger states
  , blocks, blocksPrim
  , blocksPipelined, blocksPipelinedPrim
  , ledgerStates
  , ledgerStatesPipelined
  , foldLedgerState
  , foldLedgerStateEvents
  , getEnvAndInitialLedgerStateHistory
  , CS.ignoreRollbacks
  , applyBlockThrow
  , getLastLedgerState
  )
where

import Cardano.Api qualified as C
import Cardano.Api.ChainSync.Client (
  ClientStIdle (SendMsgFindIntersect, SendMsgRequestNext),
  ClientStIntersect (ClientStIntersect, recvMsgIntersectFound, recvMsgIntersectNotFound),
  ClientStNext (ClientStNext, recvMsgRollBackward, recvMsgRollForward),
 )
import Cardano.Slotting.Slot (WithOrigin (At, Origin))
import Cardano.Slotting.Time qualified as C
import Cardano.Streaming.Callbacks qualified as CS
import Cardano.Streaming.Helpers qualified as CS
import Control.Concurrent (readMVar)
import Control.Concurrent qualified as IO
import Control.Concurrent.Async (ExceptionInLinkedThread (ExceptionInLinkedThread), link, withAsync)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException (SomeException), catch, throw)
import Control.Exception qualified as IO
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (forM_)
import Data.Function ((&))
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Time.Clock.POSIX (POSIXTime)
import Data.Time.Clock.POSIX qualified as Time
import Data.Word (Word32)
import Streaming (Of, Stream)
import Streaming.Prelude qualified as S

{- | `withChainSyncEventStream` uses the chain-sync mini-protocol to
 connect to a locally running node and fetch blocks from the given
 starting point.
-}
withChainSyncEventStream
  :: FilePath
  -- ^ Path to the node socket
  -> C.NetworkId
  -> [C.ChainPoint]
  -- ^ The point on the chain to start streaming from
  -> (Stream (Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r -> IO b)
  -- ^ The stream consumer
  -> IO b
withChainSyncEventStream socketPath networkId points consumer =
  do
    -- The chain-sync client runs in a different thread passing the blocks it
    -- receives to the stream consumer through a MVar. The chain-sync client
    -- thread and the stream consumer will each block on each other and stay
    -- in lockstep.
    --
    -- NOTE: choosing a MVar is a tradeoff towards simplicity. In this case a
    -- (bounded) queue could perform better. Indeed a properly-sized buffer
    -- can reduce the time the two threads are blocked waiting for each
    -- other. The problem here is "properly-sized". A bounded queue like
    -- Control.Concurrent.STM.TBQueue allows us to specify a max queue length
    -- but block size can vary a lot (TODO quantify this) depending on the
    -- era. We have an alternative implementation with customizable queue
    -- size (TBMQueue) but it needs to be extracted from the
    -- plutus-chain-index-core package. Using a simple MVar doesn't seem to
    -- slow down marconi's indexing, likely because the difference is
    -- negligeable compared to existing network and IO latencies.  Therefore,
    -- let's stick with a MVar now and revisit later.
    nextChainSyncEventVar <- newEmptyMVar

    let client = chainSyncStreamingClient points nextChainSyncEventVar

        localNodeConnectInfo :: C.LocalNodeConnectInfo C.CardanoMode
        localNodeConnectInfo = CS.mkLocalNodeConnectInfo networkId socketPath

    withAsync (connectToLocalNodeWithChainSyncClient localNodeConnectInfo client) $ \a -> do
      -- Make sure all exceptions in the client thread are passed to the consumer thread
      link a
      -- Run the consumer
      consumer $ S.repeatM $ takeMVar nextChainSyncEventVar
    -- Let's rethrow exceptions from the client thread unwrapped, so that the
    -- consumer does not have to know anything about async
    `catch` \(ExceptionInLinkedThread _ (SomeException e)) -> throw e

{- | `withChainSyncEventEpochNoStream` uses the chain-sync mini-protocol to
 connect to a locally running node and fetch blocks from the given
 starting point, along with their @EpochNo@.
-}
withChainSyncEventEpochNoStream
  :: FilePath
  -- ^ Path to the node socket
  -> C.NetworkId
  -> [C.ChainPoint]
  -- ^ The point on the chain to start streaming from
  -> (Stream (Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode, C.EpochNo, POSIXTime))) IO r -> IO b)
  -- ^ The stream consumer
  -> IO b
withChainSyncEventEpochNoStream socketPath networkId points consumer =
  do
    -- The chain-sync client runs in a different thread passing the blocks it
    -- receives to the stream consumer through a MVar. The chain-sync client
    -- thread and the stream consumer will each block on each other and stay
    -- in lockstep.
    --
    -- NOTE: choosing a MVar is a tradeoff towards simplicity. In this case a
    -- (bounded) queue could perform better. Indeed a properly-sized buffer
    -- can reduce the time the two threads are blocked waiting for each
    -- other. The problem here is "properly-sized". A bounded queue like
    -- Control.Concurrent.STM.TBQueue allows us to specify a max queue length
    -- but block size can vary a lot (TODO quantify this) depending on the
    -- era. We have an alternative implementation with customizable queue
    -- size (TBMQueue) but it needs to be extracted from the
    -- plutus-chain-index-core package. Using a simple MVar doesn't seem to
    -- slow down marconi's indexing, likely because the difference is
    -- negligeable compared to existing network and IO latencies.  Therefore,
    -- let's stick with a MVar now and revisit later.
    nextChainSyncEventVar <- newEmptyMVar
    eraHistoryVar <- newEmptyMVar

    let localNodeConnectInfo :: C.LocalNodeConnectInfo C.CardanoMode
        localNodeConnectInfo = CS.mkLocalNodeConnectInfo networkId socketPath

    systemStart <-
      C.queryNodeLocalState localNodeConnectInfo Nothing C.QuerySystemStart
        >>= \case
          Left err -> fail $ show err
          Right systemStart -> pure systemStart

    let queryHistoryInMode :: C.QueryInMode C.CardanoMode (C.EraHistory C.CardanoMode)
        queryHistoryInMode = C.QueryEraHistory C.CardanoModeIsMultiEra

        askHistory :: IO ()
        askHistory = do
          res <- C.queryNodeLocalState localNodeConnectInfo Nothing queryHistoryInMode
          case res of
            Left err -> fail $ show err
            Right h -> putMVar eraHistoryVar h

        getHistory = readMVar eraHistoryVar

        attachEpochAndTime
          :: CS.ChainSyncEvent (C.BlockInMode C.CardanoMode)
          -> IO (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode, C.EpochNo, POSIXTime))
        attachEpochAndTime (CS.RollBackward cp ct) = pure $ CS.RollBackward cp ct
        attachEpochAndTime evt@(CS.RollForward (C.BlockInMode (C.Block (C.BlockHeader sn _ _) _) _) _) = do
          history <- getHistory
          let epochAndTime = do
                (epoch, _, _) <- C.slotToEpoch sn history
                (relativeTime, _) <- C.getProgress sn history
                pure (epoch, Time.utcTimeToPOSIXSeconds $ C.fromRelativeTime systemStart relativeTime)
          case epochAndTime of
            Left _ -> askHistory *> attachEpochAndTime evt
            Right (epoch, time) -> pure $ (,epoch,time) <$> evt

        client = chainSyncStreamingClient points nextChainSyncEventVar

    askHistory

    withAsync (connectToLocalNodeWithChainSyncClient localNodeConnectInfo client) $ \a -> do
      -- Make sure all exceptions in the client thread are passed to the consumer thread
      link a
      -- Run the consumer
      consumer $ S.repeatM $ takeMVar nextChainSyncEventVar >>= attachEpochAndTime
    -- Let's rethrow exceptions from the client thread unwrapped, so that the
    -- consumer does not have to know anything about async
    `catch` \(ExceptionInLinkedThread _ (SomeException e)) -> throw e

connectToLocalNodeWithChainSyncClient
  :: C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainSyncClient (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()
  -> IO ()
connectToLocalNodeWithChainSyncClient connectInfo client =
  let
    localNodeSyncClientProtocols =
      C.LocalNodeClientProtocols
        { C.localChainSyncClient = C.LocalChainSyncClient client
        , C.localStateQueryClient = Nothing
        , C.localTxMonitoringClient = Nothing
        , C.localTxSubmissionClient = Nothing
        }
   in
    C.connectToLocalNode connectInfo localNodeSyncClientProtocols

{- | `chainSyncStreamingClient` is the client that connects to a local node
 and runs the chain-sync mini-protocol. This client is fire-and-forget
 and does not require any control.

 If the starting point is such that an intersection cannot be found, this
 client will throw a NoIntersectionFound exception.
-}
chainSyncStreamingClient
  :: [C.ChainPoint]
  -> MVar (CS.ChainSyncEvent e)
  -> C.ChainSyncClient e C.ChainPoint C.ChainTip IO ()
chainSyncStreamingClient points nextChainEventVar =
  C.ChainSyncClient $ pure $ SendMsgFindIntersect points onIntersect
  where
    onIntersect =
      ClientStIntersect
        { recvMsgIntersectFound = \cp ct ->
            C.ChainSyncClient $ do
              putMVar nextChainEventVar (CS.RollBackward cp ct)
              sendRequestNext
        , recvMsgIntersectNotFound =
            -- There is nothing we can do here
            throw CS.NoIntersectionFound
        }

    sendRequestNext =
      pure $ SendMsgRequestNext onNext (pure onNext)
      where
        onNext =
          ClientStNext
            { recvMsgRollForward = \bim ct ->
                C.ChainSyncClient $ do
                  putMVar nextChainEventVar (CS.RollForward bim ct)
                  sendRequestNext
            , recvMsgRollBackward = \cp ct ->
                C.ChainSyncClient $ do
                  putMVar nextChainEventVar (CS.RollBackward cp ct)
                  sendRequestNext
            }

{- | Create stream of @ChainSyncEvent (BlockInMode CardanoMode)@ from
 a node at @socketPath@ with @networkId@ starting at @point@.
-}
blocks
  :: C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
blocks lnc cp = blocksPrim lnc cp IO.newChan IO.writeChan IO.readChan

-- | Create stream of @ChainSyncEvent (BlockInMode CardanoMode)@ from
-- connection @con@ starting at @point@.
--
-- Parametrise over creating, writing to, and reading from a
-- concurrent variable.
blocksPrim
  :: forall chan r
   . C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> IO chan
  -> (chan -> CS.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ())
  -> (chan -> IO (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode)))
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
blocksPrim lnc chainPoint init_ write read_ = do
  chan <- liftIO init_
  void $ liftIO $ CS.linkedAsync $ CS.blocksCallback lnc chainPoint $ write chan
  S.repeatM $ read_ chan

blocksPipelined
  :: Word32
  -> C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
blocksPipelined pipelineSize lnc cp = blocksPipelinedPrim pipelineSize lnc cp IO.newChan IO.writeChan IO.readChan

blocksPipelinedPrim
  :: Word32
  -> C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> IO chan
  -> (chan -> CS.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ())
  -> (chan -> IO (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode)))
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
blocksPipelinedPrim pipelineSize lnc chainPoint init_ write read_ = do
  chan <- liftIO init_
  void $ liftIO $ CS.linkedAsync $ CS.blocksCallbackPipelined pipelineSize lnc chainPoint $ write chan
  S.repeatM $ read_ chan

-- * Ledger states

-- | Get a stream of permanent ledger states
ledgerStates :: FilePath -> FilePath -> C.ValidationMode -> S.Stream (S.Of C.LedgerState) IO r
ledgerStates config socket validationMode = do
  (env, initialLedgerStateHistory) <- liftIO $ getEnvAndInitialLedgerStateHistory config
  blocks (CS.mkConnectInfo env socket) C.ChainPointAtGenesis
    & foldLedgerState env initialLedgerStateHistory validationMode

-- | Get a stream of ledger states over a pipelined chain sync
ledgerStatesPipelined
  :: Word32 -> FilePath -> FilePath -> C.ValidationMode -> S.Stream (S.Of C.LedgerState) IO r
ledgerStatesPipelined pipelineSize config socket validationMode = do
  (env, initialLedgerStateHistory) <- liftIO $ getEnvAndInitialLedgerStateHistory config
  blocksPipelined pipelineSize (CS.mkConnectInfo env socket) C.ChainPointAtGenesis
    & foldLedgerState env initialLedgerStateHistory validationMode

-- * Apply block

{- | Fold a stream of blocks into a stream of ledger states. This is
 implemented in a similar way as `foldBlocks` in
 cardano-api:Cardano.Api.LedgerState, the difference being that this
 keeps waiting for more blocks when chainsync server and client are
 fully synchronized.
-}
foldLedgerState
  :: C.Env
  -> LedgerStateHistory
  -> C.ValidationMode
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  -> S.Stream (S.Of C.LedgerState) IO r
foldLedgerState env initialLedgerStateHistory validationMode =
  S.map (fst . snd) . foldLedgerStateEvents env initialLedgerStateHistory validationMode

-- | Like `foldLedgerState`, but also produces blocks and `C.LedgerEvent`s.
foldLedgerStateEvents
  :: C.Env
  -> LedgerStateHistory
  -> C.ValidationMode
  -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode, LedgerStateEvents)) IO r
foldLedgerStateEvents env initialLedgerStateHistory validationMode = loop initialLedgerStateHistory
  where
    applyBlock_ :: C.LedgerState -> C.Block era -> IO (C.LedgerState, [C.LedgerEvent])
    applyBlock_ ledgerState = applyBlockThrow env ledgerState validationMode

    loop
      :: LedgerStateHistory
      -> S.Stream (S.Of (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
      -> S.Stream (S.Of (C.BlockInMode C.CardanoMode, LedgerStateEvents)) IO r
    loop ledgerStateHistory source =
      lift (S.next source) >>= \case
        Left r -> pure r
        Right (chainSyncEvent, source') -> do
          ledgerStateHistory' <- case chainSyncEvent of
            CS.RollForward blockInMode@(C.BlockInMode block _) _ct -> do
              newLedgerState <- liftIO $ applyBlock_ (getLastLedgerState ledgerStateHistory) block
              let (ledgerStateHistory', committedStates) = pushLedgerState env ledgerStateHistory (CS.bimSlotNo blockInMode) newLedgerState blockInMode
              forM_ committedStates $ \(_, (ledgerState, ledgerEvents), currBlockMay) -> case currBlockMay of
                Origin -> return ()
                At currBlock -> S.yield (currBlock, (ledgerState, ledgerEvents))
              pure ledgerStateHistory'
            CS.RollBackward cp _ct -> pure $ case cp of
              C.ChainPointAtGenesis -> initialLedgerStateHistory
              C.ChainPoint slotNo _ -> rollBackLedgerStateHist ledgerStateHistory slotNo

          loop ledgerStateHistory' source'

getEnvAndInitialLedgerStateHistory :: FilePath -> IO (C.Env, LedgerStateHistory)
getEnvAndInitialLedgerStateHistory configPath = do
  (env, initialLedgerState) <-
    either IO.throw pure =<< (runExceptT $ C.initialLedgerState $ C.File configPath)
  let initialLedgerStateHistory = singletonLedgerStateHistory initialLedgerState
  return (env, initialLedgerStateHistory)

applyBlockThrow
  :: C.Env
  -> C.LedgerState
  -> C.ValidationMode
  -> C.Block era
  -> IO (C.LedgerState, [C.LedgerEvent])
applyBlockThrow env ledgerState validationMode block = case C.applyBlock env ledgerState validationMode block of
  Left err -> IO.throw err
  Right ls -> pure ls

-- * Copy-paste code

--
-- The following is pasted in from cardano-api:Cardano.Api.LedgerState.
-- (`getLastLedgerState` and `singletonLedgerStateHistory` aren't a
-- direct copy-paste, but they are extracted from within `foldBlocks`)

{- | A history of k (security parameter) recent ledger states. The head is the
 most recent item. Elements are:

 * Slot number that a new block occurred
 * The ledger state and events after applying the new block
 * The new block
-}
type LedgerStateHistory = History LedgerStateEvents

type History a = Seq (C.SlotNo, a, WithOrigin (C.BlockInMode C.CardanoMode))

type LedgerStateEvents = (C.LedgerState, [C.LedgerEvent])

-- | Add a new ledger state to the history
pushLedgerState
  :: C.Env
  -- ^ Environment used to get the security param, k.
  -> History a
  -- ^ History of k items.
  -> C.SlotNo
  -- ^ Slot number of the new item.
  -> a
  -- ^ New item to add to the history
  -> C.BlockInMode C.CardanoMode
  -- ^ The block that (when applied to the previous
  -- item) resulted in the new item.
  -> (History a, History a)
  -- ^ ( The new history with the new item appended
  --   , Any existing items that are now past the security parameter
  --      and hence can no longer be rolled back.
  --   )
pushLedgerState env hist ix st block =
  Seq.splitAt
    (fromIntegral $ C.envSecurityParam env + 1)
    ((ix, st, At block) Seq.:<| hist)

rollBackLedgerStateHist :: History a -> C.SlotNo -> History a
rollBackLedgerStateHist hist maxInc = Seq.dropWhileL ((> maxInc) . (\(x, _, _) -> x)) hist

getLastLedgerState :: LedgerStateHistory -> C.LedgerState
getLastLedgerState ledgerStates' =
  maybe
    (error "Impossible! Missing Ledger state")
    (\(_, (ledgerState, _), _) -> ledgerState)
    (Seq.lookup 0 ledgerStates')

singletonLedgerStateHistory :: C.LedgerState -> LedgerStateHistory
singletonLedgerStateHistory ledgerState = Seq.singleton (0, (ledgerState, []), Origin)
