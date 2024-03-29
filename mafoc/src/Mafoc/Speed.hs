module Mafoc.Speed where

import Control.Concurrent (MVar, modifyMVar_, newMVar)
import Control.Concurrent.Async qualified as IO
import Control.Concurrent.QSemN (signalQSemN)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan (dupTChan, readTChan)

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS
import Mafoc.Core qualified as Mafoc
import Marconi.ChainIndex.Indexers qualified as Marconi
import Marconi.Core.Storable qualified as RI

-- * Measuer local chainsync speed

data BlockSource
  = Callback
      { callbackOptionsSocketPath :: Mafoc.SocketPath
      , callbackOptionsNodeConfigPath :: Mafoc.NodeConfig
      , callbackOptionsStart :: Maybe C.ChainPoint
      , callbackOptionsEnd :: Maybe C.SlotNo
      }
  | CallbackPipelined
      { callbackPipelinedOptionsSocketPath :: Mafoc.SocketPath
      , callbackPipelinedOptionsNodeConfigPath :: Mafoc.NodeConfig
      , callbackPipelinedOptionsStart :: Maybe C.ChainPoint
      , callbackPipelinedOptionsEnd :: Maybe C.SlotNo
      , callbackPipelinedPipelineSize :: Word32
      }
  | RewindableIndex
      { rewindableIndexOptionsSocketPath :: Mafoc.SocketPath
      , rewindableIndexOptionsStart :: Maybe C.ChainPoint
      , rewindableIndexOptionsEnd :: Maybe C.SlotNo
      , rewindableIndexNetworkId :: C.NetworkId
      }
  deriving (Show)

mkCallback
  :: (C.LocalNodeConnectInfo C.CardanoMode -> C.ChainPoint -> (CS.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ()) -> IO b)
  -> Mafoc.SocketPath
  -> Mafoc.NodeConfig
  -> Maybe C.ChainPoint
  -> Maybe C.SlotNo
  -> IO b
mkCallback f socketPath nodeConfig cpFromCli maybeEnd = do
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory (coerce nodeConfig)
  let from = fromMaybe C.ChainPointAtGenesis cpFromCli
      io g = f (CS.mkConnectInfo env $ coerce socketPath) from $ \case
        CS.RollForward bim _ct -> g bim
        CS.RollBackward{} -> pure ()

  case maybeEnd of
    Just end -> io $ \bim -> printAndDieWhenEnd end bim
    _ -> do
      putStrLn "No end"
      io $ \bim ->
        let slotNo = CS.bimSlotNo bim
            w = coerce slotNo :: Word64
         in when (w `mod` 10000 == 0) $ print slotNo

-- * Rewindable index

data NoopHandler = NoopHandler
data instance RI.StorableEvent NoopHandler = NoopEvent C.ChainPoint
type instance RI.StorableMonad NoopHandler = IO
type instance RI.StorablePoint NoopHandler = C.ChainPoint
instance RI.HasPoint (RI.StorableEvent NoopHandler) C.ChainPoint where getPoint (NoopEvent cp) = cp
instance RI.Buffered NoopHandler where
  persistToStorage _ h = pure h
  getStoredEvents _h = pure $ [NoopEvent C.ChainPointAtGenesis]
instance RI.Rewindable NoopHandler where
  rewindStorage _ h = pure h
type NoopIndexer = RI.State NoopHandler

rewindableIndex :: Mafoc.SocketPath -> Maybe C.ChainPoint -> Maybe C.SlotNo -> C.NetworkId -> IO ()
rewindableIndex socketPath cpFromCli maybeEnd networkId = do
  coordinator <- Marconi.initialCoordinator 1 2160
  workerChannel <- atomically . dupTChan $ Marconi._channel coordinator
  indexer :: NoopIndexer <- RI.emptyState 10 NoopHandler
  mIndexer <- newMVar indexer
  case maybeEnd of
    Just end -> do
      let loop :: MVar NoopIndexer -> IO ()
          loop index = do
            signalQSemN (Marconi._barrier coordinator) 1
            event <- atomically $ readTChan workerChannel
            case event of
              CS.RollForward (bim, _, _) _ -> do
                modifyMVar_ index (RI.insert $ NoopEvent $ Mafoc.blockChainPoint bim)
                printAndDieWhenEnd end bim
                loop index
              CS.RollBackward cp _ct -> do
                modifyMVar_ index $ \ix -> RI.rewind cp ix
                loop index

      void $ IO.withAsync (loop mIndexer) $ \a -> do
        IO.link a
        CS.withChainSyncEventEpochNoStream
          (coerce socketPath)
          networkId
          [fromMaybe C.ChainPointAtGenesis cpFromCli]
          (Marconi.mkIndexerStream coordinator)
    _ -> putStrLn "Must specify final slot!"

-- * Exit loops with exception

printAndDieWhenEnd :: C.SlotNo -> C.BlockInMode C.CardanoMode -> IO ()
printAndDieWhenEnd end bim =
  let slotNo = CS.bimSlotNo bim
      w = coerce slotNo :: Word64
   in do
        when (w `mod` 10000 == 0) $ print slotNo
        when (slotNo >= end) $ do
          putStrLn $ "Reached the requested end slot: " <> show end
          undefined
