{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedLabels  #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Cmds.FoldLedgerState where

import Options.Applicative qualified as Opt
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Mafoc.CLI qualified as Opt
import Mafoc.Core (ConcurrencyPrimitive, NodeConfig (NodeConfig), NodeInfo,
                   SocketPath (SocketPath), UpTo (SlotNo), blockChainPoint,
                   takeUpTo, blockProducer, rollbackRingBuffer)
import Mafoc.Upstream (querySecurityParam)
import Mafoc.LedgerState qualified as LedgerState
import Mafoc.Signal qualified as Signal
import Mafoc.StateFile qualified as StateFile
import Mafoc.Exceptions qualified as E
import Mafoc.Logging qualified as Logging
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data FoldLedgerState = FoldLedgerState
  { maybeFromLedgerState :: Maybe FilePath
  , toSlotNo             :: C.SlotNo
  , nodeInfo             :: NodeInfo NodeConfig
  , logging              :: Bool
  , profiling            :: Maybe Logging.ProfilingConfig
  , pipelineSize         :: Word32
  , concurrencyPrimitive :: Maybe ConcurrencyPrimitive
  } deriving Show

parseCli :: Opt.ParserInfo FoldLedgerState
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "fold-ledgerstate"
  <> Opt.header "fold-ledgerstate - Fold ledger state up to some slot and serialise it"
  where
    cli :: Opt.Parser FoldLedgerState
    cli = FoldLedgerState
      <$> Opt.optional (Opt.strOption (Opt.opt 'f' "from" "Path to serialised ledger state."))
      <*> Opt.commonUntilSlot
      <*> Opt.commonNodeConnectionAndConfig
      <*> Opt.commonLogging
      <*> Opt.commonProfilingConfig
      <*> Opt.commonPipelineSize
      <*> Opt.commonConcurrencyPrimitive

run :: FoldLedgerState -> Signal.Stop -> Signal.ChainsyncStats -> IO ()
run config stopSignal statsSignal = do
  c <- defaultConfigStdout
  withTrace c "mafoc" $ \trace -> do
    -- Get initial ledger state. This is at genesis when
    -- maybeFromLedgerState is Nothing, otherwise get slot
    -- number and block header hash from the filename and the rest
    -- from the file content.
    let nodeInfo' = nodeInfo config
    let nodeConfig@(NodeConfig nodeConfig') = #nodeConfig nodeInfo'
    (fromCp, ledgerCfg, extLedgerState) <- case maybeFromLedgerState config of
      Just path -> do
        (slotNo, bhh) <- StateFile.bhhFromFileName path & \case
           Left errMsg -> E.throwIO $ E.Can't_parse_chain_point_from_LedgerState_file_name path errMsg
           Right slotNoBhh' -> return slotNoBhh'
        (ledgerCfg, extLedgerState) <- LedgerState.load nodeConfig path
        return (C.ChainPoint slotNo bhh, ledgerCfg, extLedgerState)

      Nothing -> do
        (ledgerCfg, extLedgerState) <- Marconi.getInitialExtLedgerState nodeConfig'
        return (C.ChainPointAtGenesis, ledgerCfg, extLedgerState)

    let SocketPath socketPath' = #socketPath nodeInfo'
    networkId <- #getNetworkId nodeInfo'
    let lnc = CS.mkLocalNodeConnectInfo networkId socketPath'
    securityParam <- querySecurityParam lnc

    maybeProfilingConfig <- traverse (Logging.profilerInit trace (show config)) (profiling config)

    maybeLast <- S.last_ $
      blockProducer lnc (pipelineSize config) fromCp trace (concurrencyPrimitive config)
        & (if logging config then Logging.logging trace statsSignal else id)
        & maybe id Logging.profileStep maybeProfilingConfig
        & S.drop 1 -- The very first event from local chainsync is always a
                   -- rewind. We skip this because we don't have anywhere to
                   -- rollback to anyway.
        & takeUpTo trace (SlotNo $ toSlotNo config) stopSignal
        & rollbackRingBuffer securityParam
        & foldLedgerState ledgerCfg extLedgerState

    maybeCp <- case maybeLast of
      Just (cp', extLedgerState') -> Just cp' <$ case cp' of
        C.ChainPoint slotNo' bhh -> do
          let slotNoBhh = (slotNo', bhh)
          LedgerState.store (StateFile.toName "ledgerState" slotNoBhh) ledgerCfg extLedgerState'
        C.ChainPointAtGenesis -> putStrLn "No reason to store ledger state for genesis"
      Nothing -> putStrLn "Empty stream" $> Nothing

    case maybeProfilingConfig of
      Just profiling -> Logging.profilerEnd profiling maybeCp
      Nothing -> return ()

-- | Create a stream of ledger states from a stream of
-- blocks.
--
-- The caller is responsible for whether the initial ledger state
-- matches the block stream. (I.e the fact that the initial ledger was
-- achieved by applying a sequence of blocks just prior to block
-- stream given as argument.)
foldLedgerState
  :: Marconi.ExtLedgerCfg_
  -> Marconi.ExtLedgerState_
  -> S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO ()
  -> S.Stream (S.Of (C.ChainPoint, Marconi.ExtLedgerState_)) IO ()
foldLedgerState ledgerCfg initialExtLedgerState = loop initialExtLedgerState
  where
    loop extLedgerState source = S.lift (S.next source) >>= \case
      Left r -> pure r
      Right (blockInMode, source') -> let
        extLedgerState' = Marconi.applyBlock ledgerCfg extLedgerState blockInMode
        in do
        S.yield (blockChainPoint blockInMode, extLedgerState')
        loop extLedgerState' source'
