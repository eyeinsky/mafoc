{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedLabels  #-}
{-# LANGUAGE OverloadedStrings #-}

module Mafoc.Cmds.FoldLedgerState where

import Data.Function ((&))
import Data.Word (Word32)
import Options.Applicative qualified as Opt
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Streaming qualified as CS
import Mafoc.CLI qualified as Opt
import Mafoc.Core (ConcurrencyPrimitive, NodeConfig (NodeConfig), NodeInfo,
                   SocketPath (SocketPath), StopSignal, UpTo (SlotNo), blockChainPoint, blockSource,
                   initializeLedgerState, storeLedgerState, takeUpTo)
import Mafoc.Upstream (querySecurityParam)
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data FoldLedgerState = FoldLedgerState
  { maybeFromLedgerState :: Maybe FilePath
  , toSlotNo             :: C.SlotNo
  , nodeInfo             :: NodeInfo NodeConfig
  , logging              :: Bool
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
      <*> Opt.commonPipelineSize
      <*> Opt.commonConcurrencyPrimitive

run :: FoldLedgerState -> StopSignal -> IO ()
run config stopSignal = do
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
        ((slotNo', bhh), ledgerCfg, extLedgerState) <- initializeLedgerState nodeConfig path
        return (C.ChainPoint slotNo' bhh, ledgerCfg, extLedgerState)
      Nothing -> do
        (ledgerCfg, extLedgerState) <- Marconi.getInitialExtLedgerState nodeConfig'
        return (C.ChainPointAtGenesis, ledgerCfg, extLedgerState)

    let SocketPath socketPath' = #socketPath nodeInfo'
    networkId <- #getNetworkId nodeInfo'
    let lnc = CS.mkLocalNodeConnectInfo networkId socketPath'
    securityParam <- querySecurityParam lnc
    let takeUpTo' = takeUpTo trace (SlotNo $ toSlotNo config) stopSignal
        blockSource' = blockSource securityParam lnc (pipelineSize config)
          (concurrencyPrimitive config) (logging config) trace fromCp takeUpTo'

    maybeLast <- S.last_
      $ blockSource'
      & foldLedgerState ledgerCfg extLedgerState

    case maybeLast of
      Just (cp', extLedgerState') -> case cp' of
        C.ChainPoint slotNo' bhh -> do
          storeLedgerState ledgerCfg (slotNo', bhh) extLedgerState'
        C.ChainPointAtGenesis -> putStrLn "No reason to store ledger state for genesis"
      Nothing -> putStrLn "Empty stream"

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
