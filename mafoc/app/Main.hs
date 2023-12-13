module Main where

import Control.Exception qualified as IO
import Options.Applicative qualified as Opt
import Data.Text qualified as TS
import Network.Wai.Handler.Warp qualified as Warp
import Servant.Server qualified as Servant

import Cardano.Api qualified as C
import Cardano.Streaming.Callbacks qualified as CS
import Cardano.BM.Data.Severity qualified as CM

import Mafoc.CLI qualified as Opt
import Mafoc.Cmds.FoldLedgerState qualified as FoldLedgerState
import Mafoc.Cmds.SlotNoChainPoint qualified as SlotNoChainPoint
import Mafoc.Core (BatchSize, Indexer (description, parseCli), runIndexer, API, server, CommonConfig(CommonConfig, batchSize, stopSignal, checkpointSignal, statsSignal, severity, checkpointInterval), CheckpointInterval, runIndexerParallel, printChainIntervals, StatelessIndexer, ParallelismConfig)
import Mafoc.Exceptions qualified as E
import Mafoc.Indexers.AddressAppears qualified as AddressAppears
import Mafoc.Indexers.AddressBalance qualified as AddressBalance
import Mafoc.Indexers.AddressDatum qualified as AddressDatum
import Mafoc.Indexers.BlockBasics qualified as BlockBasics
import Mafoc.Indexers.Datum qualified as Datum
import Mafoc.Indexers.Deposit qualified as Deposit
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Indexers.Fingerprint qualified as Fingerprint
import Mafoc.Indexers.Mamba qualified as Mamba
import Mafoc.Indexers.MintBurn qualified as MintBurn
import Mafoc.Indexers.NoOp qualified as NoOp
import Mafoc.Indexers.ScriptTx qualified as ScriptTx
import Mafoc.Indexers.Utxo qualified as Utxo
import Mafoc.Signal qualified as Signal
import Mafoc.Speed qualified as Speed

import Mafoc.Questions.IntraBlockSpends qualified as IntraBlockSpends

main :: IO ()
main = do
  stopSignal <- Signal.setupCtrlCHandler 3
  checkpointSignal <- Signal.setupCheckpointSignal
  statsSignal <- Signal.setupChainsyncStatsSignal
  printRollbackException $ do
    Opt.customExecParser (Opt.prefs Opt.showHelpOnEmpty) cmdParserInfo >>= \case
      Speed what -> case what of
        Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
        Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
        Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId

      IndexerCommand indexerCommand' batchSize severity maybePort checkpointInterval parallelism -> let

        commonConfig :: CommonConfig
        commonConfig = CommonConfig{batchSize, stopSignal, checkpointSignal, statsSignal, severity, checkpointInterval}

        assertNoApi :: IO ()
        assertNoApi = case maybePort of
          Just{} -> E.throwIO E.Indexer_has_no_HTTP_API
          Nothing -> return ()

        runIndexerStateful :: (Show a, Indexer a) => a -> IO ()
        runIndexerStateful cli = assertNoApi >> case parallelism of
          DefaultParallelism -> runIndexer cli commonConfig Nothing
          NoParallelism      -> runIndexer cli commonConfig Nothing
          YesParallelism{}   -> E.throwIO E.Stateful_indexer_can't_be_run_with_parallelilsm

        runIndexerStateless :: (Show a, StatelessIndexer a) => a -> IO ()
        runIndexerStateless cli = assertNoApi >> case parallelism of
          DefaultParallelism -> runIndexer cli commonConfig Nothing
          YesParallelism{parallelismConfig, debugIntervals} -> if debugIntervals
            then printChainIntervals cli commonConfig parallelismConfig
            else runIndexerParallel cli commonConfig parallelismConfig
          NoParallelism -> runIndexer cli commonConfig Nothing

        in case indexerCommand' of
          BlockBasics        cli -> runIndexerStateless cli
          MintBurn           cli -> runIndexerStateless cli
          NoOp               cli -> runIndexerStateless cli
          EpochStakepoolSize cli -> runIndexerStateful  cli
          EpochNonce         cli -> runIndexerStateful  cli
          Fingerprint        cli -> runIndexerStateless cli
          ScriptTx           cli -> runIndexerStateless cli
          Deposit            cli -> runIndexerStateless cli
          AddressDatum       cli -> runIndexerStateless cli
          Utxo               cli -> runIndexerStateful  cli
          AddressBalance     cli -> runIndexerStateful  cli
          Datum              cli -> runIndexerStateless cli
          IntraBlockSpends   cli -> runIndexerStateful  cli
          AddressAppears     cli -> runIndexerStateless cli
          -- Mamba has a HTTP API defined, so we use it:
          Mamba              cli -> runIndexer cli commonConfig $ case maybePort of
            Just port -> Just $ \runtime mvar -> do
              let app = Servant.serve (Proxy @(API Mamba.Mamba)) $ server runtime mvar
              Warp.run port app
            Nothing -> Nothing

      FoldLedgerState cli -> FoldLedgerState.run cli stopSignal statsSignal
      SlotNoChainPoint dbPath slotNo -> SlotNoChainPoint.run dbPath slotNo

printRollbackException :: IO () -> IO ()
printRollbackException io = io `IO.catches`
  -- Draw attention to broken assumptions
  [ IO.Handler $ \(a :: E.CardanoAssumptionBroken) -> do
      putStrLn "An assumption about how Cardano works was broken:"
      print a
      putStrLn "Either the assumption is not true or there is a bug."
      IO.throwIO a
  ]

-- * Arguments

data Command
  = Speed Speed.BlockSource
  | IndexerCommand IndexerCommand BatchSize CM.Severity (Maybe Int) CheckpointInterval Parallelism
  | FoldLedgerState FoldLedgerState.FoldLedgerState
  | SlotNoChainPoint FilePath C.SlotNo
  deriving Show

data IndexerCommand
  = BlockBasics BlockBasics.BlockBasics
  | MintBurn MintBurn.MintBurn
  | NoOp NoOp.NoOp
  | EpochStakepoolSize EpochStakepoolSize.EpochStakepoolSize
  | EpochNonce EpochNonce.EpochNonce
  | Fingerprint Fingerprint.Fingerprint
  | ScriptTx ScriptTx.ScriptTx
  | Deposit Deposit.Deposit
  | Utxo Utxo.Utxo
  | AddressAppears AddressAppears.AddressAppears
  | AddressBalance AddressBalance.AddressBalance
  | AddressDatum AddressDatum.AddressDatum
  | Mamba Mamba.Mamba
  | Datum Datum.Datum
  | IntraBlockSpends IntraBlockSpends.IntraBlockSpends
  deriving Show

data Parallelism
  = NoParallelism
  | YesParallelism { parallelismConfig :: ParallelismConfig, debugIntervals :: Bool }
  | DefaultParallelism
  deriving Show

cmdParserInfo :: Opt.ParserInfo Command
cmdParserInfo = Opt.info (Opt.helper <*> cmdParser) $ Opt.fullDesc
  <> Opt.progDesc "mafoc"
  <> Opt.header "mafoc - Maps and folds over Cardano blockchain"

cmdParser :: Opt.Parser Command
cmdParser = Opt.subparser (indexers <> Opt.commandGroup "Indexers:")
    Opt.<|> Opt.subparser (other <> Opt.commandGroup "Other:")

  where
    other :: Opt.Mod Opt.CommandFields Command
    other =
         Opt.command "speed" (speedParserInfo :: Opt.ParserInfo Command)
      <> Opt.command "fold-ledgerstate" (FoldLedgerState <$> FoldLedgerState.parseCli)
      <> Opt.command "slot-chainpoint" (Opt.parserToParserInfo "slot-chainpoint" "slot-chainpoint" $ SlotNoChainPoint <$> Opt.strArgument (Opt.metavar "DB-PATH") <*> Opt.argument (C.SlotNo <$> Opt.auto) (Opt.metavar "SLOT-NO"))

    indexers :: Opt.Mod Opt.CommandFields Command
    indexers =
         indexerCommand "addressappears" AddressAppears
      <> indexerCommand "addressbalance" AddressBalance
      <> indexerCommand "addressdatum" AddressDatum
      <> indexerCommand "blockbasics" BlockBasics
      <> indexerCommand "deposit" Deposit
      <> indexerCommand "datum" Datum
      <> indexerCommand "epochnonce" EpochNonce
      <> indexerCommand "epochstakepoolsize" EpochStakepoolSize
      <> indexerCommand "fingerprint" Fingerprint
      <> indexerCommand "mamba" Mamba
      <> indexerCommand "mintburn" MintBurn
      <> indexerCommand "noop" NoOp
      <> indexerCommand "scripttx" ScriptTx
      <> indexerCommand "utxo" Utxo
      <> indexerCommand "intrablockspends" IntraBlockSpends

indexerCommand :: forall a . Indexer a => String -> (a -> IndexerCommand) -> Opt.Mod Opt.CommandFields Command
indexerCommand name f = Opt.command name $ Opt.parserToParserInfo descr (name <> " - " <> descr) $
  IndexerCommand
  <$> (f <$> parseCli @a)
  <*> Opt.commonBatchSize
  <*> Opt.commonLogSeverity
  <*> Opt.commonRunHttpApi
  <*> Opt.commonCheckpointInterval
  <*> (pure DefaultParallelism <|> yesParallelism <|> noParallelism)
  where
    descr = TS.unpack (description @a)

    noParallelism = let
      noThreadingDescr = "Explicitly request for no parallelism"
      opt1 = Opt.longOpt "no-parallelism" noThreadingDescr
      opt2 = Opt.longOpt "no-threading" noThreadingDescr
      in Opt.switch (opt1 <> opt2) $> NoParallelism

    yesParallelism =
      Opt.switch (Opt.longOpt "parallel" "Run in parallel") *> parallelismOpts
    parallelismOpts = YesParallelism
      <$> Opt.commonParallelismConfig
      <*> Opt.switch (Opt.longOpt "only-print-intervals" "Just print chain intervals, don't index.")

speedParserInfo :: Opt.ParserInfo Command
speedParserInfo = Opt.info parser help
  where
    parser = Opt.helper <*> (Speed <$> blockSource)
    help = Opt.fullDesc <> Opt.progDesc "speed" <> Opt.header "speed - Measure local chain sync speed"
    blockSource :: Opt.Parser Speed.BlockSource
    blockSource = Opt.subparser
       $ Opt.command "callback" (Opt.info callback mempty)
      <> Opt.command "callback-pipelined" (Opt.info callbackPipelined mempty)
      <> Opt.command "rewindable-index" (Opt.info rewindableIndex_ mempty)
      where
        callback = Speed.Callback
          <$> Opt.commonSocketPath
          <*> Opt.commonNodeConfig
          <*> Opt.commonMaybeChainPointStart
          <*> Opt.commonMaybeUntilSlot
        callbackPipelined = Speed.CallbackPipelined
          <$> Opt.commonSocketPath
          <*> Opt.commonNodeConfig
          <*> Opt.commonMaybeChainPointStart
          <*> Opt.commonMaybeUntilSlot
          <*> Opt.commonPipelineSize
        rewindableIndex_ = Speed.RewindableIndex
          <$> Opt.commonSocketPath
          <*> Opt.commonMaybeChainPointStart
          <*> Opt.commonMaybeUntilSlot
          <*> Opt.commonNetworkId
