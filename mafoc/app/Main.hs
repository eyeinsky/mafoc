{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Concurrent qualified as IO
import Control.Exception qualified as IO
import Control.Monad (void, when)
import Options.Applicative qualified as Opt
import Data.Text qualified as TS
import System.Posix.Signals qualified as Signals

import Cardano.Api qualified as C
import Cardano.Streaming.Callbacks qualified as CS

import Mafoc.CLI qualified as Opt
import Mafoc.Cmds.FoldLedgerState qualified as FoldLedgerState
import Mafoc.Cmds.SlotNoChainPoint qualified as SlotNoChainPoint
import Mafoc.Core (BatchSize, DbPathAndTableName, Indexer (description, parseCli), StopSignal (StopSignal), runIndexer)
import Mafoc.Exceptions qualified as E
import Mafoc.Indexers.AddressBalance qualified as AddressBalance
import Mafoc.Indexers.AddressDatum qualified as AddressDatum
import Mafoc.Indexers.BlockBasics qualified as BlockBasics
import Mafoc.Indexers.Deposit qualified as Deposit
import Mafoc.Indexers.EpochNonce qualified as EpochNonce
import Mafoc.Indexers.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Indexers.Mamba qualified as Mamba
import Mafoc.Indexers.MintBurn qualified as MintBurn
import Mafoc.Indexers.NoOp qualified as NoOp
import Mafoc.Indexers.ScriptTx qualified as ScriptTx
import Mafoc.Indexers.Utxo qualified as Utxo
import Mafoc.Speed qualified as Speed

main :: IO ()
main = do
  stopSignal <- setupCtrlCHandler 3
  printRollbackException $ do
    Opt.customExecParser (Opt.prefs Opt.showHelpOnEmpty) cmdParserInfo >>= \case
      Speed what -> case what of
        Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
        Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
        Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId

      IndexerCommand indexerCommand' batchSize dbPathAndTableName -> let
        runIndexer' = case indexerCommand' of
          BlockBasics configFromCli        -> runIndexer configFromCli
          MintBurn configFromCli           -> runIndexer configFromCli
          NoOp configFromCli               -> runIndexer configFromCli
          EpochStakepoolSize configFromCli -> runIndexer configFromCli
          EpochNonce configFromCli         -> runIndexer configFromCli
          ScriptTx configFromCli           -> runIndexer configFromCli
          Deposit configFromCli            -> runIndexer configFromCli
          AddressDatum configFromCli       -> runIndexer configFromCli
          Utxo configFromCli               -> runIndexer configFromCli
          AddressBalance configFromCli     -> runIndexer configFromCli
          Mamba configFromCli              -> runIndexer configFromCli
        in runIndexer' batchSize stopSignal dbPathAndTableName

      FoldLedgerState configFromCli -> FoldLedgerState.run configFromCli stopSignal
      SlotNoChainPoint dbPath slotNo -> SlotNoChainPoint.run dbPath slotNo


-- | Returns an MVar which when filled, means that the program should close.
setupCtrlCHandler :: Int -> IO StopSignal
setupCtrlCHandler max' = do
  countMVar <- IO.newMVar 0
  signalMVar <- IO.newMVar False
  let ctrlcHandler = Signals.Catch $ do
        count <- IO.modifyMVar countMVar incr
        void $ IO.swapMVar signalMVar (count >= 0)
        if count == 1
          then putStrLn "Shutting down gracefully.."
          else putStrLn "Press Ctrl-C few more times to terminate without a final checkpoint."
        when (count >= max') $ Signals.raiseSignal Signals.sigTERM
  _ <- Signals.installHandler Signals.sigINT ctrlcHandler Nothing
  return $ StopSignal signalMVar
  where
    incr :: Int -> IO (Int, Int)
    incr count = let count' = count + 1 in return (count', count')

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
  | IndexerCommand IndexerCommand BatchSize (Maybe DbPathAndTableName)
  | FoldLedgerState FoldLedgerState.FoldLedgerState
  | SlotNoChainPoint FilePath C.SlotNo
  deriving Show

data IndexerCommand
  = BlockBasics BlockBasics.BlockBasics
  | MintBurn MintBurn.MintBurn
  | NoOp NoOp.NoOp
  | EpochStakepoolSize EpochStakepoolSize.EpochStakepoolSize
  | EpochNonce EpochNonce.EpochNonce
  | ScriptTx ScriptTx.ScriptTx
  | Deposit Deposit.Deposit
  | Utxo Utxo.Utxo
  | AddressBalance AddressBalance.AddressBalance
  | AddressDatum AddressDatum.AddressDatum
  | Mamba Mamba.Mamba
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
      <> Opt.command "slot-chainpoint" (parserToParserInfo "slot-chainpoint" "slot-chainpoint" $ SlotNoChainPoint <$> Opt.strArgument (Opt.metavar "DB-PATH") <*> Opt.argument (C.SlotNo <$> Opt.auto) (Opt.metavar "SLOT-NO"))

    indexerCommand' name f = indexerCommand name (\(i, bs, db) -> IndexerCommand (f i) bs db)
    indexers :: Opt.Mod Opt.CommandFields Command
    indexers =
         indexerCommand' "addressbalance" AddressBalance
      <> indexerCommand' "addressdatum" AddressDatum
      <> indexerCommand' "blockbasics" BlockBasics
      <> indexerCommand' "deposit" Deposit
      <> indexerCommand' "epochnonce" EpochNonce
      <> indexerCommand' "epochstakepoolsize" EpochStakepoolSize
      <> indexerCommand' "mamba" Mamba
      <> indexerCommand' "mintburn" MintBurn
      <> indexerCommand' "noop" NoOp
      <> indexerCommand' "scripttx" ScriptTx
      <> indexerCommand' "utxo" Utxo

-- | Take program description, header and CLI parser, and turn it into a ParserInfo
parserToParserInfo :: String -> String -> Opt.Parser a -> Opt.ParserInfo a
parserToParserInfo progDescr header cli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc progDescr
  <> Opt.header header

indexerCommand :: forall a b . Indexer a => String -> ((a, BatchSize, Maybe DbPathAndTableName) -> b) -> Opt.Mod Opt.CommandFields b
indexerCommand name f = Opt.command name (f <$> indexerParserInfo name)

indexerParserInfo :: forall a . Indexer a => String -> Opt.ParserInfo (a, BatchSize, Maybe DbPathAndTableName)
indexerParserInfo name = Opt.info (Opt.helper <*> parseCliWithBatchSize) $ Opt.fullDesc
  <> Opt.progDesc name
  <> Opt.header (name <> " - " <> TS.unpack (description @a))
  where
    parseCliWithBatchSize :: Opt.Parser (a, BatchSize, Maybe DbPathAndTableName)
    parseCliWithBatchSize = (,,)
      <$> parseCli @a
      <*> Opt.commonBatchSize
      <*> Opt.option
            (Opt.eitherReader $ fmap Just . Opt.parseDbPathAndTableName)
            (Opt.value Nothing <> Opt.longOpt "header-db" "Optional path to sqlite database for block headers.")

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
