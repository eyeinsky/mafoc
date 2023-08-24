{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception qualified as IO
import Options.Applicative qualified as Opt
import Data.Text qualified as TS

import Cardano.Streaming.Callbacks qualified as CS

import Mafoc.CLI qualified as Opt
import Mafoc.Cmds.FoldLedgerState qualified as FoldLedgerState
import Mafoc.Core (BatchSize, Indexer (description, parseCli), runIndexer)
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
main = printRollbackException $ Opt.execParser cmdParserInfo >>= runCommand

runCommand :: Command -> IO ()
runCommand = \case
  Speed what -> case what of
    Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
    Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
    Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId

  IndexerCommand indexerCommand' batchSize -> let
    runIndexer' = case indexerCommand' of
      BlockBasics configFromCli        -> runIndexer configFromCli
      MintBurn configFromCli           -> runIndexer configFromCli
      NoOp configFromCli               -> runIndexer configFromCli
      EpochStakepoolSize configFromCli -> runIndexer configFromCli
      EpochNonce configFromCli         -> runIndexer configFromCli
      ScriptTx configFromCli           -> runIndexer configFromCli
      Deposit configFromCli            -> runIndexer configFromCli
      Utxo configFromCli               -> runIndexer configFromCli
      AddressBalance configFromCli     -> runIndexer configFromCli
      AddressDatum configFromCli       -> runIndexer configFromCli
      Mamba configFromCli              -> runIndexer configFromCli
    in runIndexer' batchSize

  FoldLedgerState configFromCli -> FoldLedgerState.run configFromCli

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
  | IndexerCommand IndexerCommand BatchSize
  | FoldLedgerState FoldLedgerState.FoldLedgerState
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
cmdParser = Opt.subparser
   $ Opt.command "speed" (speedParserInfo :: Opt.ParserInfo Command)
  <> Opt.command "fold-ledgerstate" (FoldLedgerState <$> FoldLedgerState.parseCli)
  <> indexerCommand' "addressbalance" AddressBalance
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
  where
    indexerCommand' name f = indexerCommand name (\(i, bs) -> IndexerCommand (f i) bs)

indexerCommand :: forall a b . Indexer a => String -> ((a, BatchSize) -> b) -> Opt.Mod Opt.CommandFields b
indexerCommand name f = Opt.command name (f <$> indexerParserInfo name)

indexerParserInfo :: forall a . Indexer a => String -> Opt.ParserInfo (a, BatchSize)
indexerParserInfo name = Opt.info (Opt.helper <*> parseCliWithBatchSize) $ Opt.fullDesc
  <> Opt.progDesc name
  <> Opt.header (name <> " - " <> TS.unpack (description @a))
  where
    parseCliWithBatchSize = (,) <$> parseCli @a <*> Opt.commonBatchSize

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
