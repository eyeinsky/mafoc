{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Control.Exception qualified as IO
import Options.Applicative qualified as Opt

import Cardano.Streaming.Callbacks qualified as CS

import Mafoc.CLI qualified as Opt
import Mafoc.Core (runIndexer)
import Mafoc.Folds.EpochStakepoolSize qualified as EpochStakepoolSize
import Mafoc.Maps.BlockBasics qualified as BlockBasics
import Mafoc.Maps.MintBurn qualified as MintBurn
import Mafoc.Maps.NoOp qualified as NoOp
import Mafoc.Maps.ScriptTx qualified as ScriptTx
import Mafoc.Speed qualified as Speed

main :: IO ()
main = printRollbackException $ Opt.execParser cmdParserInfo >>= \case
  Speed what -> case what of
    Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
    Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
    Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId
  BlockBasics configFromCli -> runIndexer configFromCli
  MintBurn configFromCli -> runIndexer configFromCli
  NoOp configFromCli -> runIndexer configFromCli
  EpochStakepoolSize configFromCli -> runIndexer configFromCli
  ScriptTx configFromCli -> runIndexer configFromCli

printRollbackException :: IO () -> IO ()
printRollbackException io = io `IO.catch` (\(a :: IO.SomeException) -> print a)

-- * Arguments

data Command
  = Speed Speed.BlockSource
  | BlockBasics BlockBasics.BlockBasics
  | MintBurn MintBurn.MintBurn
  | NoOp NoOp.NoOp
  | EpochStakepoolSize EpochStakepoolSize.EpochStakepoolSize
  | ScriptTx ScriptTx.ScriptTx
  deriving Show

cmdParserInfo :: Opt.ParserInfo Command
cmdParserInfo = Opt.info (Opt.helper <*> cmdParser) $ Opt.fullDesc
  <> Opt.progDesc "mafoc"
  <> Opt.header "mafoc - Maps and folds over Cardano blockchain"

cmdParser :: Opt.Parser Command
cmdParser = Opt.subparser
  $ Opt.command "speed" speedParserInfo
 <> Opt.command "blockbasics" (BlockBasics <$> BlockBasics.parseCli)
 <> Opt.command "mintburn" (MintBurn <$> MintBurn.parseCli)
 <> Opt.command "noop" (NoOp <$> NoOp.parseCli)
 <> Opt.command "epochstakepoolsize" (EpochStakepoolSize <$> EpochStakepoolSize.parseCli)

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
