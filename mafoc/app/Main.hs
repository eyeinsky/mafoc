{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Options.Applicative qualified as Opt

import Cardano.Streaming.Callbacks qualified as CS

import Mafoc.CLI qualified as Opt
import Mafoc.Indexer.Class (Indexer (initialize, run))
import Mafoc.Maps.BlockBasics qualified as BlockBasics
import Mafoc.Maps.MintBurn qualified as MintBurn
import Mafoc.Speed qualified as Speed

main :: IO ()
main = parseAndPrintCli >>= \case
  Speed what -> case what of
    Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
    Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
    Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId
  BlockBasics configFromCli -> run =<< initialize configFromCli
  MintBurn configFromCli -> run =<< initialize configFromCli

parseAndPrintCli :: IO Command
parseAndPrintCli = do
  cmd <- Opt.execParser cmdParserInfo
  print cmd
  return cmd

-- * Arguments

data Command
  = Speed Speed.BlockSource
  | BlockBasics BlockBasics.BlockBasics
  | MintBurn MintBurn.MintBurn
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
