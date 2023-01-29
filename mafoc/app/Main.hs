{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C
import Cardano.Streaming.Callbacks qualified as CS

import Mafoc.CLI qualified as Opt
import Mafoc.Maps.BlockTxCount qualified as Maps
import Mafoc.Maps.MintBurn qualified as MintBurn
import Mafoc.Speed qualified as Speed

main :: IO ()
main = do
  cmd <- Opt.execParser cmdParserInfo
  print cmd
  case cmd of
    Speed what -> case what of
      Speed.Callback socketPath nodeConfig start end -> Speed.mkCallback CS.blocksCallback socketPath nodeConfig start end
      Speed.CallbackPipelined socketPath nodeConfig start end n -> Speed.mkCallback (CS.blocksCallbackPipelined n) socketPath nodeConfig start end
      Speed.RewindableIndex socketPath start end networkId -> Speed.rewindableIndex socketPath start end networkId
    TxCount socketPath dbPath nodeConfig cpFromCli maybeEnd networkId -> Maps.indexer socketPath dbPath nodeConfig cpFromCli maybeEnd networkId
    MintBurn socketPath networkId dbPath start end kOrNodeConfig -> MintBurn.indexer socketPath networkId dbPath start end kOrNodeConfig

-- * Arguments

data Command
  = Speed Speed.BlockSource
  | TxCount
      { txCountSocketPath     :: String
      , txCountDbPath         :: String
      , txCountNodeConfigPath :: String
      , txCountStart          :: Maybe C.ChainPoint
      , txCountEnd            :: Maybe C.SlotNo
      , txCountNetworkId      :: C.NetworkId
      }
  | MintBurn
      { mintBurnSocketPath                :: String
      , mintBurnNetworkId                 :: C.NetworkId
      , mintBurnDbPath                    :: FilePath
      , mintBurnStart                     :: Maybe C.ChainPoint
      , mintBurnEnd                       :: Maybe C.SlotNo
      , mintBurnSecurityParamOrNodeConfig :: Either Natural FilePath
      }
  deriving Show

cmdParserInfo :: Opt.ParserInfo Command
cmdParserInfo = Opt.info (Opt.helper <*> cmdParser) $ Opt.fullDesc
  <> Opt.progDesc "mafoc"
  <> Opt.header "mafoc - Maps and folds over Cardano blockchain"

cmdParser :: Opt.Parser Command
cmdParser = Opt.subparser
  $ Opt.command "speed" speedParserInfo
 <> Opt.command "txcount" txCountParserInfo
 <> Opt.command "mintburn" mintBurnParserInfo

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

txCountParserInfo :: Opt.ParserInfo Command
txCountParserInfo = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "txcount"
  <> Opt.header "txcount - Count transactions per block"
  where
    cli :: Opt.Parser Command
    cli = TxCount
      <$> Opt.commonSocketPath
      <*> Opt.commonDbPath
      <*> Opt.commonNodeConfig
      <*> Opt.commonMaybeChainPointStart
      <*> Opt.commonMaybeUntilSlot
      <*> Opt.commonNetworkId

mintBurnParserInfo :: Opt.ParserInfo Command
mintBurnParserInfo = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "mintburn"
  <> Opt.header "mintburn - Index mint and burn events"
  where
    cli :: Opt.Parser Command
    cli = MintBurn
      <$> Opt.commonSocketPath
      <*> Opt.commonNetworkId
      <*> Opt.commonDbPath
      <*> Opt.commonMaybeChainPointStart
      <*> Opt.commonMaybeUntilSlot
      <*> Opt.commonSecurityParamEither
