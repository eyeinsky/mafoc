module Mafoc.CLI where

import Cardano.Api qualified as C
import Data.ByteString.Char8 qualified as C8
import Data.Proxy (Proxy (Proxy))
import Data.Word (Word32)
import Options.Applicative qualified as O

-- * Options

commonSocketPath :: O.Parser FilePath
commonSocketPath = O.strOption (opt 's' "socket-path" "Path to node socket.")

commonDbPath :: O.Parser FilePath
commonDbPath = O.strOption (opt 'd' "db-path" "Path to sqlite database.")

commonNodeConfig :: O.Parser FilePath
commonNodeConfig = O.strOption (opt 'c' "node-config" "Path to node configuration.")

commonUntilSlot :: O.Parser C.SlotNo
commonUntilSlot = parseSlotNo 'u' "until-slot" "Slot number until"

commonMaybeUntilSlot :: O.Parser (Maybe C.SlotNo)
commonMaybeUntilSlot = Just <$> parseSlotNo 'u' "until-slot" "Slot number until" O.<|> pure Nothing

commonMaybeChainPointStart :: O.Parser (Maybe C.ChainPoint)
commonMaybeChainPointStart = (Just <$> cp) O.<|> pure Nothing
  where
    cp :: O.Parser C.ChainPoint
    cp = C.ChainPoint
      <$> O.option (C.SlotNo <$> O.auto) (opt 'n' "slot-no" "Slot number")
      <*> O.option hashReader (opt 'b' "block-hash" "Hash of block header" <> O.metavar "BLOCK-HASH")

    hashReader :: O.ReadM (C.Hash C.BlockHeader)
    hashReader = O.maybeReader maybeParseHashBlockHeader O.<|> O.readerError "Malformed block hash"

    maybeParseHashBlockHeader :: String -> Maybe (C.Hash C.BlockHeader)
    maybeParseHashBlockHeader =
      either (const Nothing) Just
      . C.deserialiseFromRawBytesHex (C.proxyToAsType Proxy)
      . C8.pack

commonPipelineSize :: O.Parser Word32
commonPipelineSize = O.option O.auto (opt 'p' "pipeline-size" "Size of piplined requests.")

commonNetworkId :: O.Parser C.NetworkId
commonNetworkId = mainnet <|> C.Testnet <$> testnet
  where
    mainnet :: O.Parser C.NetworkId
    mainnet = O.flag' C.Mainnet (O.long "mainnet" <> O.help "Use the mainnet magic id.")

    testnet :: O.Parser C.NetworkMagic
    testnet = C.NetworkMagic <$> O.option O.auto
        (O.long "testnet-magic"
         <> O.metavar "NATURAL"
         <> O.help "Specify a testnet magic id.")

commonSecurityParam :: O.Parser Natural
commonSecurityParam = O.option O.auto (opt 'k' "security-param" "Security parameter -- number of slots after which they can't be rolled back")

commonSecurityParamEither :: O.Parser (Either Natural FilePath)
commonSecurityParamEither = Left <$> commonSecurityParam <|> Right <$> commonNodeConfig

-- * Helpers

simpleCmd :: String -> a -> O.Mod O.CommandFields a
simpleCmd str a = O.command str $ O.info (pure a) mempty

opt :: Char -> String -> String -> O.Mod O.OptionFields a
opt short long help = O.long long <> O.short short <> O.help help

parseSlotNo :: Char -> String -> String -> O.Parser C.SlotNo
parseSlotNo short long help = O.option (C.SlotNo <$> O.auto) (opt short long help)
