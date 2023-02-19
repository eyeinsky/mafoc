module Mafoc.CLI where

import Data.ByteString.Char8 qualified as C8
import Data.List qualified as L
import Data.Proxy (Proxy (Proxy))
import Data.Word (Word32)
import Numeric.Natural (Natural)
import Options.Applicative ((<|>))
import Options.Applicative qualified as O
import Text.Read qualified as Read

import Cardano.Api qualified as C
import Mafoc.Core (DbPathAndTableName (DbPathAndTableName), Interval (Interval),
                   LocalChainsyncConfig (LocalChainsyncConfig), UpTo (CurrentTip, Infinity, SlotNo))

-- * Options

commonSocketPath :: O.Parser FilePath
commonSocketPath = O.strOption (opt 's' "socket-path" "Path to node socket.")

commonDbPath :: O.Parser FilePath
commonDbPath = O.strOption (opt 'd' "db-path" "Path to sqlite database.")

commonDbPathAndTableName :: O.Parser DbPathAndTableName
commonDbPathAndTableName = O.option (O.eitherReader parse) (opt 'd' "db-path" "Path to sqlite database and optionally table name.")
  where
    parse :: String -> Either String DbPathAndTableName
    parse str = let (dbPath, tableName) = L.span (/= ':') str
      in do
      dbPath' <- if L.null dbPath then Left "Can't parse empty database path" else Right dbPath
      DbPathAndTableName dbPath' <$> case tableName of
        ':' : rest@(_:_) -> Right $ Just rest
        _                -> Right Nothing

commonNodeConfig :: O.Parser FilePath
commonNodeConfig = O.strOption (opt 'c' "node-config" "Path to node configuration.")

commonUntilSlot :: O.Parser C.SlotNo
commonUntilSlot = parseSlotNo 'u' "until-slot" "Slot number until"

commonMaybeUntilSlot :: O.Parser (Maybe C.SlotNo)
commonMaybeUntilSlot = Just <$> commonUntilSlot <|> pure Nothing

commonMaybeChainPointStart :: O.Parser (Maybe C.ChainPoint)
commonMaybeChainPointStart = Just <$> cp <|> pure Nothing
  where
    cp :: O.Parser C.ChainPoint
    cp = C.ChainPoint
      <$> O.option (C.SlotNo <$> O.auto) (opt 'n' "slot-no" "Slot number")
      <*> O.option hashReader (opt 'b' "block-hash" "Hash of block header" <> O.metavar "BLOCK-HASH")

    hashReader :: O.ReadM (C.Hash C.BlockHeader)
    hashReader = O.maybeReader maybeParseHashBlockHeader <|> O.readerError "Malformed block hash"

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

commonInterval :: O.Parser Interval
commonInterval = O.option (O.eitherReader parseIntervalEither) (opt 'i' "interval" "Chain interval to index")

commonLogging :: O.Parser Bool
commonLogging = O.option O.auto (opt 'q' "quiet" "Don't do any logging" <> O.value True)

commonChunkSize :: O.Parser Natural
commonChunkSize = O.option O.auto (longOpt "chunk-size" "Size of buffer to be inserted into sqlite" <> O.internal)

commonLocalChainsyncConfig :: O.Parser LocalChainsyncConfig
commonLocalChainsyncConfig = LocalChainsyncConfig
  <$> commonSocketPath
  <*> commonSecurityParamEither
  <*> commonInterval
  <*> commonNetworkId
  <*> commonLogging
  <*> commonPipelineSize
  <*> commonChunkSize

-- * Parse interval

parseIntervalEither :: String -> Either String Interval
parseIntervalEither str = Interval <$> parseFrom from <*> parseUpTo upTo
  where (from, upTo) = L.span (/= '-') str

parseFrom :: String -> Either String C.ChainPoint
parseFrom str = case str of
  "" -> Right C.ChainPointAtGenesis
  "0" -> Right C.ChainPointAtGenesis
  _ -> do
    let (fromSlotNo, bhh) = L.span (/= ':') str
    slotNo <- parseSlotNo_ fromSlotNo
    blockHeaderHash <- case bhh of
      ':' : str' -> maybe (leftError "Can't read block header hash" bhh) Right $ maybeParseHashBlockHeader str'
      _          -> leftError "No block header hash" ""
    return $ C.ChainPoint slotNo blockHeaderHash

parseUpTo :: String -> Either String UpTo
parseUpTo str = case str of
  '-' : rest -> case rest of
    "@" -> Right CurrentTip
    ""  -> Right Infinity
    _   -> SlotNo <$> parseSlotNo_ rest
  "" -> Right Infinity
  _ -> leftError "Can't read slot interval end" str

parseSlotNo_ :: String -> Either String C.SlotNo
parseSlotNo_ str = maybe (leftError "Can't read SlotNo" str) (Right . C.SlotNo) $ Read.readMaybe str

leftError :: String -> String -> Either String a
leftError label str = Left $ label <> ": '" <> str <> "'"

maybeParseHashBlockHeader :: String -> Maybe (C.Hash C.BlockHeader)
maybeParseHashBlockHeader =
  either (const Nothing) Just
  . C.deserialiseFromRawBytesHex (C.proxyToAsType Proxy)
  . C8.pack

-- * Helpers

simpleCmd :: String -> a -> O.Mod O.CommandFields a
simpleCmd str a = O.command str $ O.info (pure a) mempty

opt :: Char -> String -> String -> O.Mod O.OptionFields a
opt short long help = O.long long <> O.short short <> O.help help

longOpt :: String -> String -> O.Mod O.OptionFields a
longOpt long help = O.long long <> O.help help

parseSlotNo :: Char -> String -> String -> O.Parser C.SlotNo
parseSlotNo short long help = O.option (C.SlotNo <$> O.auto) (opt short long help)
