module Mafoc.CLI where

import Data.Coerce (coerce)
import Data.List qualified as L
import Data.Proxy (Proxy (Proxy))
import Data.Text qualified as TS
import Data.Word (Word32)
import Options.Applicative ((<|>))
import Options.Applicative qualified as O
import Text.Read qualified as Read
import Data.List.NonEmpty qualified as NE

import Cardano.Api qualified as C
import Mafoc.Core (BatchSize, ConcurrencyPrimitive, DbPathAndTableName (DbPathAndTableName), Interval (Interval),
                   LocalChainsyncConfig (LocalChainsyncConfig), LocalChainsyncConfig_, NodeConfig (NodeConfig),
                   NodeFolder (NodeFolder), NodeInfo (NodeInfo), SocketPath (SocketPath),
                   UpTo (CurrentTip, Infinity, SlotNo))
import Mafoc.StateFile (eitherParseHashBlockHeader, leftError, parseSlotNo_)
import Marconi.ChainIndex.Types qualified as Marconi

-- * Options

commonSocketPath :: O.Parser FilePath
commonSocketPath = O.strOption (opt 's' "socket-path" "Path to node socket.")

commonDbPath :: O.Parser FilePath
commonDbPath = O.strOption (opt 'd' "db-path" "Path to sqlite database.")

commonDbPathAndTableName :: O.Parser DbPathAndTableName
commonDbPathAndTableName = O.option (O.eitherReader parse) $ opt 'd' "db"
  "Optional path to sqlite database (defaults to default.db) and a table name (default is indexer-specific)."
  <> O.value (DbPathAndTableName Nothing Nothing)
  where
    parse :: String -> Either String DbPathAndTableName
    parse str = let
      (dbPath, tableName) = L.span (/= ':') str
      dbPath' = if L.null dbPath then Nothing else Just dbPath
      tableName' = case tableName of
        ':' : rest@(_:_) -> Just rest
        _                -> Nothing
      in Right $ DbPathAndTableName dbPath' tableName'


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
commonPipelineSize = O.option O.auto
  $ opt 'p' "pipeline-size" "Size of piplined requests."
  <> O.value 500

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

commonSecurityParam :: O.Parser Marconi.SecurityParam
commonSecurityParam = O.option O.auto (opt 'k' "security-param" "Security parameter -- number of slots after which they can't be rolled back")

commonSecurityParamEither :: O.Parser (Either C.NetworkId NodeConfig)
commonSecurityParamEither = Left <$> commonNetworkId <|> Right . coerce <$> commonNodeConfig

commonNodeFolder :: O.Mod O.ArgumentFields NodeConfig
commonNodeFolder =
     O.metavar "NODE-FOLDER"
  <> O.help "Path to cardano-node's folder. The program will figure out socket path, security parameter, network and node config path from it."

commonNodeConnection :: O.Parser (NodeInfo (Either C.NetworkId NodeConfig))
commonNodeConnection = coerce
  <$> (    Left <$> O.strArgument commonNodeFolder
       <|> Right <$> ((,) <$> commonSocketPath <*> commonSecurityParamEither)
      )

commonNodeConnectionAndConfig :: O.Parser (NodeInfo NodeConfig)
commonNodeConnectionAndConfig = coerce
  <$> (   Left <$> O.strArgument commonNodeFolder
      <|> Right <$> ((,) <$> commonSocketPath <*> commonNodeConfig)
      )

commonInterval :: O.Parser Interval
commonInterval = O.option (O.eitherReader parseIntervalEither)
  $ opt 'i' "interval" "Chain interval to index, defaults to from genesis to infinity"
  <> O.value (Interval C.ChainPointAtGenesis Infinity)

commonUpTo :: O.Parser UpTo
commonUpTo = O.option (O.eitherReader parseUpTo)
  $ opt 'u' "upto" "An up-to point: <slot no> or '@' which stands for current tip."

commonLogging :: O.Parser Bool
commonLogging = O.option O.auto (opt 'q' "quiet" "Don't do any logging" <> O.value True)

commonBatchSize :: O.Parser BatchSize
commonBatchSize = O.option O.auto
  $ longOpt "batch-size" "Batche size for persisting events"
  <> O.value 3000

commonLocalChainsyncConfig :: O.Parser LocalChainsyncConfig_
commonLocalChainsyncConfig = mkCommonLocalChainsyncConfig commonNodeConnection

mkCommonLocalChainsyncConfig
  :: O.Parser (NodeInfo a)
  -> O.Parser (LocalChainsyncConfig a)
mkCommonLocalChainsyncConfig commonNodeConnection_ = LocalChainsyncConfig
  <$> commonNodeConnection_
  <*> commonInterval
  <*> commonLogging
  <*> commonPipelineSize
  <*> commonConcurrencyPrimitive

commonAddress :: O.Parser (C.Address C.ShelleyAddr)
commonAddress = O.option (O.eitherReader (deserializeToCardano' . TS.pack)) $ opt 'a' "address" "Cardano Shelly address"
  where
    deserializeToCardano = C.deserialiseFromBech32 (C.proxyToAsType Proxy)
    deserializeToCardano' = either (Left . show) Right . deserializeToCardano

commonUtxoState :: O.Parser FilePath
commonUtxoState = O.option O.auto (O.long "utxo-state" <> O.value "utxoState")

-- * String parsers

maybeParseHashBlockHeader :: String -> Maybe (C.Hash C.BlockHeader)
maybeParseHashBlockHeader = either (const Nothing) Just . eitherParseHashBlockHeader

-- ** Interval

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
      ':' : str' -> either (Left . show) Right $ eitherParseHashBlockHeader str'
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

-- * Block channel

commonConcurrencyPrimitive :: O.Parser (Maybe ConcurrencyPrimitive)
commonConcurrencyPrimitive = O.option reader $
  O.long "concurrency-primitive"
    <> O.help helpText
    <> O.hidden
    <> O.value Nothing
  where
    values :: String
    values = L.intercalate ", " (map show [minBound .. maxBound :: ConcurrencyPrimitive])

    helpText :: String
    helpText =
        "Choose between concurrency primitives for passing blocks from local\
        \ chainsync thread to the indexer. The choice currently is: "
      <> values

    readStr :: String -> Maybe (Maybe ConcurrencyPrimitive)
    readStr str = case Read.readMaybe str :: Maybe ConcurrencyPrimitive of
      Just (cp :: ConcurrencyPrimitive) -> Just (Just cp)
      _                                 -> Nothing

    reader :: O.ReadM (Maybe ConcurrencyPrimitive)
    reader = O.maybeReader readStr
      <|> O.readerError ("Can't parse concurrency primitive, must be one of: " <> values)

-- * Helpers

-- | The "real" @some@.
--
-- optparse-applicative's @some@ returns a list, which doesn't reflect
-- on the type level that it must have at least one member.
some_ :: O.Parser a -> O.Parser (NE.NonEmpty a)
some_ p = O.liftA2 (NE.:|) p (O.many p)

simpleCmd :: String -> a -> O.Mod O.CommandFields a
simpleCmd str a = O.command str $ O.info (pure a) mempty

opt :: Char -> String -> String -> O.Mod O.OptionFields a
opt short long help = O.long long <> O.short short <> O.help help

longOpt :: O.HasName f => String -> String -> O.Mod f a
longOpt long help = O.long long <> O.help help

parseSlotNo :: Char -> String -> String -> O.Parser C.SlotNo
parseSlotNo short long help = O.option (C.SlotNo <$> O.auto) (opt short long help)
