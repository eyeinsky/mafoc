{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE OverloadedLabels #-}
module Mafoc.CLI where

import Data.List qualified as L
import Data.Text qualified as TS
import Options.Applicative ((<|>))
import Options.Applicative qualified as O
import Text.Read qualified as Read
import Data.List.NonEmpty qualified as NE

import Cardano.BM.Data.Severity qualified as CM
import Cardano.Api qualified as C
import Mafoc.Core (BatchSize, ConcurrencyPrimitive(MVar), DbPathAndTableName (DbPathAndTableName), Interval (Interval),
                   LocalChainsyncConfig (LocalChainsyncConfig), LocalChainsyncConfig_, NodeConfig (NodeConfig),
                   NodeFolder (NodeFolder), NodeInfo (NodeInfo), SocketPath (SocketPath),
                   UpTo (CurrentTip, Infinity, SlotNo))
import Mafoc.Upstream (LedgerEra, lastChainPointOfPreviousEra, lastBlockOf)
import Mafoc.Logging (ProfilingConfig(ProfilingConfig))
import Mafoc.StateFile (eitherParseHashBlockHeader, leftError, parseSlotNo_)

import Marconi.ChainIndex.Types qualified as Marconi

-- * Options

commonSocketPath :: O.Parser FilePath
commonSocketPath = O.strOption (opt 's' "socket-path" "Path to node socket.")

commonDbPath :: O.Parser FilePath
commonDbPath = O.strOption (opt 'd' "db-path" "Path to sqlite database.")

commonDbPathAndTableName :: O.Parser DbPathAndTableName
commonDbPathAndTableName = O.option (O.eitherReader parseDbPathAndTableName) $ opt 'd' "db"
  "Optional path to sqlite database (defaults to default.db) and a table name (default is indexer-specific)."
  <> O.value (DbPathAndTableName Nothing Nothing)

parseDbPathAndTableName :: String -> Either String DbPathAndTableName
parseDbPathAndTableName str = let
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
    hashReader = O.eitherReader eitherParseHashBlockHeader <|> O.readerError "Malformed block hash"

commonPipelineSize :: O.Parser Word32
commonPipelineSize = O.option O.auto
  $ opt 'p' "pipeline-size" "Number of parallel requests for chainsync mini-protocol."
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
  <> O.value (Interval (False, Right C.ChainPointAtGenesis) Infinity)
  where
    parseIntervalEither :: String -> Either String Interval
    parseIntervalEither str = let
      (fromPart, upTo) = L.span (\c -> not $ c `elem` ("-+" :: String)) str
      in do
      from@(_, eitherSlotNoOrCp) <- fromEra fromPart <|> parseFrom fromPart
      Interval from <$> parseUpTo (#slotNo eitherSlotNoOrCp) upTo

    (parseEra, _listAsText) = boundedEnum @LedgerEra "era"

    -- Parse Interval{from} as era
    fromEra :: String -> Either String (Bool, Either C.SlotNo C.ChainPoint)
    fromEra str = startForEra =<< parseEra str
      where
        startForEra :: LedgerEra -> Either String (Bool, Either C.SlotNo C.ChainPoint)
        startForEra era = Right (False, Right $ lastChainPointOfPreviousEra era)

    -- Parse Interval{upTo} as era
    upToEra :: String -> Either String UpTo
    upToEra str = upUntilIncluding =<< parseEra str
      where
        upUntilIncluding :: LedgerEra -> Either String UpTo
        upUntilIncluding era = maybe (Left msg) (Right . SlotNo . fst) $ lastBlockOf era
          where
            msg = "Era " <> show era <> " hasn't ended yet. To index up until the current tip and then drop off, use @. To index everything and keep indexing then don't specify interval end."

    -- Parse Interval{upTo}
    parseUpTo :: C.SlotNo -> String -> Either String UpTo
    parseUpTo fromSlotNo str = case str of
      "" -> Right Infinity
      '-' : absoluteUpTo -> case absoluteUpTo of
        "@" -> Right CurrentTip
        ""  -> Right Infinity
        _ | all isDigit absoluteUpTo -> SlotNo <$> parseSlotNo_ absoluteUpTo
          | otherwise -> upToEra absoluteUpTo
      '+' : relativeUpTo
        | not (null digits) && null rest -> do
            toSlotNoNatural <- toSlotNoNaturalEither
            SlotNo <$> naturalSlotNo (fromSlotNoNatural + toSlotNoNatural)
        | otherwise -> Left "Can't parse relativeUpTo"
        where
          (digits, rest) = L.span isDigit relativeUpTo
          toSlotNoNaturalEither = Read.readEither digits :: Either String Natural
          fromSlotNoNatural = fromIntegral @_ @Natural $ coerce @_ @Word64 fromSlotNo
          naturalSlotNo :: Natural -> Either String C.SlotNo
          naturalSlotNo slotNo = if slotNo > fromIntegral (maxBound :: Word64)
            then Left $ "Slot number can't be larger than Word64 maxBound: " <> show slotNo
            else Right $ C.SlotNo (fromIntegral slotNo :: Word64)
      _ -> leftError "Can't read slot interval end" str

commonIntervalInfo :: O.Parser (Interval, Maybe DbPathAndTableName)
commonIntervalInfo = (,) <$> commonInterval <*> commonHeaderDb

commonHeaderDb :: O.Parser (Maybe DbPathAndTableName)
commonHeaderDb = O.option
  (O.eitherReader $ fmap Just . parseDbPathAndTableName)
  (O.value Nothing <> longOpt "header-db" "Optional path to sqlite database for block headers.")

commonLogging :: O.Parser Bool
commonLogging = O.option O.auto (opt 'q' "quiet" "Don't do any logging" <> O.value True)

commonProfilingConfig :: O.Parser (Maybe ProfilingConfig)
commonProfilingConfig = commonProfileSingleField <|> ensureAtLeastOneField <$> multiFieldPrim
  where
    multiFieldPrim :: O.Parser (Maybe Natural, Maybe FilePath, Maybe String)
    multiFieldPrim = (,,)
      <$> O.option (O.maybeReader $ Just . Read.readMaybe) (longOpt "profile-sample-rate" sampleRateLong <> O.value Nothing)
      <*> O.option (O.maybeReader $ Just . Just) (longOpt "profile-outfile" outfileLong <> O.value Nothing)
      <*> O.option (O.maybeReader $ Just . Just) (longOpt "profile-comment" commentLong <> O.value Nothing)

    ensureAtLeastOneField :: (Maybe Natural, Maybe FilePath, Maybe String) -> Maybe ProfilingConfig
    ensureAtLeastOneField (a, b, c) = if
      | Just sampleRate <- a -> Just $ ProfilingConfig sampleRate             (defaultOutfileF b) (defaultCommentF c)
      | Just outfile    <- b -> Just $ ProfilingConfig (defaultSampleRateF a) outfile             (defaultCommentF c)
      | Just comment    <- c -> Just $ ProfilingConfig (defaultSampleRateF a) (defaultOutfileF b) comment
      | otherwise            -> Nothing

    defaultSampleRate  = 10
    defaultSampleRateF = fromMaybe defaultSampleRate
    defaultOutfile     = "profile.log"
    defaultOutfileF    = fromMaybe defaultOutfile
    defaultComment     = ""
    defaultCommentF    = fromMaybe defaultComment

    defaultEmptyOutfile str = case str of
      [] -> defaultOutfile
      _ -> str

    sampleRateLong = "Sample system resource use every n seconds."
    outfileLong = "Destination file for profiling info."
    commentLong = "Comment for profiling info."

    commonProfileSingleField :: O.Parser (Maybe ProfilingConfig)
    commonProfileSingleField = O.option (O.eitherReader parseProfile)
      $ longOpt "profile" sampleRateLong
      <> O.value Nothing
      <> O.metavar "SECONDS:FILE:COMMENT"
      where
      parseProfile :: String -> Either String (Maybe ProfilingConfig)
      parseProfile str = let
        secondsStr = L.takeWhile (/= ':') str
        parts = mapMaybe (\case ':' : rest -> Just $ takeWhile (/= ':') rest; _ -> Nothing) $  L.tails str
        in do
        seconds <- Read.readEither secondsStr
        (outfile, comment) <- case parts of
          outfile : comments@(_ : _) -> return (defaultEmptyOutfile outfile, L.intercalate ":" comments)
          outfile : [] -> return (defaultEmptyOutfile outfile, defaultComment)
          [] -> return (defaultOutfile, defaultComment)
        return $ Just $ ProfilingConfig seconds outfile comment

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
  <*> commonIntervalInfo
  <*> commonLogging
  <*> commonProfilingConfig
  <*> commonPipelineSize
  <*> commonConcurrencyPrimitive

commonAddress :: O.Parser (C.Address C.ShelleyAddr)
commonAddress = O.option (O.eitherReader (deserializeToCardano' . TS.pack)) $ opt 'a' "address" "Cardano Shelly address"
  where
    deserializeToCardano = C.deserialiseFromBech32 (C.proxyToAsType Proxy)
    deserializeToCardano' = either (Left . show) Right . deserializeToCardano

commonUtxoState :: O.Parser FilePath
commonUtxoState = O.option O.auto (O.long "utxo-state" <> O.value "utxoState")

commonLogSeverity :: O.Parser CM.Severity
commonLogSeverity = let
  (parseSeverity, listAsText) = boundedEnum "severity"
  in O.option (O.eitherReader parseSeverity)
     $  O.long "log-severity"
     <> O.help ("Log messages up until specified severity: " <> listAsText)
     <> O.value CM.Notice

-- * String parsers

-- ** Interval

parseFrom :: String -> Either String (Bool, Either C.SlotNo C.ChainPoint)
parseFrom str = case str of
  "" -> Right (False, Right C.ChainPointAtGenesis)
  "0" -> Right (False, Right C.ChainPointAtGenesis)
  '@' : str' -> (True, ) <$> parseSlotNoOrChainPoint str'
  str' -> (False, ) <$> parseSlotNoOrChainPoint str'

parseSlotNoOrChainPoint :: String -> Either String (Either C.SlotNo C.ChainPoint)
parseSlotNoOrChainPoint str = do
  slotNo <- parseSlotNo_ fromSlotNo
  case bhh of
    [] -> return $ Left slotNo
    ':' : bhh' -> do
      blockHeaderHash <- eitherParseHashBlockHeader bhh'
      pure $ Right $ C.ChainPoint slotNo blockHeaderHash
    _ -> error "!! This is impossible"
  where (fromSlotNo, bhh) = L.span (/= ':') str

-- * Block channel

commonConcurrencyPrimitive :: O.Parser ConcurrencyPrimitive
commonConcurrencyPrimitive = O.option (O.eitherReader $ parse) $
  O.long "concurrency-primitive"
    <> O.help helpText
    <> O.hidden
    <> O.value MVar
  where
    helpText :: String
    helpText =
        "Choose between concurrency primitives for passing blocks from local\
        \ chainsync thread to the indexer. The choice currently is: "
      <> listAsText

    (parse, listAsText) = boundedEnum @ConcurrencyPrimitive "concurrency primitive"

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

-- | Take program description, header and CLI parser, and turn it into a ParserInfo
parserToParserInfo :: String -> String -> O.Parser a -> O.ParserInfo a
parserToParserInfo progDescr header cli = O.info (O.helper <*> cli) $ O.fullDesc
  <> O.progDesc progDescr
  <> O.header header

boundedEnum :: forall a . (Bounded a, Enum a, Show a) => String -> (String -> Either String a, String)
boundedEnum textDescr = (doLookup, listAsText)
  where
    values :: [a]
    values = [minBound .. maxBound]

    mapping :: [(String, a)]
    mapping = map (\s -> (map toLower $ show s, s)) values

    doLookup :: String -> Either String a
    doLookup str = case L.lookup str mapping of
      Just s -> Right s
      Nothing -> Left $ "unknown " <> textDescr <> " '" <> str <> "', must be one of " <> listAsText

    listAsText :: String
    listAsText = L.intercalate ", " (map fst mapping)
