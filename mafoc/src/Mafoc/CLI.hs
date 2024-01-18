module Mafoc.CLI
  ( module Mafoc.CLI
  , parseSlotNo_
  ) where

import Data.Time (NominalDiffTime)
import Data.List qualified as L
import Data.Text qualified as TS
import Data.Text.Encoding qualified as TS
import Options.Applicative qualified as O
import Text.Read qualified as Read
import Data.List.NonEmpty qualified as NE

import Cardano.BM.Data.Severity qualified as CM
import Cardano.Api qualified as C
import Mafoc.Core (BatchSize, ConcurrencyPrimitive, DbPathAndTableName (DbPathAndTableName), Interval (Interval),
                   LocalChainsyncConfig (LocalChainsyncConfig), LocalChainsyncConfig_, NodeConfig,
                   NodeFolder, NodeInfo (NodeInfo), SocketPath,
                   UpTo (CurrentTip, Infinity, SlotNo), CheckpointInterval(Every, Never),
                   SecurityParam, ParallelismConfig(ParallelismConfig, intervalLength, maybeMaxThreads), IntervalLength(Slots, Percent), defaultParallelism, defaultBatchSize, defaultSeverity, defaultCheckpointInterval, defaultPipelineSize, defaultConcurrencyPrimitive, NodeInfo_)
import Mafoc.Upstream (LedgerEra, lastChainPointOfPreviousEra, lastBlockOf)
import Mafoc.Logging (ProfilingConfig(ProfilingConfig))
import Mafoc.StateFile (eitherParseHashBlockHeader, leftError, parseSlotNo_)
import Mafoc.MultiAsset (AssetFingerprint, AsType(AsAssetFingerprint))

-- * Options

commonSocketPath :: O.Parser SocketPath
commonSocketPath = O.strOption (opt 's' "socket-path" "Path to node socket.")

commonDbPath :: O.Parser FilePath
commonDbPath = O.strOption (opt 'd' "db-path" "Path to sqlite database.")

commonDbPathAndTableName :: O.Parser DbPathAndTableName
commonDbPathAndTableName = O.option (O.eitherReader parseDbPathAndTableName) $ opt 'd' "db"
  "Optional path to sqlite database (defaults to default.db) and a table name (default is indexer-specific)."
  <> O.value (DbPathAndTableName Nothing Nothing)

commonAssetFingerprint :: O.Parser AssetFingerprint
commonAssetFingerprint = assetFingerprint $ O.long "fingerprint"

assetFingerprint :: O.Mod O.OptionFields AssetFingerprint -> O.Parser AssetFingerprint
assetFingerprint = O.option (O.eitherReader f)
  where
    f :: String -> Either String AssetFingerprint
    f = either (Left . C.displayError) Right . C.deserialiseFromBech32 AsAssetFingerprint . TS.pack

parseDbPathAndTableName :: String -> Either String DbPathAndTableName
parseDbPathAndTableName str = let
      (dbPath, tableName) = L.span (/= ':') str
      dbPath' = if L.null dbPath then Nothing else Just dbPath
      tableName' = case tableName of
        ':' : rest@(_:_) -> Just rest
        _                -> Nothing
      in Right $ DbPathAndTableName dbPath' tableName'

commonNodeConfig :: O.Parser NodeConfig
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
  <> O.value defaultPipelineSize

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

commonSecurityParam :: O.Parser SecurityParam
commonSecurityParam = O.option O.auto (opt 'k' "security-param" "Security parameter -- number of slots after which they can't be rolled back")

commonSecurityParamEither :: O.Parser (Either C.NetworkId NodeConfig)
commonSecurityParamEither = Left <$> commonNetworkId <|> Right <$> commonNodeConfig

modNodeFolder :: O.HasMetavar f => O.Mod f NodeFolder
modNodeFolder =
     O.metavar "NODE-FOLDER"
  <> O.help "Path to cardano-node's folder. The program will figure out socket path, security parameter, network and node config path from it."

commonNodeConnection :: O.Parser (NodeInfo (Either C.NetworkId NodeConfig))
commonNodeConnection = NodeInfo
  <$> (    Left <$> O.strArgument modNodeFolder
       <|> Right <$> ((,) <$> commonSocketPath <*> commonSecurityParamEither)
      )

commonNodeInfoOption :: O.Parser NodeInfo_
commonNodeInfoOption = NodeInfo <$> nodeFolderOrSocketAndConfig
  where
    nodeFolderOrSocketAndConfig :: O.Parser (Either NodeFolder (SocketPath, Either C.NetworkId NodeConfig))
    nodeFolderOrSocketAndConfig =
          Left <$> O.strOption (O.long "node-folder" <> modNodeFolder)
      <|> Right <$> ((,) <$> commonSocketPath <*> commonSecurityParamEither)

commonNodeConnectionAndConfig :: O.Parser (NodeInfo NodeConfig)
commonNodeConnectionAndConfig = NodeInfo
  <$> (   Left <$> O.strArgument modNodeFolder
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
commonIntervalInfo = (,) <$> commonInterval <*> commonMaybeHeaderDb

commonMaybeHeaderDb :: O.Parser (Maybe DbPathAndTableName)
commonMaybeHeaderDb = O.option
  (O.eitherReader $ fmap Just . parseDbPathAndTableName)
  (O.value Nothing <> longOpt "header-db" "Optional path to sqlite database for block headers.")

commonHeaderDb :: O.Parser DbPathAndTableName
commonHeaderDb = O.option
  (O.eitherReader parseDbPathAndTableName)
  (longOpt "header-db" "Optional path to sqlite database for block headers.")

commonLogging :: O.Parser Bool
commonLogging = O.option O.auto (opt 'q' "quiet" "Don't do any logging" <> O.value True)

commonProfilingConfig :: O.Parser (Maybe ProfilingConfig)
commonProfilingConfig = commonProfileSingleField <|> ensureAtLeastOneField <$> multiFieldPrim
  where
    multiFieldPrim :: O.Parser (Maybe NominalDiffTime, Maybe FilePath, Maybe String)
    multiFieldPrim = (,,)
      <$> O.option (O.eitherReader $ fmap Just . parseNominalDiffTime) (longOpt "profile-sample-rate" sampleRateLong <> O.value Nothing)
      <*> O.option (O.maybeReader $ Just . Just) (longOpt "profile-outfile" outfileLong <> O.value Nothing)
      <*> O.option (O.maybeReader $ Just . Just) (longOpt "profile-comment" commentLong <> O.value Nothing)

    ensureAtLeastOneField :: (Maybe NominalDiffTime, Maybe FilePath, Maybe String) -> Maybe ProfilingConfig
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
  <> O.value defaultBatchSize

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

stateFilePrefix :: FilePath -> O.Parser FilePath
stateFilePrefix default_ = O.option O.auto
  $ O.long "state-file-prefix"
  <> O.value default_
  <> O.help "Prefix used for indexer's sate files."

commonLogSeverity :: O.Parser CM.Severity
commonLogSeverity = let
  (parseSeverity, listAsText) = boundedEnum "severity"
  in O.option (O.eitherReader parseSeverity)
     $  O.long "log-severity"
     <> O.help ("Log messages up until specified severity: " <> listAsText)
     <> O.value defaultSeverity

commonMaybeAssetId :: O.Parser (Maybe (C.PolicyId, Maybe C.AssetName))
commonMaybeAssetId = O.option
  (O.eitherReader $ fmap Just . parseAssetId)
  (longOpt "asset-id" "Either ${policyId}.${assetName} or just ${policyId}" <> O.value Nothing)

commonAssetId :: O.Parser (C.PolicyId, Maybe C.AssetName)
commonAssetId = O.option
  (O.eitherReader parseAssetId)
  (longOpt "asset-id" "Either ${policyId}.${assetName} or just ${policyId}")

parseAssetId :: String -> Either String (C.PolicyId, Maybe C.AssetName)
parseAssetId str = case span (/= '.') str of
  (policyIdStr, []) -> (,Nothing) <$> parsePolicyId policyIdStr
  (policyIdStr, assetNameStr@(_ : _)) -> do
    policyId <- parsePolicyId policyIdStr
    assetName <- parseAssetName assetNameStr
    return (policyId, Just assetName)
  where
    parsePolicyId :: String -> Either String C.PolicyId
    parsePolicyId = first show . C.deserialiseFromRawBytesHex C.AsPolicyId . TS.encodeUtf8 . TS.pack
    parseAssetName :: String -> Either String C.AssetName
    parseAssetName = first show . C.deserialiseFromRawBytesHex C.AsAssetName . TS.encodeUtf8 . TS.pack

commonRunHttpApi :: O.Parser (Maybe Int)
commonRunHttpApi = O.option (Just <$> O.eitherReader (readEither @Int)) $
  longOpt "http-api" "Run HTTP API if the indexer has one."
  <> O.metavar "PORT"
  <> O.value Nothing

commonCheckpointInterval :: O.Parser CheckpointInterval
commonCheckpointInterval = O.option (O.eitherReader parse) $
  longOpt "checkpoint-interval" "Maximum checkpoint interval in seconds (default), [m]inutes, [h]ours or [d]ays."
  <> O.metavar "DECIMAL"
  <> O.value defaultCheckpointInterval
  where
    parse :: String -> Either String CheckpointInterval
    parse str = case str of
      "never" -> Right Never
      _ ->  (\n -> if n == 0 then Never else Every n ) <$> parseNominalDiffTime str

commonIgnoreMissingUtxos :: O.Parser Bool
commonIgnoreMissingUtxos = O.switch (longOpt "ignore-missing-utxos" desc <> O.internal)
  where
    desc =
      "Ignore missing transaction outputs. When processing spends then default is to throw \
      \an exception when transaction input is not found from the UTxO set. This is useful \
      \for benchmarking and debugging, when chain is traversed from the middle and earlier \
      \transaction outputs are not known."

commonParallelismConfig :: O.Parser ParallelismConfig
commonParallelismConfig = ParallelismConfig <$> intervalLength' <*> maybeMaxThreads'
  where
    intervalLength' = O.option (O.eitherReader parseIntervalLength)
      $ longOpt "interval-length" "How many slots should an interval processed in parallel be"
      <> O.metavar "SLOT-NO"
      <> O.value (intervalLength defaultParallelism)
    maybeMaxThreads' = O.option (O.eitherReader $ fmap Just . readEither @Natural)
      $ longOpt "max-threads" "How many threads should be running at once"
      <> O.value (maybeMaxThreads defaultParallelism)

testParserArgs :: O.Parser a -> [String] -> IO a
testParserArgs cli args = O.execParserPure O.defaultPrefs pinfo args & O.handleParseResult
  where
    pinfo = parserToParserInfo "testProg" "testHeader" cli

testParser :: O.Parser a -> String -> IO a
testParser cli str = testParserArgs cli $ words str

-- * String parsers

parseIntervalLength :: String -> Either String IntervalLength
parseIntervalLength str = (Percent <$> parsePercent str) <|> (Slots <$> readEither str)

parsePercent :: String -> Either String Scientific
parsePercent str = case reverse str of
  '%' : nat -> readEither $ reverse nat
  _ -> Left "Percent value should have \"%\" at the end"

-- ** NominalDiffTime

parseNominalDiffTime :: String -> Either String NominalDiffTime
parseNominalDiffTime str = case NE.nonEmpty str of
  Nothing -> Left "empty string"
  Just str' -> let (digits, suffix) = NE.span (\c -> isDigit c || c == '.') str'
    in case NE.nonEmpty digits of
      Nothing -> Left $ "must be number with a time unit: " <> unitsHelp
      Just digits' -> let
        unitMultiplier :: Either String NominalDiffTime
        unitMultiplier = case suffix of
          ""  -> Right 1
          "s" -> Right 1
          "m" -> Right 60
          "h" -> Right $ 60 * 60
          "d" -> Right $ 24 * 60 * 60
          _ -> Left $ "unknown time unit, supported units are: " <> unitsHelp
        scalar = realToFrac <$> readEither @Double (NE.toList digits')
        in (*) <$> scalar <*> unitMultiplier
  where
    unitsHelp = "s(econd), m(inute), h(our), d(ay)"

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
    <> O.value defaultConcurrencyPrimitive
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

opt :: O.HasName f => Char -> String -> String -> O.Mod f a
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
    mapping :: [(String, a)]
    mapping = boundedEnumMapping

    doLookup :: String -> Either String a
    doLookup str = case L.lookup str mapping of
      Just s -> Right s
      Nothing -> Left $ "unknown " <> textDescr <> " '" <> str <> "', must be one of " <> listAsText

    listAsText :: String
    listAsText = L.intercalate ", " (map fst mapping)

boundedEnumMapping :: (Bounded a, Enum a, Show a) => [(String, a)]
boundedEnumMapping = map (\s -> (map toLower $ show s, s)) boundedEnumAll
  where
    boundedEnumAll :: (Bounded a, Enum a) => [a]
    boundedEnumAll = [minBound .. maxBound]
