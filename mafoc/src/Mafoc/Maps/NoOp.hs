module Mafoc.Maps.NoOp where

import Data.Word (Word32)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Mafoc.CLI qualified as Opt
import Mafoc.Core (BlockSourceConfig (BlockSourceConfig), DbPathAndTableName,
                   Indexer (Event, Runtime, State, initialize, persist, toEvent), Interval, defaultTableName,
                   findIntervalToBeIndexed, getSecurityParam, sqliteInitBookmarks)

data NoOp = NoOp
  { chunkSize                 :: Int
  , socketPath                :: String
  , dbPathAndTableName        :: DbPathAndTableName
  , securityParamOrNodeConfig :: Either Natural FilePath
  , interval                  :: Interval
  , networkId                 :: C.NetworkId
  , logging                   :: Bool
  , pipelineSize              :: Word32
  } deriving Show

parseCli :: Opt.ParserInfo NoOp
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "noop"
  <> Opt.header "noop - Index nothing, just drain blocks over local chainsync protocol"
  where
    cli :: Opt.Parser NoOp
    cli = NoOp
      <$> Opt.commonChunkSize
      <*> Opt.commonSocketPath
      <*> Opt.commonDbPathAndTableName
      <*> Opt.commonSecurityParamEither
      <*> Opt.commonInterval
      <*> Opt.commonNetworkId
      <*> Opt.commonQuiet
      <*> Opt.commonPipelineSize

instance Indexer NoOp where
  type Event NoOp = ()
  data State NoOp = EmptyState
  data Runtime NoOp = Runtime
  toEvent _blk _state = Just (EmptyState, ())
  initialize config = do
    let (dbPath, tableName) = defaultTableName "noop" $ dbPathAndTableName config
    c <- SQL.open dbPath
    -- sqliteInit c tableName
    sqliteInitBookmarks c
    interval' <- findIntervalToBeIndexed (interval config) c tableName
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (EmptyState, BlockSourceConfig localNodeCon interval' k (logging config) (pipelineSize config), Runtime)
  persist _runtime _event = pure ()
