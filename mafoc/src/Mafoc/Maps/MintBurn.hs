{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}

{- | Indexer for mint and burn events.

The implementation for converting blocks to events and persisting
these into sqlite is outsourced from marconi.

This just provides the CLI interface and a streaming runtime.

-}

module Mafoc.Maps.MintBurn where

import Control.Monad.IO.Class (liftIO)
import Data.Function ((&))
import Database.SQLite.Simple qualified as SQL
import Mafoc.CLI qualified as Opt
import Numeric.Natural (Natural)
import Options.Applicative qualified as Opt
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as C
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Indexers.MintBurn qualified as Marconi.MintBurn

import Mafoc.Helpers (Interval (Interval), findIntervalToBeIndexed, fromChainSyncEvent, getSecurityParam, loopM,
                      takeUpTo)
import Mafoc.Indexer.Class (Indexer (Runtime, initialize, run))
import Mafoc.RollbackRingBuffer (rollbackRingBuffer)

streamer :: SQL.Connection -> C.LocalNodeConnectInfo C.CardanoMode -> Interval -> Natural -> S.Stream (S.Of Marconi.MintBurn.TxMintEvent) IO ()
streamer sqlCon lnCon (Interval from upTo) k = C.blocks lnCon from
  & fromChainSyncEvent
  & rollbackRingBuffer k
  & takeUpTo upTo
  & S.mapMaybe Marconi.MintBurn.toUpdate
  & \source -> do
      liftIO (Marconi.MintBurn.sqliteInit sqlCon)
      loopM source $ \event -> do
        liftIO $ Marconi.MintBurn.sqliteInsert sqlCon [event]
        S.yield event

-- | Configuration data type which does double-duty as the tag for the
-- indexer.
data MintBurn = MintBurn
  { dbPath                    :: FilePath
  , socketPath                :: String
  , networkId                 :: C.NetworkId
  , interval                  :: Interval
  , securityParamOrNodeConfig :: Either Natural FilePath
  } deriving (Show)

parseCli :: Opt.ParserInfo MintBurn
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "mintburn"
  <> Opt.header "mintburn - Index mint and burn events"
  where
    cli :: Opt.Parser MintBurn
    cli = MintBurn
      <$> Opt.commonDbPath
      <*> Opt.commonSocketPath
      <*> Opt.commonNetworkId
      <*> Opt.commonInterval
      <*> Opt.commonSecurityParamEither

instance Indexer MintBurn where

  data Runtime MintBurn = Runtime
    { sqlConnection       :: SQL.Connection
    , interval_           :: Interval
    , localNodeConnection :: C.LocalNodeConnectInfo C.CardanoMode
    , securityParam       :: Natural
    , cliConfig           :: MintBurn
    }

  initialize config = do
    c <- SQL.open $ dbPath config
    Marconi.MintBurn.sqliteInit c
    interval' <- findIntervalToBeIndexed (interval config) c "mintburn"
    let localNodeCon = CS.mkLocalNodeConnectInfo (networkId config) (socketPath config)
    k <- either pure getSecurityParam $ securityParamOrNodeConfig config
    return (Runtime c interval' localNodeCon k config)

  run (Runtime{sqlConnection, localNodeConnection, interval_, securityParam}) =
    S.effects $ streamer sqlConnection localNodeConnection interval_ securityParam
