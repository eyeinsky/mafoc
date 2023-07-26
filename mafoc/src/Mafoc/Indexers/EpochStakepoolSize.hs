{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Mafoc.Indexers.EpochStakepoolSize where

import Data.Coerce (coerce)
import Data.Function (on)
import Data.List (intercalate, sortBy)
import Data.Map.Strict qualified as M
import Data.Maybe (mapMaybe)
import Data.String (fromString)
import Data.Text qualified as TS
import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt
import System.Directory (listDirectory, removeFile)

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Config qualified as O
import Ouroboros.Consensus.Ledger.Abstract qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Cardano.Streaming qualified as CS
import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, checkpoint, initialize, persistMany, toEvent),
                   LocalChainsyncConfig (batchSize_, concurrencyPrimitive_, interval_, logging_, nodeInfo, pipelineSize_),
                   LocalChainsyncRuntime (LocalChainsyncRuntime), NodeConfig, defaultTableName,
                   nodeFolderToConfigPath, nodeInfoSocketPath, from, sqliteOpen, SlotNoBhh, traceInfo
                  )
import Mafoc.Upstream (getNetworkId, querySecurityParam)
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data EpochStakepoolSize = EpochStakepoolSize
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

parseCli :: Opt.ParserInfo EpochStakepoolSize
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "epochstakepoolsize"
  <> Opt.header "epochstakepoolsize - Index stakepool sizes in absolute ADA at every epoch"
  where
    cli :: Opt.Parser EpochStakepoolSize
    cli = EpochStakepoolSize
      <$> Opt.mkCommonLocalChainsyncConfig Opt.commonNodeConnectionAndConfig
      <*> Opt.commonDbPathAndTableName

data EpochStakepoolSizeEvent = EpochStakepoolSizeEvent
  { epochNo  :: C.EpochNo
  , stakeMap :: M.Map C.PoolId C.Lovelace
  }

instance Indexer EpochStakepoolSize where

  type Event EpochStakepoolSize = EpochStakepoolSizeEvent

  data Runtime EpochStakepoolSize = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , ledgerCfg     :: O.LedgerCfg (O.ExtLedgerState (O.HardForkBlock (O.CardanoEras O.StandardCrypto)))
    }
  data State EpochStakepoolSize = State
    { extLedgerState :: O.ExtLedgerState (O.HardForkBlock (O.CardanoEras O.StandardCrypto))
    , maybeEpochNo   :: Maybe C.EpochNo
    }

  toEvent (Runtime{ledgerCfg}) state blockInMode = return (State newExtLedgerState maybeCurrentEpochNo, maybeEvent)
    where
    newExtLedgerState = Marconi.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeCurrentEpochNo = Marconi.getEpochNo newExtLedgerState
    stakeMap = Marconi.getStakeMap newExtLedgerState
    maybeEvent :: Maybe EpochStakepoolSizeEvent
    maybeEvent = case maybeEpochNo state of
      Just previousEpochNo -> case maybeCurrentEpochNo of
        -- Epoch number increases: it is epoch boundary so emit an event
        Just currentEpochNo -> let epochDiff = currentEpochNo - previousEpochNo
          in case epochDiff of
               -- Epoch increased, emit an event
               1 -> Just $ EpochStakepoolSizeEvent currentEpochNo stakeMap
               -- Epoch remained the same, don't emit an event
               0 -> Nothing
               _ -> error $ "EpochStakepoolSize indexer: assumption violated: epoch changed by " <> show epochDiff <> " instead of expected 0 or 1."
        _ -> error $ "EpochStakepoolSize indexer: assumption violated: there was a previous epoch no, but there is none now — this can't be!"
        -- todo: Replace the errors above with specific exceptions
      _ -> case maybeCurrentEpochNo of
        -- There was no previous epoch no (= it was Byron era) but
        -- there is one now: emit an event as this started a new
        -- epoch.
        Just currentEpochNo -> Just $ EpochStakepoolSizeEvent currentEpochNo stakeMap
        -- No previous epoch no and no current epoch no, the Byron
        -- era continues.
        _                   -> Nothing

  initialize EpochStakepoolSize{chainsyncConfig, dbPathAndTableName} trace = do
    let nodeInfo' = nodeInfo chainsyncConfig
        nodeConfig = case nodeInfo' of
          Left nodeFolder                  -> nodeFolderToConfigPath nodeFolder
          Right (_socketPath, nodeConfig') -> nodeConfig'
        socketPath = nodeInfoSocketPath nodeInfo'

    networkId <- getNetworkId nodeConfig
    let lnc = CS.mkLocalNodeConnectInfo networkId socketPath
    securityParam' <- querySecurityParam lnc
    let (dbPath, tableName) = defaultTableName "stakepool_delegation" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName

    (ledgerConfig, extLedgerState, startFrom) <- listExtLedgerStates "." >>= \case
      -- A ledger state exists on disk, resume from there
      (fn, (slotNo, bhh)) : _ -> do
        cfg <- Marconi.getLedgerConfig nodeConfig
        let O.ExtLedgerCfg topLevelConfig = cfg
        extLedgerState <- Marconi.loadExtLedgerState (O.configCodec topLevelConfig) fn >>= \case
          Right (_, extLedgerState) -> return extLedgerState
          Left msg                  -> error $ "Error while deserialising file " <> fn <> ", error: " <> show msg
        let cp = C.ChainPoint slotNo bhh
        traceInfo trace $ "Found on-disk ledger state, resuming from: " <> show cp
        return (cfg, extLedgerState, cp)
      -- No existing ledger states, start from the beginning
      [] -> do
        (cfg, st) <- Marconi.getInitialExtLedgerState nodeConfig
        traceInfo trace "No on-disk ledger state found, resuming from genesis"
        return (cfg, st, C.ChainPointAtGenesis)

    let chainsyncRuntime' = LocalChainsyncRuntime
          lnc
          ((interval_ chainsyncConfig) {from = startFrom})
          securityParam'
          (logging_ chainsyncConfig)
          (pipelineSize_ chainsyncConfig)
          (batchSize_ chainsyncConfig)
          (concurrencyPrimitive_ chainsyncConfig)

    return ( State extLedgerState $ Marconi.getEpochNo extLedgerState
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName events

  checkpoint Runtime{ledgerCfg, sqlConnection, tableName} State{extLedgerState} slotNoBhh = do
    let O.ExtLedgerCfg topLevelConfig = ledgerCfg
    putStrLn $ "Write ledger state"
    Marconi.writeExtLedgerState (bhhToFileName slotNoBhh) (O.configCodec topLevelConfig) extLedgerState
    putStrLn $ "Wrote ledger state"
    mapM_ (removeFile . bhhToFileName) . drop 2 . map snd =<< listExtLedgerStates "."
    putStrLn $ "Removed other files"

-- * Ledger state checkpoint

bhhToFileName :: SlotNoBhh -> FilePath
bhhToFileName (slotNo, blockHeaderHash) = intercalate "_"
  [ "ledgerState"
  , show slotNo'
  , TS.unpack (C.serialiseToRawBytesHexText blockHeaderHash)
  ]
  where
    slotNo' = coerce slotNo :: Word64

bhhFromFileName :: String -> Either String SlotNoBhh
bhhFromFileName str = case splitOn '_' str of
  _ : slotNoStr : blockHeaderHashHex : _ -> (,)
    <$> Opt.parseSlotNo_ slotNoStr
    <*> Opt.eitherParseHashBlockHeader_ blockHeaderHashHex
  _ -> Left "Can't parse ledger state file name, must be <slot no> _ <block header hash>"
  where

splitOn :: Eq a => a -> [a] -> [[a]]
splitOn x xs = case span (/= x) xs of
  (prefix, _x : rest) -> prefix : recurse rest
  (lastChunk, [])     -> [lastChunk]
  where
    recurse = splitOn x

listExtLedgerStates :: FilePath -> IO [(FilePath, SlotNoBhh)]
listExtLedgerStates dirPath = sortBy (flip compare `on` snd) . mapMaybe parse <$> listDirectory dirPath
  where
    parse :: FilePath -> Maybe (FilePath, SlotNoBhh)
    parse fn = either (const Nothing) Just . fmap (fn,) $ bhhFromFileName fn

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " \
  \   ( pool_id   BLOB NOT NULL  \
  \   , lovelace  INT NOT NULL   \
  \   , epoch_no  INT NOT NULL   )"

sqliteInsert :: SQL.Connection -> String -> [EpochStakepoolSizeEvent] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c
  ("INSERT INTO " <> fromString tableName <>" (epoch_no, pool_id, lovelace) VALUES (?, ?, ?)")
  (toRows =<< events)
  where
   toRows :: EpochStakepoolSizeEvent -> [(C.EpochNo, C.PoolId, C.Lovelace)]
   toRows (EpochStakepoolSizeEvent epochNo m) = map (\(keyHash, lovelace) -> (epochNo, keyHash, lovelace)) $ M.toList m
