{-# OPTIONS_GHC -fno-warn-orphans #-}
module Mafoc.Indexers.EpochStakepoolSize where

import Control.Exception (throw)
import Data.Map.Strict qualified as M
import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig,
                   loadLatestTrace, sqliteOpen, defaultTableName, initializeLocalChainsync)
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.LedgerState qualified as LedgerState

data EpochStakepoolSize = EpochStakepoolSize
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

data EpochStakepoolSizeEvent = EpochStakepoolSizeEvent

instance Indexer EpochStakepoolSize where

  description = "Index stakepool sizes (in ADA) per epoch"

  parseCli = EpochStakepoolSize
    <$> Opt.mkCommonLocalChainsyncConfig Opt.commonNodeConnectionAndConfig
    <*> Opt.commonDbPathAndTableName

  data Event EpochStakepoolSize = Event
    { epochNo  :: C.EpochNo
    , stakeMap :: M.Map C.PoolId C.Lovelace
    }

  data Runtime EpochStakepoolSize = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , ledgerCfg     :: LedgerState.ExtLedgerCfg_
    }
  data State EpochStakepoolSize = State
    { extLedgerState :: LedgerState.ExtLedgerState_
    , maybeEpochNo   :: Maybe C.EpochNo
    }

  toEvents Runtime{ledgerCfg} state blockInMode = (State newExtLedgerState maybeCurrentEpochNo, coerce maybeEvent)
    where
    newExtLedgerState = LedgerState.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeCurrentEpochNo = LedgerState.getEpochNo newExtLedgerState
    stakeMap = LedgerState.getStakeMap newExtLedgerState
    maybeEvent :: [Event EpochStakepoolSize]
    maybeEvent = case EpochResolution.resolve (maybeEpochNo state) maybeCurrentEpochNo of
      EpochResolution.New epochNo -> [Event epochNo stakeMap]
      EpochResolution.SameOrAbsent -> []
      EpochResolution.AssumptionBroken exception -> throw exception

  initialize EpochStakepoolSize{chainsyncConfig, dbPathAndTableName} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
    networkId <- #getNetworkId nodeConfig
    chainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace
    let (dbPath, tableName) = defaultTableName "stakepool_delegation" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName
    ((ledgerConfig, extLedgerState), stateChainPoint) <- loadLatestTrace "ledgerState" (LedgerState.init_ nodeConfig) (LedgerState.load nodeConfig) trace
    return ( State extLedgerState (LedgerState.getEpochNo extLedgerState)
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{ledgerCfg} State{extLedgerState} slotNoBhh = void $ LedgerState.store "ledgerState" slotNoBhh ledgerCfg extLedgerState

-- * Sqlite

sqliteInit :: SQL.Connection -> String -> IO ()
sqliteInit c tableName = SQL.execute_ c $
  " CREATE TABLE IF NOT EXISTS " <> fromString tableName <> " \
  \   ( pool_id   BLOB NOT NULL  \
  \   , lovelace  INT NOT NULL   \
  \   , epoch_no  INT NOT NULL   )"

sqliteInsert :: SQL.Connection -> String -> [Event EpochStakepoolSize] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c
  ("INSERT INTO " <> fromString tableName <>" (epoch_no, pool_id, lovelace) VALUES (?, ?, ?)")
  (toRows =<< events)
  where
   toRows :: Event EpochStakepoolSize -> [(C.EpochNo, C.PoolId, C.Lovelace)]
   toRows (Event epochNo m) = map (\(keyHash, lovelace) -> (epochNo, keyHash, lovelace)) $ M.toList m
