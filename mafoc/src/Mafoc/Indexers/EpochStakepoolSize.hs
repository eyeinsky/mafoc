{-# OPTIONS_GHC -fno-warn-orphans #-}
module Mafoc.Indexers.EpochStakepoolSize where

import Control.Exception (throw)
import Data.Map.Strict qualified as M
import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, ensureStartFromCheckpoint,
                   loadLatestTrace, sqliteOpen, defaultTableName, initializeLocalChainsync, SqliteTable, query1)
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.LedgerState qualified as LedgerState
import Mafoc.Exceptions qualified as E


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
    newLedgerState = O.ledgerState newExtLedgerState
    maybeCurrentEpochNo = LedgerState.getEpochNo newLedgerState

    maybeEvent :: [Event EpochStakepoolSize]
    maybeEvent = case EpochResolution.resolve (maybeEpochNo state) maybeCurrentEpochNo of
      EpochResolution.New _epochNo -> case LedgerState.getEpochNoAndStakeMap newLedgerState of
        Just (epochNo, stakeMap) -> [Event epochNo stakeMap]
        Nothing -> throw $ E.TextException "!!! The impossible happened, (empty) stake map must exist on epoch boundary"
      EpochResolution.SameOrAbsent -> []
      EpochResolution.AssumptionBroken exception -> throw exception

  initialize cli@EpochStakepoolSize{chainsyncConfig, dbPathAndTableName} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
    networkId <- #getNetworkId nodeConfig
    chainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace $ show cli
    let (dbPath, tableName) = defaultTableName defaultTable dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInit sqlCon tableName
    ((ledgerConfig, extLedgerState), stateChainPoint) <- loadLatestTrace "ledgerState" (LedgerState.init_ nodeConfig) (LedgerState.load nodeConfig) trace
    chainsyncRuntime'' <- ensureStartFromCheckpoint chainsyncRuntime' stateChainPoint

    return ( State extLedgerState (LedgerState.getEpochNo $ O.ledgerState extLedgerState)
           , chainsyncRuntime''
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

defaultTable :: String
defaultTable = "stakepool_delegation"

queryEpochStakepoolSize :: C.EpochNo -> SqliteTable -> IO [(C.PoolId, C.Lovelace)]
queryEpochStakepoolSize epochNo (con, table) = query1 con query' epochNo
  where
    query' = "SELECT pool_id, lovelace FROM " <> fromString table <> " WHERE epoch_no = ?"

queryEpochStakepoolSizes :: SqliteTable -> IO [(C.EpochNo, C.PoolId, C.Lovelace)]
queryEpochStakepoolSizes (con, table) = SQL.query_ con query'
  where
    query' = "SELECT epoch_no, pool_id, lovelace FROM " <> fromString table
