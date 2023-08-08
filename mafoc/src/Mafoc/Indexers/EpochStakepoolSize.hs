{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TupleSections     #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Mafoc.Indexers.EpochStakepoolSize where

import Data.Map.Strict qualified as M
import Data.String (fromString)
import Database.SQLite.Simple qualified as SQL
import Data.Coerce (coerce)

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, initializeLedgerStateAndDatabase,
                   storeLedgerState)
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi

data EpochStakepoolSize = EpochStakepoolSize
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

data EpochStakepoolSizeEvent = EpochStakepoolSizeEvent

instance Indexer EpochStakepoolSize where

  description = "Index stakepool sizes per epoch in absolute ADA"

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
    , ledgerCfg     :: Marconi.ExtLedgerCfg_
    }
  data State EpochStakepoolSize = State
    { extLedgerState :: Marconi.ExtLedgerState_
    , maybeEpochNo   :: Maybe C.EpochNo
    }

  toEvents Runtime{ledgerCfg} state blockInMode = (State newExtLedgerState maybeCurrentEpochNo, coerce maybeEvent)
    where
    newExtLedgerState = Marconi.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeCurrentEpochNo = Marconi.getEpochNo newExtLedgerState
    stakeMap = Marconi.getStakeMap newExtLedgerState
    maybeEvent :: [Event EpochStakepoolSize]
    maybeEvent = case maybeEpochNo state of
      Just previousEpochNo -> case maybeCurrentEpochNo of
        -- Epoch number increases: it is epoch boundary so emit an event
        Just currentEpochNo -> let epochDiff = currentEpochNo - previousEpochNo
          in case epochDiff of
               -- Epoch increased, emit an event
               1 -> [Event currentEpochNo stakeMap]
               -- Epoch remained the same, don't emit an event
               0 -> []
               _ -> error $ "EpochStakepoolSize indexer: assumption violated: epoch changed by " <> show epochDiff <> " instead of expected 0 or 1."
        _ -> error $ "EpochStakepoolSize indexer: assumption violated: there was a previous epoch no, but there is none now — this can't be!"
        -- todo: Replace the errors above with specific exceptions
      _ -> case maybeCurrentEpochNo of
        -- There was no previous epoch no (= it was Byron era) but
        -- there is one now: emit an event as this started a new
        -- epoch.
        Just currentEpochNo -> [Event currentEpochNo stakeMap]
        -- No previous epoch no and no current epoch no, the Byron
        -- era continues.
        _                   -> []

  initialize EpochStakepoolSize{chainsyncConfig, dbPathAndTableName} trace = do
    (extLedgerState, epochNo, chainsyncRuntime', sqlCon, tableName, ledgerConfig) <-
      initializeLedgerStateAndDatabase chainsyncConfig trace dbPathAndTableName sqliteInit "stakepool_delegation"
    return ( State extLedgerState epochNo
           , chainsyncRuntime'
           , Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName $ coerce events

  checkpoint Runtime{ledgerCfg} State{extLedgerState} slotNoBhh = storeLedgerState ledgerCfg slotNoBhh extLedgerState

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