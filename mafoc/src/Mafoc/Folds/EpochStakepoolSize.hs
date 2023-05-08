{-# LANGUAGE MultiWayIf        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Mafoc.Folds.EpochStakepoolSize where

import Control.Monad.IO.Class (liftIO)
import Data.Map.Strict qualified as M
import Data.String (fromString)
import Database.SQLite.Simple qualified as SQL
import Options.Applicative qualified as Opt

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Ouroboros.Consensus.Cardano.Block qualified as O
import Ouroboros.Consensus.Ledger.Abstract qualified as O
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, checkpoint, initialize, persistMany, toEvent),
                   LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, initializeSqlite)
import Marconi.ChainIndex.Indexers.EpochState qualified as Marconi


data EpochStakepoolSize = EpochStakepoolSize
  { chainsync          :: LocalChainsyncConfig
  , nodeConfig         :: FilePath
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

parseCli :: Opt.ParserInfo EpochStakepoolSize
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "epochstakepoolsize"
  <> Opt.header "epochstakepoolsize - Index stakepool sizes in absolute ADA at every epoch"
  where
    cli :: Opt.Parser EpochStakepoolSize
    cli = EpochStakepoolSize
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonNodeConfig
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


  initialize EpochStakepoolSize{chainsync, nodeConfig, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync chainsync
    let (dbPath, tableName) = defaultTableName "stakepool_delegation" dbPathAndTableName
    (sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName sqliteInit chainsyncRuntime trace
    (ledgerConfig, extLedgerState) <- liftIO $ Marconi.getInitialExtLedgerState nodeConfig
    let maybeEpochNo = Marconi.getEpochNo extLedgerState
    return (State extLedgerState maybeEpochNo, chainsyncRuntime', Runtime sqlCon tableName ledgerConfig)

  persistMany Runtime{sqlConnection, tableName} events = sqliteInsert sqlConnection tableName events

  checkpoint _runtime _slotNoBhh = return ()
  -- TODO: write ledger state, add row to checkpoint table; also load
  -- this ledger state in initialize.

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
