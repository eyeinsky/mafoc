{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedLabels        #-}
{-# LANGUAGE NamedFieldPuns    #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Indexers.EpochNonce where

import Cardano.Api qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Control.Exception qualified as E
import Database.SQLite.Simple qualified as SQL

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, sqliteOpen, defaultTableName, initializeLocalChainsync,
                   loadLatestTrace
                  )
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.LedgerState qualified as LedgerState

data EpochNonce = EpochNonce
  { chainsyncConfig    :: LocalChainsyncConfig NodeConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer EpochNonce where

  description = "Index epoch numbers and epoch nonces"

  parseCli = EpochNonce
    <$> Opt.mkCommonLocalChainsyncConfig Opt.commonNodeConnectionAndConfig
    <*> Opt.commonDbPathAndTableName

  data Event EpochNonce = Event
    { epochNo         :: C.EpochNo
    , epochNonce      :: Ledger.Nonce
    , blockNo         :: C.BlockNo
    , blockHeaderHash :: C.Hash C.BlockHeader
    , slotNo          :: C.SlotNo
    }
    deriving (Show)

  data Runtime EpochNonce = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , ledgerCfg     :: LedgerState.ExtLedgerCfg_
    }
  data State EpochNonce = State
    { extLedgerState       :: LedgerState.ExtLedgerState_
    , maybePreviousEpochNo :: Maybe C.EpochNo
    }

  toEvents (Runtime{ledgerCfg}) state blockInMode = (State newExtLedgerState maybeEpochNo, coerce maybeEvent)
    where
    newExtLedgerState = LedgerState.applyBlock ledgerCfg (extLedgerState state) blockInMode
    maybeEpochNo = LedgerState.getEpochNo newExtLedgerState
    epochNonce = LedgerState.getEpochNonce newExtLedgerState
    maybeEvent :: [Event EpochNonce]
    maybeEvent = case EpochResolution.resolve (maybePreviousEpochNo state) maybeEpochNo of
      EpochResolution.New epochNo -> [Event epochNo epochNonce (#blockNo blockInMode) (#blockHeaderHash blockInMode) (#slotNo blockInMode)]
      EpochResolution.SameOrAbsent -> []
      EpochResolution.AssumptionBroken exception -> E.throw exception

  initialize EpochNonce{chainsyncConfig, dbPathAndTableName} trace = do
    let nodeConfig = #nodeConfig chainsyncConfig
    networkId <- #getNetworkId nodeConfig
    chainsyncRuntime' <- initializeLocalChainsync chainsyncConfig networkId trace

    let (dbPath, tableName) = defaultTableName "epoch_nonce" dbPathAndTableName
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
  " CREATE TABLE IF NOT EXISTS " <> fromString tableName <>
  "   ( epoch_no          INT  NOT NULL \
  \   , nonce             BLOB NOT NULL \
  \   , block_no          INT  NOT NULL \
  \   , block_header_hash BLOB NOT NULL \
  \   , slot_no           INT  NOT NULL)"

sqliteInsert :: SQL.Connection -> String -> [Event EpochNonce] -> IO ()
sqliteInsert c tableName events = SQL.executeMany c template $ map toRow events
  where
    template = "INSERT INTO " <> fromString tableName <>" VALUES (?, ?, ?, ?, ?)"
    toRow Event{epochNo, epochNonce, blockNo, blockHeaderHash, slotNo} = (epochNo, epochNonce, blockNo, blockHeaderHash, slotNo)
