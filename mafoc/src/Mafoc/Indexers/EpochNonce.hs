module Mafoc.Indexers.EpochNonce where

import Control.Exception qualified as E
import Database.SQLite.Simple qualified as SQL

import Cardano.Api qualified as C
import Cardano.Ledger.Shelley.API qualified as Ledger
import Ouroboros.Consensus.Ledger.Extended qualified as O

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig, NodeConfig, sqliteOpen, defaultTableName, initializeLocalChainsync,
                   loadLatestTrace, ensureStartFromCheckpoint, SqliteTable, query1
                  )
import Mafoc.EpochResolution qualified as EpochResolution
import Mafoc.LedgerState qualified as LedgerState
import Mafoc.Upstream.Orphans () -- FromRow Ledger.Nonce

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
    newLedgerState = O.ledgerState newExtLedgerState
    maybeEpochNo = LedgerState.getEpochNo newLedgerState
    epochNonce = LedgerState.getEpochNonce newExtLedgerState
    maybeEvent :: [Event EpochNonce]
    maybeEvent = case EpochResolution.resolve (maybePreviousEpochNo state) maybeEpochNo of
      EpochResolution.New epochNo -> [Event epochNo epochNonce (#blockNo blockInMode) (#blockHeaderHash blockInMode) (#slotNo blockInMode)]
      EpochResolution.SameOrAbsent -> []
      EpochResolution.AssumptionBroken exception -> E.throw exception

  initialize cli@EpochNonce{chainsyncConfig, dbPathAndTableName} trace = do
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

defaultTable :: String
defaultTable = "epoch_nonce"

queryEpochNonce :: C.EpochNo -> SqliteTable -> IO (Maybe Ledger.Nonce)
queryEpochNonce epochNo (con, table) = query1 con query' epochNo >>= \case
  [SQL.Only res] -> return $ Just res
  [] -> return Nothing
  _ : _ : _ -> error "there shouldn't be more than one result"
  where
    query' = "SELECT nonce FROM " <> fromString table <> " WHERE epoch_no = ?"

queryEpochNonces :: SqliteTable -> IO [(C.EpochNo, Ledger.Nonce)]
queryEpochNonces (con, table) = SQL.query_ con $ "SELECT epoch_no, nonce FROM " <> fromString table
