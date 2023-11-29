module Mafoc.Indexers.NoOp where

import Database.SQLite.Simple qualified as SQL
import Prettyprinter (Pretty (pretty), (<+>))

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_,
                   setCheckpointSqlite, getCheckpointSqlite, sqliteInitCheckpoints, sqliteOpen, useLaterStartingPoint, stateless, traceInfo)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig_
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

instance Indexer NoOp where
  description = "Don't index anything, but drain blocks over local chainsync to measure speed ceiling"
  parseCli = NoOp
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName
  data Event NoOp = EmptyEvent
  newtype State NoOp = Stateless ()
  data Runtime NoOp = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    }
  toEvents _runtime _state _blockInMode = (stateless, [EmptyEvent])
  initialize cli@NoOp{chainsync, dbPathAndTableName} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace $ show cli
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlCon
    maybeCheckpointedChainPoint <- getCheckpointSqlite sqlCon tableName
    chainsyncRuntime' <- case maybeCheckpointedChainPoint of
      Just cp -> traceInfo trace ("Found checkpoint " <+> pretty cp) $> useLaterStartingPoint chainsyncRuntime cp
      Nothing -> traceInfo trace "No checkpoint found" $> chainsyncRuntime
    return (stateless, chainsyncRuntime', Runtime sqlCon tableName)
  persistMany _runtime _events = return ()
  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
