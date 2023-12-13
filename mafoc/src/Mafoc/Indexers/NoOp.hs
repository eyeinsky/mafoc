module Mafoc.Indexers.NoOp where

import Database.SQLite.Simple qualified as SQL
import Prettyprinter (Pretty (pretty), (<+>))
import Options.Applicative qualified as Opt

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName,
                   Indexer (Event, Runtime, State, checkpoint, description, initialize, parseCli, persistMany, toEvents),
                   LocalChainsyncConfig_, defaultTableName, initializeLocalChainsync_,
                   setCheckpointSqlite, getCheckpointSqlite, sqliteInitCheckpoints, sqliteOpen, useLaterStartingPoint, stateless, traceInfo)

data NoOp = NoOp
  { chainsync           :: LocalChainsyncConfig_
  , dbPathAndTableName  :: DbPathAndTableName
  , numberOfEventsPerBlock :: Int
  } deriving Show

instance Indexer NoOp where

  description = "Don't index anything, but drain blocks over local chainsync to measure speed ceiling"

  parseCli = NoOp
    <$> Opt.commonLocalChainsyncConfig
    <*> Opt.commonDbPathAndTableName
    <*> emptyEventsNumber
    where
      emptyEventsNumber = Opt.option Opt.auto
         $ Opt.longOpt "empty-events" "Generate empty event for each block. Otherwise no events are generate and persist is never run."
        <> Opt.value 1


  data Event NoOp = EmptyEvent
  newtype State NoOp = Stateless ()
  data Runtime NoOp = Runtime
    { sqlConnection :: SQL.Connection
    , tableName     :: String
    , eventsPerBlock :: [Event NoOp]
    }
  toEvents Runtime{eventsPerBlock} _state _blockInMode = (stateless, eventsPerBlock)
  initialize cli@NoOp{chainsync, dbPathAndTableName, numberOfEventsPerBlock} trace = do
    chainsyncRuntime <- initializeLocalChainsync_ chainsync trace $ show cli
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    sqlCon <- sqliteOpen dbPath
    sqliteInitCheckpoints sqlCon
    maybeCheckpointedChainPoint <- getCheckpointSqlite sqlCon tableName
    chainsyncRuntime' <- case maybeCheckpointedChainPoint of
      Just cp -> traceInfo trace ("Found checkpoint " <+> pretty cp) $> useLaterStartingPoint chainsyncRuntime cp
      Nothing -> traceInfo trace "No checkpoint found" $> chainsyncRuntime
    return (stateless, chainsyncRuntime', Runtime sqlCon tableName $ replicate numberOfEventsPerBlock EmptyEvent)
  persistMany _runtime _events = return ()
  checkpoint Runtime{sqlConnection, tableName} _state slotNoBhh = setCheckpointSqlite sqlConnection tableName slotNoBhh
