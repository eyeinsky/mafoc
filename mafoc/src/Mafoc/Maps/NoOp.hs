{-# LANGUAGE NamedFieldPuns #-}
module Mafoc.Maps.NoOp where

import Options.Applicative qualified as Opt

import Mafoc.CLI qualified as Opt
import Mafoc.Core (DbPathAndTableName, Indexer (Event, Runtime, State, initialize, persist, toEvent),
                   LocalChainsyncConfig, defaultTableName, initializeLocalChainsync, initializeSqlite)

data NoOp = NoOp
  { chainsync          :: LocalChainsyncConfig
  , dbPathAndTableName :: DbPathAndTableName
  } deriving Show

parseCli :: Opt.ParserInfo NoOp
parseCli = Opt.info (Opt.helper <*> cli) $ Opt.fullDesc
  <> Opt.progDesc "noop"
  <> Opt.header "noop - drain blocks over local chainsync protocol"
  where
    cli :: Opt.Parser NoOp
    cli = NoOp
      <$> Opt.commonLocalChainsyncConfig
      <*> Opt.commonDbPathAndTableName

instance Indexer NoOp where
  type Event NoOp = ()
  data State NoOp = EmptyState
  data Runtime NoOp = Runtime
  toEvent _blk _state = pure (EmptyState, Just ())
  initialize NoOp{chainsync, dbPathAndTableName} = do
    chainsyncRuntime <- initializeLocalChainsync chainsync
    let (dbPath, tableName) = defaultTableName "noop" dbPathAndTableName
    (_sqlCon, chainsyncRuntime') <- initializeSqlite dbPath tableName (\_ _ -> return ()) chainsyncRuntime
    return (EmptyState, chainsyncRuntime', Runtime)
  persist _runtime _event = return ()
