module Mafoc.Cmds.SlotNoChainPoint where

import Database.SQLite.Simple qualified as SQL
import Cardano.Api qualified as C
import Data.Text.IO qualified as TS
import Mafoc.Core (renderPretty, chainPointForSlotNo)

run :: FilePath -> C.SlotNo -> IO ()
run dbPath slotNo = do
  sqlCon <- SQL.open dbPath
  maybeChainPoint <- chainPointForSlotNo (sqlCon, "blockbasics") slotNo
  case maybeChainPoint of
    Just cp -> TS.putStrLn $ renderPretty cp
    Nothing -> return ()
