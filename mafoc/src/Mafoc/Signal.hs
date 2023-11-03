module Mafoc.Signal where

import Control.Concurrent qualified as IO
import Streaming.Prelude qualified as S
import System.Posix.Signals qualified as Signals

-- * Stop

newtype Stop = Stop (IO.MVar Bool)

-- | Returns an MVar which when filled, means that the program should close.
setupCtrlCHandler :: Int -> IO Stop
setupCtrlCHandler max' = do
  countMVar <- IO.newMVar 0
  signalMVar <- IO.newMVar False
  let ctrlcHandler = Signals.Catch $ do
        count <- IO.modifyMVar countMVar incr
        void $ IO.swapMVar signalMVar (count >= 0)
        if count == 1
          then putStrLn "Shutting down gracefully.." --
          else putStrLn "Press Ctrl-C few more times to terminate without a final checkpoint."
        when (count >= max') $ Signals.raiseSignal Signals.sigTERM
  _ <- Signals.installHandler Signals.sigINT ctrlcHandler Nothing
  return $ Stop signalMVar
  where
    incr :: Int -> IO (Int, Int)
    incr count = let count' = count + 1 in return (count', count')

-- | Stop streaming when the signal MVar is filled with a True.
takeWhileStopSignal :: Stop -> S.Stream (S.Of a) IO r -> S.Stream (S.Of a) IO ()
takeWhileStopSignal (Stop stopSignalMVar) = S.takeWhileM (\_ -> not <$> IO.readMVar stopSignalMVar)

-- * Chekpoint

newtype Checkpoint = Checkpoint (IO.MVar Bool)

setupCheckpointSignal :: IO Checkpoint
setupCheckpointSignal = do
  mvar <- IO.newMVar False
  let handler = Signals.Catch $ do
        void $ IO.swapMVar mvar True
        putStrLn "Chekpoint requested." -- todo trace debug
  _ <- Signals.installHandler Signals.sigUSR1 handler Nothing
  return $ Checkpoint mvar

-- * Print chainsync statistics
--
-- | Signal for requesting chainsync statistics.

newtype ChainsyncStats = ChainsyncStats (IO.MVar Bool)

setupChainsyncStatsSignal :: IO ChainsyncStats
setupChainsyncStatsSignal = do
  mvar <- IO.newMVar False
  let handler = Signals.Catch $ do
        void $ IO.swapMVar mvar True
        putStrLn "Chainsync stats requested." -- todo trace debug
  _ <- Signals.installHandler Signals.sigUSR2 handler Nothing
  return $ ChainsyncStats mvar

-- * Reset signal

resetGet :: Coercible a (IO.MVar Bool) => a -> IO Bool
resetGet signal = IO.swapMVar (coerce signal) False
