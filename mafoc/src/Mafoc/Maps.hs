{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module Mafoc.Maps where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Word (Word64)
import Database.SQLite.Simple qualified as SQL
import Numeric.Natural (Natural)
import Streaming qualified as S
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Streaming qualified as CS
import Marconi.Index.MintBurn ()

-- * Helpers

getSecurityParam :: FilePath -> IO Natural
getSecurityParam nodeConfig = do
  (env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  pure $ fromIntegral $ C.envSecurityParam env

-- | Helper to create loops
stremFold :: Monad m => (a -> b -> m b) -> b -> S.Stream (S.Of a) m r -> S.Stream (S.Of a) m r
stremFold f acc_ source_ = loop acc_ source_
  where
    loop acc source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        acc' <- lift $ f e acc
        S.yield e
        loop acc' source'
