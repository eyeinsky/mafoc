{-# LANGUAGE DataKinds #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GADTs #-}

module Cardano.Streaming.Callbacks where

import Control.Exception (throw)
import Data.Word (Word32)

import Cardano.Api qualified as C
import Cardano.Slotting.Slot (WithOrigin (At, Origin))
import Network.TypedProtocol.Pipelined (N (Z), Nat (Succ, Zero))
import Ouroboros.Network.Protocol.ChainSync.Client qualified as CS
import Ouroboros.Network.Protocol.ChainSync.ClientPipelined qualified as CSP
import Ouroboros.Network.Protocol.ChainSync.PipelineDecision (
  PipelineDecision (Collect),
  pipelineDecisionMax,
 )

import Cardano.Streaming.Helpers qualified as H

-- * Raw chain-sync clients using callback

blocksCallbackPipelined
  :: Word32
  -> C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> (H.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ())
  -> IO ()
blocksCallbackPipelined n con point callback =
  C.connectToLocalNode con $ localChainsyncClientPipelined $ pure $ CSP.SendMsgFindIntersect [point] onIntersect'
  where
    onIntersect' =
      CSP.ClientPipelinedStIntersect
        { CSP.recvMsgIntersectFound = \_ _ -> pure $ work n
        , CSP.recvMsgIntersectNotFound = throw H.NoIntersectionFound
        }

    work
      :: Word32 -> CSP.ClientPipelinedStIdle 'Z (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()
    work pipelineSize = requestMore Origin Origin Zero
      where
        requestMore -- was clientIdle_RequestMoreN
          :: WithOrigin C.BlockNo
          -> WithOrigin C.BlockNo
          -> Nat n
          -> CSP.ClientPipelinedStIdle n (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()
        requestMore clientTip serverTip rqsInFlight =
          let
           in -- handle a response

              -- fire more requests

              case pipelineDecisionMax pipelineSize rqsInFlight clientTip serverTip of
                Collect -> case rqsInFlight of
                  Succ predN -> CSP.CollectResponse Nothing (clientNextN predN)
                _ -> CSP.SendMsgRequestNextPipelined (requestMore clientTip serverTip (Succ rqsInFlight))

        clientNextN
          :: Nat n
          -> CSP.ClientStNext n (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()
        clientNextN rqsInFlight =
          CSP.ClientStNext
            { CSP.recvMsgRollForward = \bim ct -> do
                callback $ H.RollForward bim ct
                return $ requestMore (At $ H.bimBlockNo bim) (H.fromChainTip ct) rqsInFlight
            , CSP.recvMsgRollBackward = \cp ct -> do
                callback $ H.RollBackward cp ct
                return $ requestMore Origin (H.fromChainTip ct) rqsInFlight
            }

blocksCallback
  :: C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> (H.ChainSyncEvent (C.BlockInMode C.CardanoMode) -> IO ())
  -> IO ()
blocksCallback con point callback = C.connectToLocalNode con $ localChainsyncClient $ pure $ CS.SendMsgFindIntersect [point] $ onIntersect $ \_ _ -> sendRequestNext
  where
    sendRequestNext = pure $ CS.SendMsgRequestNext onNext (pure onNext)
      where
        onNext =
          CS.ClientStNext
            { CS.recvMsgRollForward = \bim ct -> CS.ChainSyncClient $ do
                callback $ H.RollForward bim ct
                sendRequestNext
            , CS.recvMsgRollBackward = \cp ct -> CS.ChainSyncClient $ do
                callback $ H.RollBackward cp ct
                sendRequestNext
            }

intersectCallback
  :: C.LocalNodeConnectInfo C.CardanoMode
  -> C.ChainPoint
  -> (C.ChainPoint -> C.ChainTip -> IO (CS.ClientStIdle (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()))
  -> IO ()
intersectCallback con point callback =
  C.connectToLocalNode con $ localChainsyncClient $ pure $ CS.SendMsgFindIntersect [point] (onIntersect callback)

onIntersect
  :: (C.ChainPoint -> C.ChainTip -> IO (CS.ClientStIdle (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()))
  -> CS.ClientStIntersect (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ()
onIntersect callback =
  CS.ClientStIntersect
    { CS.recvMsgIntersectFound = \cp ct -> CS.ChainSyncClient $ callback cp ct
    , CS.recvMsgIntersectNotFound = throw H.NoIntersectionFound
    }

localChainsyncClient
  :: IO (CS.ClientStIdle (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ())
  -> C.LocalNodeClientProtocolsInMode C.CardanoMode
localChainsyncClient action = C.LocalNodeClientProtocols
  { C.localChainSyncClient = C.LocalChainSyncClient $ CS.ChainSyncClient action
  , C.localTxSubmissionClient = Nothing
  , C.localStateQueryClient = Nothing
  , C.localTxMonitoringClient = Nothing
  }

localChainsyncClientPipelined
  :: IO (CSP.ClientPipelinedStIdle 'Z (C.BlockInMode C.CardanoMode) C.ChainPoint C.ChainTip IO ())
  -> C.LocalNodeClientProtocolsInMode C.CardanoMode
localChainsyncClientPipelined action = C.LocalNodeClientProtocols
  { C.localChainSyncClient = C.LocalChainSyncClientPipelined $ CSP.ChainSyncClientPipelined action
  , C.localTxSubmissionClient = Nothing
  , C.localStateQueryClient = Nothing
  , C.localTxMonitoringClient = Nothing
  }
