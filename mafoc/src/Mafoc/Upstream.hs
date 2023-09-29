{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE TupleSections         #-}
{-# LANGUAGE NamedFieldPuns         #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Mafoc.Upstream where

import System.FilePath ((</>))
import GHC.OverloadedLabels (IsLabel (fromLabel))
import Control.Exception (throwIO, Exception, throw)
import Data.List qualified as L
import Streaming.Prelude qualified as S
import Database.SQLite.Simple.ToField qualified as SQL
import Database.SQLite.Simple.FromField qualified as SQL
import Data.Map.Strict qualified as M
import Data.Either (rights)

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as C
import Cardano.Streaming qualified as CS
import Cardano.Streaming.Helpers qualified as CS
import Marconi.ChainIndex.Types qualified as Marconi
import Marconi.ChainIndex.Utils qualified as Marconi
import Ouroboros.Consensus.HardFork.Combinator.AcrossEras qualified as O
import Mafoc.Exceptions qualified as E
import Cardano.Ledger.Alonzo.TxWits qualified as Ledger
import Cardano.Ledger.Alonzo.Scripts.Data (Data, DataHash)
import Cardano.Ledger.Era (EraCrypto)

import Cardano.BM.Configuration.Model qualified as CM
import Cardano.BM.Data.BackendKind qualified as CM
import Cardano.BM.Data.Output qualified as CM
import Cardano.BM.Data.Severity qualified as CM

-- * Additions to cardano-api

type SlotNoBhh = (C.SlotNo, C.Hash C.BlockHeader)

instance IsLabel "slotNoBhh" (C.BlockHeader -> SlotNoBhh) where
  fromLabel (C.BlockHeader slotNo bhh _) = (slotNo, bhh)

instance IsLabel "slotNoBhh" (C.Block era -> SlotNoBhh) where
  fromLabel (C.Block bh _txs) = fromLabel @"slotNoBhh" bh

instance IsLabel "slotNoBhh" (C.BlockInMode era -> SlotNoBhh) where
  fromLabel (C.BlockInMode block _eim) = fromLabel @"slotNoBhh" block

getSecurityParamAndNetworkId :: FilePath -> IO (Marconi.SecurityParam, C.NetworkId)
getSecurityParamAndNetworkId nodeConfig = do
  (env :: C.Env, _) <- CS.getEnvAndInitialLedgerStateHistory nodeConfig
  let securityParam' = C.envSecurityParam env :: Word64
  pure (Marconi.SecurityParam securityParam', CS.envNetworkId env)

getNetworkId :: FilePath -> IO C.NetworkId
getNetworkId nodeConfig = CS.envNetworkId . fst <$> CS.getEnvAndInitialLedgerStateHistory nodeConfig

instance Ord C.ChainTip where
  compare C.ChainTipAtGenesis C.ChainTipAtGenesis = EQ
  compare C.ChainTipAtGenesis _                   = LT
  compare _ C.ChainTipAtGenesis                   = GT
  compare (C.ChainTip a _ _) (C.ChainTip b _ _)   = compare a b

tipDistance :: C.BlockInMode mode -> C.ChainTip -> Natural
tipDistance blk ct = let
  blockNoToNatural :: C.BlockNo -> Natural
  blockNoToNatural = fromIntegral . coerce @_ @Word64
  tipBlockNo = case ct of
    C.ChainTipAtGenesis     -> 0
    C.ChainTip _ _ blockNo' -> blockNo'
  blockBlockNo = blockNo blk
  in case blockNoToNatural tipBlockNo `minusNaturalMaybe` blockNoToNatural blockBlockNo of
       Just n -> n
       Nothing -> throw $ E.Block_number_ahead_of_tip blockBlockNo ct

-- ** Query node

deriving instance Exception C.AcquiringFailure
deriving instance Exception O.EraMismatch
newtype UnspecifiedException = UnspecifiedException String deriving Show
instance Exception UnspecifiedException

-- | Query the current era of the local node's current state.
queryCurrentEra :: C.LocalNodeConnectInfo C.CardanoMode -> IO C.AnyCardanoEra
queryCurrentEra localNodeConnectInfo =
 C.queryNodeLocalState localNodeConnectInfo Nothing queryInMode >>= \case
  Left acquiringFailure -> throwIO acquiringFailure
  Right anyCardanoEra -> return anyCardanoEra

 where
  queryInMode :: C.QueryInMode C.CardanoMode C.AnyCardanoEra
  queryInMode = C.QueryCurrentEra C.CardanoModeIsMultiEra

-- | Query security param from the local node given a Shelley based era.
querySecurityParamEra :: C.LocalNodeConnectInfo C.CardanoMode -> C.ShelleyBasedEra era -> IO Marconi.SecurityParam
querySecurityParamEra localNodeConnectInfo shelleyBasedEra = do
  C.queryNodeLocalState localNodeConnectInfo Nothing queryInMode >>= \case
    Left acquiringFailure -> throwIO acquiringFailure
    Right rest -> case rest of
      Left eraMismatch -> throwIO eraMismatch
      Right genesisParams -> return $ getSecurityParam genesisParams
  where
    queryInMode :: C.QueryInMode C.CardanoMode (Either O.EraMismatch C.GenesisParameters)
    queryInMode =
      C.QueryInEra (Marconi.toShelleyEraInCardanoMode shelleyBasedEra) $
        C.QueryInShelleyBasedEra shelleyBasedEra C.QueryGenesisParameters

    getSecurityParam :: C.GenesisParameters -> Marconi.SecurityParam
    getSecurityParam = fromIntegral . C.protocolParamSecurity

querySecurityParam :: C.LocalNodeConnectInfo C.CardanoMode -> IO Marconi.SecurityParam
querySecurityParam localNodeConnectInfo = do
  C.AnyCardanoEra era <- queryCurrentEra localNodeConnectInfo
  case Marconi.shelleyBasedToCardanoEra era of
    Nothing -> throwIO $ UnspecifiedException "The security parameter can only be queried in shelley based era."
    Just shelleyBasedEra -> querySecurityParamEra localNodeConnectInfo shelleyBasedEra

-- ** Block accessors

-- | Create a ChainPoint from BlockInMode
blockChainPoint :: C.BlockInMode mode -> C.ChainPoint
blockChainPoint (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = C.ChainPoint slotNo hash

blockNo :: C.BlockInMode mode -> C.BlockNo
blockNo (C.BlockInMode (C.Block (C.BlockHeader _slotNo _bh blockNo') _) _) = blockNo'

blockSlotNoBhh :: C.BlockInMode mode -> SlotNoBhh
blockSlotNoBhh (C.BlockInMode (C.Block (C.BlockHeader slotNo hash _blockNo) _txs) _) = (slotNo, hash)

blockSlotNo :: C.BlockInMode mode -> C.SlotNo
blockSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) _) _) = slotNo

chainPointSlotNo :: C.ChainPoint -> C.SlotNo
chainPointSlotNo = \case
  C.ChainPoint slotNo _ -> slotNo
  C.ChainPointAtGenesis -> C.SlotNo 0

-- blockTxs :: forall mode era . C.IsCardanoEra era => C.BlockInMode mode -> (C.EraInMode era mode, [C.Tx era])
-- blockTxs (C.BlockInMode (C.Block _bh txs :: C.Block era) (_eim :: C.EraInMode era mode)) = txs

txoAddressAny :: C.TxOut ctx era -> C.AddressAny
txoAddressAny (C.TxOut address _value _TxOutDatum _ReferenceScript) = toAddressAny address

toAddressAny :: C.AddressInEra era -> C.AddressAny
toAddressAny = \case
  C.AddressInEra C.ByronAddressInAnyEra addr -> C.AddressByron addr
  C.AddressInEra (C.ShelleyAddressInEra _) addr -> C.AddressShelley addr

-- ** Labels

-- | We use overloaded labels instead of lens as we don't need to
-- change the values we access.

-- *** TxId

-- | We say /calculate/ because transaction body is actually hashed.
instance IsLabel "calculateTxId" (C.TxBody era -> C.TxId) where
  fromLabel = C.getTxId

instance IsLabel "calculateTxId" (C.Tx era -> C.TxId) where
  fromLabel (C.Tx txb _) = fromLabel @"calculateTxId" @(C.TxBody era -> C.TxId) txb

-- *** ChainPoint

instance IsLabel "chainPoint" (C.BlockHeader -> C.ChainPoint) where
  fromLabel (C.BlockHeader slotNo bhh _) = C.ChainPoint slotNo bhh

instance IsLabel "chainPoint" (C.Block era -> C.ChainPoint) where
  fromLabel (C.Block bh _txs) = fromLabel @"chainPoint" bh

instance IsLabel "chainPoint" (C.BlockInMode era -> C.ChainPoint) where
  fromLabel (C.BlockInMode block _eim) = fromLabel @"chainPoint" block

instance IsLabel "chainPoint" (SlotNoBhh -> C.ChainPoint) where
  fromLabel (slotNo, bhh) = C.ChainPoint slotNo bhh

-- *** BlockNo

instance IsLabel "blockNo" (C.BlockHeader -> C.BlockNo) where
  fromLabel (C.BlockHeader _slotNo _bhh blockNo') = blockNo'

instance IsLabel "blockNo" (C.Block era -> C.BlockNo) where
  fromLabel (C.Block bh _txs) = fromLabel @"blockNo" bh

instance IsLabel "blockNo" (C.BlockInMode mode -> C.BlockNo) where
  fromLabel (C.BlockInMode block _eim) = fromLabel @"blockNo" block

-- *** SlotNo

instance IsLabel "slotNo" (C.BlockInMode era -> C.SlotNo) where
  fromLabel (C.BlockInMode block _eim) = fromLabel @"slotNo" block

instance IsLabel "slotNo" (C.Block era -> C.SlotNo) where
  fromLabel (C.Block blockHeader _) = fromLabel @"slotNo" blockHeader

instance IsLabel "slotNo" (C.BlockHeader -> C.SlotNo) where
  fromLabel (C.BlockHeader slotNo _bhh _) = slotNo

instance IsLabel "slotNo" (Either C.SlotNo C.ChainPoint -> C.SlotNo) where
  fromLabel = either id (fromLabel @"slotNo")

instance IsLabel "slotNo" (C.ChainPoint -> C.SlotNo) where
  fromLabel = \case
    C.ChainPoint slotNo _bhh -> slotNo
    C.ChainPointAtGenesis -> 0

instance IsLabel "slotNo" (C.ChainTip -> C.SlotNo) where
  fromLabel = \case
    C.ChainTip slotNo _bhh _blockNo -> slotNo
    C.ChainTipAtGenesis -> 0

-- *** Block header hash

instance IsLabel "blockHeaderHash" (C.BlockInMode era -> C.Hash C.BlockHeader) where
  fromLabel (C.BlockInMode block _eim) = fromLabel @"blockHeaderHash" block

instance IsLabel "blockHeaderHash" (C.Block era -> C.Hash C.BlockHeader) where
  fromLabel (C.Block blockHeader _) = fromLabel @"blockHeaderHash" blockHeader

instance IsLabel "blockHeaderHash" (C.BlockHeader -> C.Hash C.BlockHeader) where
  fromLabel (C.BlockHeader _slotNo bhh _) = bhh

-- * Streaming

streamPassReturn
  :: Monad m
  => S.Stream (S.Of a) m r
  -> (a -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r)
  -> S.Stream (S.Of b) m r
streamPassReturn source f = lift (S.next source) >>= \case
  Left r                 -> pure r
  Right (event, source') -> f event source'

-- | Consume a stream @source@ in a loop and run effect @f@ on it.
loopM :: (MonadTrans t1, Monad m, Monad (t1 m)) => S.Stream (S.Of t2) m b -> (t2 -> t1 m a) -> t1 m b
loopM source f = loop source
  where
    loop source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (event, source'') -> do
        _ <- f event
        loop source''

-- | Fold a stream of @a@'s, yield a stream of @b@s, while keeping a state of @st".
foldYield :: Monad m => (st -> a -> m (st, b)) -> st -> S.Stream (S.Of a) m r -> S.Stream (S.Of b) m r
foldYield f st source = loop st source
  where
    loop st' source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (e, source'') -> do
        (st'', e') <- lift $ f st' e
        S.yield e'
        loop st'' source''

-- * Base

-- in base since: base-4.8.0.0
minusNaturalMaybe :: Natural -> Natural -> Maybe Natural
minusNaturalMaybe a b
  | a < b = Nothing
  | otherwise = Just (a - b)

-- * Send traces to stdout

-- Copy of defaultConfigStdout with s/out/err/ in
-- iohk-monitoring/src/Cardano/BM/Configuration/Static.lhs in
-- https://github.com/input-output-hk/iohk-monitoring-framework
defaultConfigStderr :: IO CM.Configuration
defaultConfigStderr = do
    c <- CM.empty
    CM.setMinSeverity c CM.Debug
    CM.setSetupBackends c [CM.KatipBK]
    CM.setDefaultBackends c [CM.KatipBK]
    CM.setSetupScribes c [ CM.ScribeDefinition {
                              CM.scName = "text"
                            , CM.scFormat = CM.ScText
                            , CM.scKind = CM.StderrSK
                            , CM.scPrivacy = CM.ScPublic
                            , CM.scRotation = Nothing
                            , CM.scMinSev = minBound
                            , CM.scMaxSev = maxBound
                            }
                         ,  CM.ScribeDefinition {
                              CM.scName = "json"
                            , CM.scFormat = CM.ScJson
                            , CM.scKind = CM.StderrSK
                            , CM.scPrivacy = CM.ScPublic
                            , CM.scRotation = Nothing
                            , CM.scMinSev = minBound
                            , CM.scMaxSev = maxBound
                            }
                         ]
    CM.setDefaultScribes c ["StderrSK::text"]
    return c

defaultConfigStderrSeverity :: CM.Severity -> IO CM.Configuration
defaultConfigStderrSeverity minSeverity = do
    c <- CM.empty
    CM.setMinSeverity c minSeverity
    CM.setSetupBackends c [CM.KatipBK]
    CM.setDefaultBackends c [CM.KatipBK]
    CM.setSetupScribes c [ CM.ScribeDefinition {
                              CM.scName = "text"
                            , CM.scFormat = CM.ScText
                            , CM.scKind = CM.StderrSK
                            , CM.scPrivacy = CM.ScPublic
                            , CM.scRotation = Nothing
                            , CM.scMinSev = minBound
                            , CM.scMaxSev = maxBound
                            }
                         ,  CM.ScribeDefinition {
                              CM.scName = "json"
                            , CM.scFormat = CM.ScJson
                            , CM.scKind = CM.StderrSK
                            , CM.scPrivacy = CM.ScPublic
                            , CM.scRotation = Nothing
                            , CM.scMinSev = minBound
                            , CM.scMaxSev = maxBound
                            }
                         ]
    CM.setDefaultScribes c ["StderrSK::text"]
    return c

-- * Newtypes for file paths
--
-- | Not sure if anyone actually wants these upstreamed

newtype NodeFolder = NodeFolder FilePath
  deriving newtype (Show, IsString)
newtype NodeConfig = NodeConfig FilePath
  deriving newtype (Show, IsString)
newtype SocketPath = SocketPath FilePath
  deriving newtype (Show, IsString)

instance IsLabel "nodeConfig" (NodeFolder -> NodeConfig) where
  fromLabel (NodeFolder nodeFolder) = NodeConfig (mkPath nodeFolder)
    where
      mkPath :: FilePath -> FilePath
      mkPath nodeFolder' = nodeFolder' </> "config" </> "config.json"

instance IsLabel "getNetworkId" (NodeConfig -> IO C.NetworkId) where
  fromLabel (NodeConfig nodeConfig') = getNetworkId nodeConfig'

-- * Hard-coded chainpoints
--
-- | Shamelessly stolen from Kupo's docs https://cardanosolutions.github.io/kupo/#section/Era-boundaries

lastByron, lastShelley, lastAllegra, lastMary, lastAlonzo :: SlotNoBhh
lastByron = (4492799, "f8084c61b6a238acec985b59310b6ecec49c0ab8352249afd7268da5cff2a457")
lastShelley = (16588737, "4e9bbbb67e3ae262133d94c3da5bffce7b1127fc436e7433b87668dba34c354a")
lastAllegra = (23068793, "69c44ac1dda2ec74646e4223bc804d9126f719b1c245dadc2ad65e8de1b276d7")
lastMary = (39916796, "e72579ff89dc9ed325b723a33624b596c08141c7bd573ecfff56a1f7229e4d09")
lastAlonzo = (72316796, "c58a24ba8203e7629422a24d9dc68ce2ed495420bf40d9dab124373655161a20")

data LedgerEra = Byron | Shelley | Allegra | Mary | Alonzo | Babbage
  deriving (Eq, Ord, Enum, Bounded, Show)

lastBlockOf :: LedgerEra -> Maybe SlotNoBhh
lastBlockOf = \case
  Byron -> Just lastByron
  Shelley -> Just lastShelley
  Allegra -> Just lastAllegra
  Mary -> Just lastMary
  Alonzo -> Just lastAlonzo
  Babbage -> Nothing

lastChainPointOfPreviousEra :: LedgerEra -> C.ChainPoint
lastChainPointOfPreviousEra era = maybe C.ChainPointAtGenesis (uncurry C.ChainPoint . fromMaybe impossible . lastBlockOf) maybePreviousEra
  where
    allEras = [minBound .. maxBound] :: [LedgerEra]
    pairs = zip allEras $ tail allEras
    maybePreviousEra = fmap fst $ L.find (\(_, era1) -> era1 == era) pairs :: Maybe LedgerEra

    -- Because we zip eras with it's tail, then the first era of the
    -- pair always has a last block.
    impossible = error "!!! Last block for previous ledger era always exists"

newtype TxIndexInBlock = TxIndexInBlock Word64
  deriving newtype (Show, Eq, Ord, Num, Enum, SQL.ToField)

-- * Datum

allDatums :: C.Tx era -> [(C.Hash C.ScriptData, C.ScriptData)]
allDatums tx = fromTxOuts <> fromPlutus
  where
  fromTxOuts = rights $ txDatums tx
  fromPlutus = txPlutusDatums tx


-- ** Datums from script data

-- | Get a map of datum hash to datum from a list of transactions.
plutusDatums :: [C.Tx era] -> [(C.Hash C.ScriptData, C.ScriptData)]
plutusDatums txs = txPlutusDatums =<< txs

txPlutusDatums :: C.Tx era -> [(C.Hash C.ScriptData, C.ScriptData)]
txPlutusDatums tx = map (datumFromAlonzo . snd) $ txAlonzoDatums tx

txAlonzoDatums :: C.Tx era -> [(DataHash (EraCrypto (C.ShelleyLedgerEra era)), Data (C.ShelleyLedgerEra era))]
txAlonzoDatums (C.Tx txBody _) = M.toList $ alonzoTxBodyDatums txBody

alonzoTxBodyDatums :: C.TxBody era -> M.Map (DataHash (EraCrypto (C.ShelleyLedgerEra era))) (Data (C.ShelleyLedgerEra era))
alonzoTxBodyDatums txBody = case txBody of
  C.ShelleyTxBody _ _ _ (C.TxBodyScriptData _ (Ledger.TxDats' data_) _) _ _ -> data_
  _ -> mempty

datumFromAlonzo :: Data ledgerera -> (C.Hash C.ScriptData, C.ScriptData)
datumFromAlonzo alonzoDat = let d = C.fromAlonzoData alonzoDat
  in ( C.hashScriptDataBytes d -- TODO: don't rehash data, get it directly
     , C.getScriptData d)

-- ** From TxOut

getFilteredTxOutDatumsFromTxs
  :: Maybe (C.Address C.ShelleyAddr -> Bool) -> [C.Tx era] -> M.Map (C.Hash C.ScriptData) C.ScriptData
getFilteredTxOutDatumsFromTxs addressFilter txs = M.fromList $ rights $ map snd $ getFilteredAddressDatumsFromTxs addressFilter txs

getFilteredAddressDatumsFromTxs
  :: Maybe (C.Address C.ShelleyAddr -> Bool)
  -> [C.Tx era]
  -> [(C.AddressAny, Either (C.Hash C.ScriptData) (C.Hash C.ScriptData, C.ScriptData))]
getFilteredAddressDatumsFromTxs shelleyAddressFilter txs = case shelleyAddressFilter of
  Nothing -> concatMap txAddressDatums txs
  Just p -> concat $ map (filter (p' . fst) . txAddressDatums) txs
    where
      p' = \case
        C.AddressByron _ -> True -- TODO: support byron addresses
        C.AddressShelley addr -> p addr

txAddressDatums :: C.Tx era -> [(C.AddressAny, Either (C.Hash C.ScriptData) (C.Hash C.ScriptData, C.ScriptData))]
txAddressDatums (C.Tx (C.TxBody C.TxBodyContent{C.txOuts}) _) = mapMaybe (\txOut -> (txoAddressAny txOut,) <$> maybeDatum txOut) txOuts

txDatums :: C.Tx era -> [Either (C.Hash C.ScriptData) (C.Hash C.ScriptData, C.ScriptData)]
txDatums (C.Tx (C.TxBody C.TxBodyContent{C.txOuts}) _) = mapMaybe maybeDatum txOuts

maybeDatum :: C.TxOut C.CtxTx era -> Maybe (Either (C.Hash C.ScriptData) (C.Hash C.ScriptData, C.ScriptData))
maybeDatum (C.TxOut (C.AddressInEra _ _) _ dat _) = case dat of
  C.TxOutDatumHash _ dh -> Just $ Left dh
  C.TxOutDatumInTx _ d -> Just $ Right (C.hashScriptDataBytes d, C.getScriptData d)
  C.TxOutDatumInline _ d -> Just $ Right (C.hashScriptDataBytes d, C.getScriptData d)
  C.TxOutDatumNone -> Nothing
