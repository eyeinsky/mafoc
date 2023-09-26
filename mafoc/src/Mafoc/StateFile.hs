{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE TupleSections #-}

module Mafoc.StateFile where

import Data.ByteString.Char8 qualified as C8
import Data.List qualified as L
import Data.Text qualified as TS
import System.Directory (listDirectory, removeFile)
import Text.Read qualified as Read

import Cardano.Api qualified as C

import Mafoc.Upstream (SlotNoBhh)

-- * Store

store :: String -> SlotNoBhh -> (FilePath -> IO ()) -> IO FilePath
store prefix slotNoBhh storeTo = storeTo path *> keepLatestTwo prefix $> path
  where path = toName prefix slotNoBhh :: FilePath

toName :: String -> SlotNoBhh -> FilePath
toName prefix (slotNo, blockHeaderHash) =
  L.intercalate
    "_"
    [ prefix
    , show (coerce slotNo :: Word64)
    , TS.unpack (C.serialiseToRawBytesHexText blockHeaderHash)
    ]

keepLatestTwo :: String -> IO ()
keepLatestTwo prefix = mapM_ (removeFile . toName prefix) . drop 2 . map snd =<< list "." prefix

-- * Read

-- | Load ledger state from file, while taking the chain point it's at from the file name.
loadLatest :: String -> (FilePath -> IO a) -> IO a -> IO (a, C.ChainPoint)
loadLatest prefix load init_ =
  list "." prefix >>= \case
    -- A state exists on disk, resume from there
    (fn, (slotNo, bhh)) : _ -> do
      state <- load fn
      let cp = C.ChainPoint slotNo bhh
      return (state, cp)
    -- No existing states, start from genesis
    [] -> do
      state <- init_
      return (state, C.ChainPointAtGenesis)

list :: FilePath -> String -> IO [(FilePath, SlotNoBhh)]
list dirPath prefix = L.sortBy (flip compare `on` snd) . mapMaybe parse <$> listDirectory dirPath
  where
    parse :: FilePath -> Maybe (FilePath, SlotNoBhh)
    parse fn = case prefixBhhFromFileName fn of
      Right (filePrefix, slotNoBhh)
        | prefix == filePrefix -> Just (fn, slotNoBhh)
        | otherwise -> Nothing
      Left _err -> Nothing

-- * Parsers

bhhFromFileName :: String -> Either String SlotNoBhh
bhhFromFileName = fmap snd . prefixBhhFromFileName

prefixBhhFromFileName :: String -> Either String (String, SlotNoBhh)
prefixBhhFromFileName str = case reverse parts of
    blockHeaderHashHex : slotNoStr : rest -> let
      prefix = L.intercalate "_" $ reverse rest
      in fmap ((,) prefix) $
        (,)
          <$> parseSlotNo_ slotNoStr
          <*> eitherParseHashBlockHeader blockHeaderHashHex
    _ -> Left "Can't parse state file name, must be: <prefix> _ <slot no> _ <block header hash> ..."
  where
    parts = splitOn '_' str


eitherParseHashBlockHeader :: String -> Either String (C.Hash C.BlockHeader)
eitherParseHashBlockHeader str = case C.deserialiseFromRawBytesHex (C.proxyToAsType Proxy) $ C8.pack str of
  Left err -> Left $ show err
  Right result -> Right result

parseSlotNo_ :: String -> Either String C.SlotNo
parseSlotNo_ str = maybe (leftError "Can't read SlotNo" str) (Right . C.SlotNo) $ Read.readMaybe str

-- * Helpers

leftError :: String -> String -> Either String a
leftError label str = Left $ label <> ": '" <> str <> "'"

splitOn :: (Eq a) => a -> [a] -> [[a]]
splitOn x xs = case span (/= x) xs of
  (prefix, _x : rest) -> prefix : recurse rest
  (lastChunk, []) -> [lastChunk]
  where
    recurse = splitOn x
