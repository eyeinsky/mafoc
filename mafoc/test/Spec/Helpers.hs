module Spec.Helpers where

import Control.Monad (forM_)
import Data.String (fromString)
import Hedgehog qualified as H

footnotes :: H.MonadTest m => [(String, String)] -> m ()
footnotes = H.footnote . unlines . map (\(label, a) -> label <> ": " <> a )

classifier :: H.MonadTest m => String -> [(String, Bool)] -> m ()
classifier groupName cases = forM_ cases $ \(string, bool) ->
  H.classify (fromString $ "[" <> groupName <> "] " <> string) bool
