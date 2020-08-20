-- |
-- Module      : Network.NSQ.Identify
-- Description : The metadata component for formatting and parsing the metadata sent to nsqd as part of the feature negotiation done upon connection establish.
module Network.NSQ.Identify (defaultIdentify, encodeMetadata) where

import qualified Data.Aeson as A
import qualified Data.ByteString.Lazy as BL
import Data.Maybe
import qualified Data.Text as T
import Network.NSQ.Types
import Prelude hiding (take)

-- | Build a default 'IdentifyMetadata' that makes sense which is
-- basically just setting the client 'Identification' and leaving
-- the rest of the settings up to the server to determine.
defaultIdentify :: T.Text -> T.Text -> IdentifyMetadata
defaultIdentify cid host =
  IdentifyMetadata
    { ident = Identification cid host Nothing Nothing Nothing,
      tls = Nothing,
      compression = Nothing,
      heartbeatInterval = Nothing,
      outputBufferSize = Nothing,
      outputBufferTimeout = Nothing,
      sampleRate = Nothing,
      custom = Nothing,
      customNegotiation = False
    }

-- | Encode the metadata from 'IdentifyMetadata' into a 'ByteString' for
-- feeding the 'Identify' 'Command' for sending the metadata to the nsq
-- daemon as part of the feature negotiation.
encodeMetadata :: IdentifyMetadata -> BL.ByteString
encodeMetadata = A.encode
