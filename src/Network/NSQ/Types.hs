{-# LANGUAGE OverloadedStrings #-}

-- |
-- Module      : Network.NSQ.Types
-- Description : Not much to see here, just the types for the library.
module Network.NSQ.Types
  ( MsgId,
    Topic,
    Channel,
    LogName,
    Message (..),
    Command (..),
    -- TODO: probably don't want to export constructor here but want pattern matching
    FrameType (..),
    ErrorType (..),
    OptionalSetting (..),
    TLS (..),
    Compression (..),
    Identification (..),
    IdentifyMetadata (..),
    -- Connection config/state
    NSQConnection (..),
    defaultUserAgent
  )
where

import Data.Aeson ((.=))
import qualified Data.Aeson as A
import qualified Data.Aeson.Types as AT
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import qualified Data.Map.Strict as Map
import Data.Maybe (catMaybes, fromMaybe)
import qualified Data.Text as T
import Data.Word (Word16, Word64, Word8)
import Network.Socket (PortNumber)

-- High level arch:
--  * One queue per topic/channel
--  * This queue can be feed by multiple nsqd (load balanced/nsqlookup for ex)
--  * Probably will have one set of state/config per nsqd connection and per
--  queue/topic/channel
--  * Can probably later on provide helpers for consuming the queue

-- TODO: consider using monad-journal logger for pure code tracing

-- | Per Connection configuration such as: per nsqd state (rdy, load balance),
-- per topic state (channel)
data NSQConnection = NSQConnection
  { server :: String,
    port :: PortNumber,
    logName :: LogName,
    identConf :: IdentifyMetadata
  }

-- | Logger Name for a connection (hslogger format)
type LogName = String

-- | Message Id, it is a 16-byte hexdecimal string encoded as ASCII
type MsgId = BS.ByteString

-- | NSQ Topic, the only allowed character in a topic is @\\.a-zA-Z0-9_-@
type Topic = T.Text

-- | NSQ Channel, the only allowed character in a channel is @\\.a-zA-Z0-9_-@
-- A channel can be marked as ephemeral by toggling the 'Bool' value to
-- true in 'Sub'
type Channel = T.Text

-- | NSQ Command
data Command
  = -- | The protocol version
    Protocol
  | -- | No-op, usually used in reply to a 'Heartbeat' request from the server.
    NOP
  | -- | Client Identification + possible features negotiation.
    Identify IdentifyMetadata
  | -- | Subscribe to a specified 'Topic'/'Channel', use 'True' if its an ephemeral channel.
    Sub Topic Channel Bool
  | -- | Publish a message to the specified 'Topic'.
    Pub Topic BS.ByteString
  | -- | Publish multiple messages to a specified 'Topic'.
    MPub Topic [BS.ByteString]
  | -- | Update @RDY@ state (ready to recieve messages). Number of message you can process at once.
    Rdy Word64
  | -- | Finish a message.
    Fin MsgId
  | -- | Re-queue a message (failure to process), Timeout is in milliseconds.
    Req MsgId Word64
  | -- | Reset the timeout for an in-flight message.
    Touch MsgId
  | -- | Cleanly close the connection to the NSQ daemon.
    Cls
  | -- | Catch-all command for future expansion/custom commands.
    Command BS.ByteString
  deriving (Show)

-- | Frame Type of the incoming data from the NSQ daemon.
data FrameType
  = -- | Response to a 'Command' from the server.
    FTResponse
  | -- | An error in response to a 'Command'.
    FTError
  | -- | Messages.
    FTMessage
  | -- | For future extension for handling new Frame Types.
    FTUnknown Int32
  deriving (Show)

-- | Types of error that the server can return in response to an 'Command'
data ErrorType
  = -- | Something went wrong with the command (IDENTIFY, SUB, PUB, MPUB, RDY, FIN, REQ, TOUCH, CLS)
    Invalid
  | -- | Bad Body (IDENTIFY, MPUB)
    BadBody
  | -- | Bad Topic (most likely used disallowed characters) (SUB, PUB, MPUB)
    BadTopic
  | -- | Bad channel (Like 'BadTopic' probably used an disallowed character) (SUB)
    BadChannel
  | -- | Bad Message (PUB, MPUB)
    BadMessage
  | -- | Publishing a message failed (PUB)
    PubFailed
  | -- | Same as 'PubFailed' (MPUB)
    MPubFailed
  | -- | Finish failed (Probably already finished or non-existant message-id) (FIN)
    FinFailed
  | -- | Requeue failed (REQ)
    ReqFailed
  | -- | Touch failed (TOUCH)
    TouchFailed
  | -- | New unknown type of error (ANY)
    Unknown BS.ByteString
  deriving (Show)

-- | The message and replies back from the server.
data Message
  = -- | Everything is allright.
    OK
  | -- | Heartbeat, reply with the 'NOP' 'Command'.
    Heartbeat
  | -- | Server has closed the connection.
    CloseWait
  | -- | The server sent back an error.
    Error ErrorType
  | -- | A message to be processed. The values are: Nanosecond Timestamp, number of attempts, Message Id, and the content of the message to be processed.
    Message Int64 Word16 MsgId BS.ByteString
  | -- | Catch-all message for future expansion. This currently includes the reply from 'Identify' if feature negotiation is set.
    CatchAllMessage FrameType BS.ByteString
  deriving (Show)

-- | Optional settings, if 'Disabled' then this setting will be put in the
-- json as disabled specifically vs "not being listed".
data OptionalSetting = Disabled | Custom Word64
  deriving (Show)

-- | TLS version supported
data TLS = NoTLS | TLSV1
  deriving (Show)

-- | For 'Deflate' its the compression level from 0-9
data Compression = NoCompression | Snappy | Deflate Word8
  deriving (Show)

-- | The client identification
data Identification = Identification
  { -- | An identifier of this consumer, something specific to this consumer
    clientId :: T.Text,
    -- | Hostname of the machine the client is running on
    hostname :: T.Text,
    -- | Deprecated in favor of client_id
    shortId :: Maybe T.Text,
    -- | Deprecated in favor of hostname
    longId :: Maybe T.Text,
    -- | Default (client_library_name/version)
    userAgent :: Maybe T.Text
  }
  deriving (Show)

-- | Metadata for feature negotiation, if any of the values
-- are set it will be sent to the server otherwise they will be omitted.
-- If the setting is set to 'Nothing' it will not be sent to the server,
-- and if its set to 'Just' 'Disabled' it will be sent to the server as
-- disabled explicitly.
data IdentifyMetadata = IdentifyMetadata
  { -- | Client identification
    ident :: Identification,
    -- | TLS
    tls :: Maybe TLS,
    -- | Compression
    compression :: Maybe Compression,
    -- | The time between each heartbeat (disabled = -1)
    heartbeatInterval :: Maybe OptionalSetting,
    -- | The size of the buffer (disabled = -1)
    outputBufferSize :: Maybe OptionalSetting,
    -- | The timeout for the buffer (disabled = -1)
    outputBufferTimeout :: Maybe OptionalSetting,
    -- | Sampling of the message will be sent to the client (disabled = 0)
    sampleRate :: Maybe OptionalSetting,
    -- | Map of possible key -> value for future protocol expansion
    custom :: Maybe (Map.Map T.Text T.Text),
    -- | Set if there are any 'custom' values to send
    customNegotiation :: Bool
  }
  deriving (Show)

-- | Take the custom settings out of the custom map and render Aeson pairs
customMetadata :: Maybe (Map.Map T.Text T.Text) -> [AT.Pair]
customMetadata Nothing = []
customMetadata (Just val) = Map.foldrWithKey (\k v xs -> (k .= v) : xs) [] val

-- | Tls settings
tlsSettings :: Maybe TLS -> [Maybe AT.Pair]
tlsSettings Nothing = []
tlsSettings (Just NoTLS) = [Just $ "tls_v1" .= False]
tlsSettings (Just TLSV1) = [Just $ "tls_v1" .= True]

-- | Take an optional setting and render an Aeson pair.
optionalSettings :: T.Text -> Int -> Maybe OptionalSetting -> Maybe AT.Pair
optionalSettings _ _ Nothing = Nothing
optionalSettings name def (Just Disabled) = Just (name, A.toJSON def)
optionalSettings name _ (Just (Custom val)) = Just (name, A.toJSON val)

-- | Render the Aeson pairs for optional compression
optionalCompression :: Maybe Compression -> [Maybe AT.Pair]
optionalCompression Nothing = []
optionalCompression (Just NoCompression) =
  Just <$> ["snappy" .= False, "deflate" .= False]
optionalCompression (Just Snappy) =
  Just <$> ["snappy" .= True, "deflate" .= False]
optionalCompression (Just (Deflate l)) =
  Just <$> ["snappy" .= False, "deflate" .= True, "deflate_level" .= l]

-- | Generate a collection of Aeson pairs to insert into the json
-- that is being sent to the server as part of the metadata negotiation.
featureNegotiation :: IdentifyMetadata -> [AT.Pair]
featureNegotiation im =
  catMaybes $
    tlsSettings (tls im)
      ++ [ optionalSettings "heartbeat_interval" (-1) $ heartbeatInterval im,
           optionalSettings "output_buffer_size" (-1) $ outputBufferSize im,
           optionalSettings "output_buffer_timeout" (-1) $ outputBufferTimeout im,
           optionalSettings "sample_rate" 0 $ sampleRate im
         ]
      ++ optionalCompression (compression im)

-- | The default user agent to send, for identifying what client library is
-- connecting to the nsqd.
defaultUserAgent :: T.Text
defaultUserAgent = "hsnsq/0.1.2.0" -- TODO: find out how to identify this in the build step

instance A.ToJSON IdentifyMetadata where
  toJSON im@(IdentifyMetadata {ident = i}) =
    A.object $
       -- Identification section
        [ "client_id" .= clientId i,
          "hostname" .= hostname i,
          "short_id" .= fromMaybe (clientId i) (shortId i),
          "long_id" .= fromMaybe (hostname i) (longId i),
          "user_agent" .= fromMaybe defaultUserAgent (userAgent i),
          -- Feature Negotiation section
          "feature_negotiation"
            .= (not (null $ featureNegotiation im) || customNegotiation im)
        ]
          ++ featureNegotiation im
          ++ customMetadata (custom im)
      
