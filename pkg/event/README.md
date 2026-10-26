# Package Event (`pkg/event`)

`pkg/event` provides the standardized event envelope models, event types, and JSON serialization helpers for exchanging messages with WSBroker.

## Event Types

| Type | Constant | Description |
| :--- | :--- | :--- |
| `attach` | `event.TypeAttach` | Subscribes the client to a channel. |
| `detach` | `event.TypeDetach` | Unsubscribes the client from a channel. |
| `message` | `event.TypeMessage` | Transmits a broadcast or point-to-point data payload. |
| `file` | `event.TypeFile` | Transmits a binary file payload with attached metadata (filename, mime-type, size). |
| `ping` | `event.TypePing` | Ping heartbeat frame. |
| `pong` | `event.TypePong` | Pong heartbeat response frame. |

## Event Envelope Schema

```json
{
  "id": "evt-a1b2c3d4",
  "refId": "evt-b9c8d7e6",
  "type": "message",
  "source": "sensor-service",
  "target": "telemetry",
  "channel": "telemetry",
  "token": "optional-auth-token",
  "data": {
    "temperature": 21.5,
    "status": "nominal"
  }
}
```

### Envelope Fields

| Field | JSON Key | Type | Description |
| :--- | :--- | :--- | :--- |
| `ID` | `id` | `string` (optional) | Unique 16-byte random hex event identifier. |
| `RefID` | `refId` | `string` (optional) | Referenced event identifier for correlating replies and responses. |
| `Type` | `type` | `EventType` | Event message type (e.g. `message`, `attach`, `ping`, `pong`). |
| `Source` | `source` | `string` | Sender identifier. |
| `Target` | `target` | `string` | Target client identifier or channel name. |
| `Channel` | `channel` | `string` (optional) | Channel name for routed/broadcast messages. |
| `Token` | `token` | `string` (optional) | Optional authentication or authorization token. |
| `Data` | `data` | `any` | Raw JSON payload value. |

## Helper Methods

- `NewMessageEvent(source, target string, data any) (*Event, error)`: Constructs a message event with arbitrary JSON-serializable payload.
- `NewFileEvent(source, target string, file *FileMessage, token ...string) (*Event, error)`: Constructs a file event carrying binary payload and metadata.
- `NewAttachEvent(source, channel string) (*Event, error)`: Constructs a channel subscription event.
- `NewDetachEvent(source, channel string) (*Event, error)`: Constructs a channel unsubscription event.
- `NewPingEvent(source string, data ...any) (*Event, error)`: Constructs a ping heartbeat.
- `NewPongEvent(source, target string, data ...any) (*Event, error)`: Constructs a pong response with optional `refId`.
- `NewReplyEvent(orig *Event, replyType EventType, data any) (*Event, error)`: Constructs a reply event correlating to the original event with `refId` populated.
- `evt.CreateReply(replyType EventType, data any) (*Event, error)`: Creates a reply event correlating to `evt` with `refId` set to `evt.ID`.
- `evt.UnmarshalData(targetPtr any) error`: Decodes the inner JSON payload directly into a target struct or map.
- `evt.GetStringData() (string, error)`: Extracts raw or JSON string payloads safely.
- `evt.GetRefID() string` / `evt.SetRefID(refID string)`: Getter and setter for the correlated reference ID.
