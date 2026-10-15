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
  "type": "message",
  "source": "sensor-service",
  "target": "telemetry",
  "token": "optional-auth-token",
  "data": {
    "temperature": 21.5,
    "status": "nominal"
  }
}
```

## Helper Methods

- `NewMessageEvent(source, target string, data any) (*Event, error)`: Constructs a message event with arbitrary JSON-serializable payload.
- `NewFileEvent(source, target string, file *FileMessage, token ...string) (*Event, error)`: Constructs a file event carrying binary payload and metadata.
- `NewAttachEvent(source, channel string) (*Event, error)`: Constructs a channel subscription event.
- `NewDetachEvent(source, channel string) (*Event, error)`: Constructs a channel unsubscription event.
- `NewPingEvent(source string, data ...any) (*Event, error)`: Constructs a ping heartbeat.
- `NewPongEvent(source, target string, data ...any) (*Event, error)`: Constructs a pong response.
- `evt.UnmarshalData(targetPtr any) error`: Decodes the inner JSON payload directly into a target struct or map.
- `evt.GetStringData() (string, error)`: Extracts raw or JSON string payloads safely.
