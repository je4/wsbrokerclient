# Package Event (`pkg/event`)

`pkg/event` provides standardized event envelope models, event types, JSON serialization helpers, dynamic custom payload registration, and type conversion utilities for communicating with WSBroker.

## Event Types

The package defines built-in system, time synchronization, and metadata event types alongside support for arbitrary custom domain event types.

| Type | Constant | Description | Default Payload |
| :--- | :--- | :--- | :--- |
| `attach` | `event.TypeAttach` | Subscribes the client to a channel. | `string` (channel name) |
| `detach` | `event.TypeDetach` | Unsubscribes the client from a channel. | `string` (channel name) |
| `message` | `event.TypeMessage` | Transmits a broadcast or point-to-point data payload. | `string` / `any` |
| `file` | `event.TypeFile` | Transmits a binary file payload with metadata. | `*event.FileMessage` |
| `ping` | `event.TypePing` | Ping heartbeat frame. | `string` / `any` |
| `pong` | `event.TypePong` | Pong heartbeat response frame. | `string` / `any` |
| `ntp_query` | `event.TypeNTPQuery` | NTP client query packet. | `[]byte` |
| `ntp_response` | `event.TypeNTPResponse` | NTP server response packet. | `[]byte` |
| `ntp_error` | `event.TypeNTPError` | NTP upstream error message. | `string` |
| `channel_info_query` | `event.TypeChannelInfoQuery` | Queries channel status and subscribers. | `string` (channel name) |
| `channel_info_response` | `event.TypeChannelInfoResponse` | Channel metadata and subscriber list response. | `*event.ChannelInfo` |
| `channel_info_error` | `event.TypeChannelInfoError` | Channel query error response. | `string` |

---

## Event Envelope Schema

All events share a common envelope structure:

```json
{
  "id": "evt-a1b2c3d4e5f60718",
  "refId": "evt-b9c8d7e6f5a43210",
  "type": "message",
  "source": "sensor-service",
  "target": "telemetry",
  "channel": "telemetry",
  "session": "sess-a1b2c3d4",
  "timestamp": "2026-10-31T12:00:00Z",
  "data": {
    "temperature": 21.5,
    "status": "nominal"
  }
}
```

### Envelope Models

- **`BaseEvent`**: Contains all transport, routing, and correlation metadata (`ID`, `RefID`, `Type`, `Source`, `Target`, `Channel`, `Session`, `Timestamp`).
- **`Event[T any]`**: Couples `BaseEvent` with a strongly-typed `Data T` payload.
- **`RawEvent`**: Alias for `Event[jsontext.Value]`, representing an event with an unparsed, raw JSON payload.

### Envelope Fields

| Field | JSON Key | Type | Description |
| :--- | :--- | :--- | :--- |
| `ID` | `id` | `string` (optional) | Unique 16-byte random hex event identifier generated via `GenerateEventID()`. |
| `RefID` | `refId` | `string` (optional) | Correlated request event ID for RPC requests, replies, and error tracking. |
| `Type` | `type` | `EventType` | Event message type (built-in or custom domain string). |
| `Source` | `source` | `string` | Client identifier of the sender. |
| `Target` | `target` | `string` (optional) | Target client identifier or channel name. |
| `Channel` | `channel` | `string` (optional) | Channel name for routed/broadcast messages. |
| `Session` | `session` | `string` (optional) | Optional session or workflow context identifier. |
| `Timestamp` | `timestamp` | `time.Time` (optional) | Timestamp when the event was created. |
| `Data` | `data` | `T` / `jsontext.Value` | Payload data. |

---

## Universal Event Constructors

Rather than requiring separate factory functions for each event type, `pkg/event` provides consolidated universal constructors:

- **`NewRawEvent(eventType EventType, source, target string, data any) (*RawEvent, error)`**: Constructs a `RawEvent` by JSON-marshaling `data`.
- **`NewEventTyped[T any](eventType EventType, source, target string, data T) (*Event[T], error)`**: Constructs a strongly-typed `Event[T]`.
- **`NewEvent(data DataInterface, target string, session ...string) (*RawEvent, error)`**: Constructs a `RawEvent` from any payload type implementing `DataInterface` (such as `FileMessage`).
- **`NewMessageEvent(source, target string, data any) (*RawEvent, error)`**: Helper shortcut for `NewRawEvent(TypeMessage, source, target, data)`.
- **`NewReplyEvent(original *RawEvent, replyType EventType, data any) (*RawEvent, error)`**: Creates a correlated reply event with `RefID` set to `original.ID` and `Target` set to `original.Source`.
- **`raw.CreateReply(replyType EventType, data any) (*RawEvent, error)`**: Method on `RawEvent` to produce a correlated reply.

---

## Dynamic Type Registry

The event package includes a thread-safe registry mapping `EventType` strings to payload types. This allows generic handlers and `RawEvent.GetData()` to dynamically unmarshal JSON payloads into their concrete Go structs without compile-time coupling.

### Registration

Register payload types during application or package initialization (`init()`):

```go
package rfidclient

import "github.com/dronemapper/wsbrokerclient/pkg/event"

const TypeTagRead event.EventType = "rfid/tag_read"

type TagReadData struct {
    EPC  string  `json:"epc"`
    RSSI float64 `json:"rssi"`
    Antenna int  `json:"antenna"`
}

func init() {
    // Registers TagReadData for automatic dynamic decoding
    event.RegisterPayload[TagReadData](TypeTagRead)
}
```

> **Fail-Fast Safety**: `RegisterPayload[T]` and `RegisterPayloadFactory` will panic at startup if called with an empty event type or if an event type is registered more than once.

### Dynamic Unmarshaling (`GetData` & `UnmarshalDynamic`)

When `raw.GetData()` or `event.UnmarshalDynamic(raw)` is called on a `RawEvent`:
1. It looks up `raw.Type` in the type registry.
2. If registered, it unmarshals the JSON into a pointer to the registered struct (`*T`) and returns it as `any`.
3. If unregistered, it falls back to parsing as a string, primitive, or generic JSON map/slice structure.

```go
data, err := raw.GetData()
if err != nil {
    log.Fatalf("failed to decode payload: %v", err)
}

switch v := data.(type) {
case *TagReadData:
    fmt.Printf("RFID Tag read: EPC=%s RSSI=%.1f\n", v.EPC, v.RSSI)
case *event.FileMessage:
    fmt.Printf("Received file: %s (%d bytes)\n", v.Filename, v.Size)
default:
    fmt.Printf("Generic payload: %v\n", v)
}
```

---

## Type Conversion & Fail-Fast Validation (`ToTyped`)

`event.ToTyped[T](raw, expectedTypes...)` converts a `RawEvent` into a typed `*Event[T]`.

To maximize performance and prevent data corruption, `ToTyped` validates compatibility **before** unmarshaling JSON (without reflection overhead):
1. **Expected Types Check**: If optional `expectedTypes` are provided, `raw.Type` must match one of them.
2. **`DataInterface` Check**: If `T` implements `DataInterface`, `raw.Type` must match `di.Type()`.
3. **Fail-Fast**: If validation fails, `ToTyped` returns an error immediately and avoids JSON unmarshaling.

```go
// Direct conversion with expected type validation
tagEvt, err := event.ToTyped[TagReadData](rawEvt, TypeTagRead)
if err != nil {
    // Fails immediately if rawEvt.Type != "rfid/tag_read"
    return err
}

fmt.Printf("Read Tag EPC: %s\n", tagEvt.Data.EPC)
```
