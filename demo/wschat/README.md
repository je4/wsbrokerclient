# WSChat Demo Application

`wschat` is a reference demo application that demonstrates how to integrate and use the **`wsbrokerclient`** Go library in a real-world web service. It features an interactive, HTMX-powered chat web interface with dynamic channel management, mutual TLS (mTLS) authentication via `certloader`, and automatic reconnection handling.

---

## Table of Contents

- [Overview](#overview)
- [Key `wsbrokerclient` Concepts Demonstrated](#key-wsbrokerclient-concepts-demonstrated)
  - [1. Client Configuration and Initialization](#1-client-configuration-and-initialization)
  - [2. mTLS Security via `certloader`](#2-mtls-security-via-certloader)
  - [3. Connection Lifecycle Callbacks](#3-connection-lifecycle-callbacks)
  - [4. Inbound Message Handling & Echo Filtering](#4-inbound-message-handling--echo-filtering)
  - [5. Message Publishing](#5-message-publishing)
  - [6. Dynamic Channel Subscription (`Attach`)](#6-dynamic-channel-subscription-attach)
  - [7. Graceful Termination](#7-graceful-termination)
- [Architecture](#architecture)
- [Configuration Reference (`wschat.toml`)](#configuration-reference-wschattoml)
- [Running the Demo](#running-the-demo)
- [Testing](#testing)

---

## Overview

The `wschat` demo shows how `wsbrokerclient` simplifies WebSocket communication with [WSBroker](https://github.com/je4/wsbroker) by abstracting low-level protocol concerns such as:

- Automatic reconnects with exponential backoff and jitter.
- Automatic channel resubscription upon reconnect.
- Dynamic mTLS client certificate reloading (via MiniVault, File, Dev, Env, or ACME).
- Thread-safe pub/sub messaging and asynchronous event dispatching.

---

## Key `wsbrokerclient` Concepts Demonstrated

### 1. Client Configuration and Initialization

In `demo/wschat/client.go`, `wsbrokerclient` is instantiated using `DefaultConfig()` and customized for the application's needs:

```go
cfg := wsbrokerclient.DefaultConfig()
cfg.Endpoint = conf.WSBroker
cfg.ClientName = conf.Name
cfg.InitialChannels = conf.Channels
cfg.AutoReconnect = true
cfg.ReconnectInterval = 1 * time.Second
cfg.MaxReconnectInterval = 10 * time.Second
cfg.Logger = logger

if conf.ClientTLS.Type != "" {
    cfg.TLSCertLoader = &conf.ClientTLS
}

wsClient, err := wsbrokerclient.New(cfg)
```

- **`Endpoint`**: Target WebSocket broker URL (e.g. `https://localhost:8765` or `wss://...`).
- **`ClientName`**: Unique client identifier used for broker authorization and sender identification.
- **`InitialChannels`**: List of channels to subscribe to immediately upon connecting.
- **`AutoReconnect`**: When `true`, enables resilient background retries if the broker restarts or the network drops.

---

### 2. mTLS Security via `certloader`

`wsbrokerclient` natively integrates with [`go.ub.unibas.ch/cloud/certloader/v2`](https://go.ub.unibas.ch/cloud/certloader). In `wschat.toml`, certificates and policies are defined with URI Subject Alternative Names (SANs) for client and channel authorization:

```toml
[clienttls]
type = "minivault"

[clienttls.minivault]
baseurl = "https://localhost:8759/api/v1"
parenttoken = "pa.22f09dfd1cbfbe9f.c17b29cb2b730533"
tokentype = "client_cert"
tokenpolicies = ["wsbroker.client"]
certtype = "client_cert"
uris = ["wsbroker:client:test01", "wsbroker:channel:all", "wsbroker:channel:intern"]
ca = ["certs/minivault.ca.pem"]
usesystempool = false
```

`certloader` automatically handles dynamic certificate retrieval, renewal, and CA pool management.

---

### 3. Connection Lifecycle Callbacks

`wsbrokerclient` provides lifecycle hooks that allow applications to react to connection state changes and keep local state / UI synchronized:

```go
// Triggered on successful WebSocket connection
c.client.OnConnect(func() {
    c.store.SetStatus("Connected")
})

// Triggered when connection is lost
c.client.OnDisconnect(func(err error) {
    c.store.SetStatus("Disconnected")
})

// Triggered after automatic reconnection succeeds
c.client.OnReconnect(func() {
    c.store.SetStatus("Connected")
})

// Triggered on communication or reconnect errors
c.client.OnError(func(err error) {
    if c.client.GetState() == wsbrokerclient.StateReconnecting {
        c.store.SetStatus("Reconnecting")
    }
})
```

---

### 4. Inbound Message Handling & Echo Filtering

Incoming messages from all subscribed channels are received via `OnMessage`:

```go
c.client.OnMessage(func(evt *event.Event) {
    channel := evt.Target
    if channel == "" {
        channel = evt.Source
    }

    content, err := evt.GetStringData()
    if err != nil {
        content = fmt.Sprintf("%v", evt.Data)
    }

    // Skip self-sent messages to avoid duplicate display in UI
    if evt.Source == c.config.Name {
        return
    }

    c.store.AddMessage(ChatMessage{
        ID:        fmt.Sprintf("%d-%s", time.Now().UnixNano(), evt.Source),
        Sender:    evt.Source,
        Channel:   channel,
        Content:   content,
        Timestamp: time.Now(),
    })
})
```

---

### 5. Message Publishing

Publishing messages to a specific channel is performed via `client.SendMessage`:

```go
err := c.client.SendMessage(ctx, channel, content)
```

For structured data (e.g. JSON or Go structs), `client.Broadcast(ctx, channel, payload)` is also available.

---

### 6. Dynamic Channel Subscription (`Attach`)

Clients can dynamically join new channels at runtime without reconnecting:

```go
err := c.client.Attach(ctx, channel)
```

`wsbrokerclient` maintains the channel subscription list in memory and automatically re-attaches to all active channels when reconnecting.

---

### 7. Graceful Termination

To close WebSocket connections cleanly without resource leaks during application shutdown:

```go
if err := chatClient.Close(); err != nil {
    logger.Error().Err(err).Msg("error closing chat client")
}
```

---

## Architecture

```text
Browser (HTMX UI)
      │
      │ HTTPS (localhost:random_port)
      ▼
Server (demo/wschat/server.go + handlers.go)
      │
      ├──> MessageStore (in-memory state & history)
      │
      └──> ChatClient (demo/wschat/client.go)
                 │
                 │ mTLS WebSocket (WSS)
                 ▼
          WSBroker Server
```

---

## Configuration Reference (`wschat.toml`)

| Key | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Unique client identifier (e.g. `"test01"`). |
| `wsbroker` | `string` | WSBroker URL (e.g. `"https://localhost:8765"`). |
| `channels` | `[]string` | Initial channels to subscribe to (e.g. `["all", "intern"]`). |
| `clienttls` | `table` | `certloader` mTLS configuration for WSBroker connection. |
| `webtls` | `table` | TLS configuration for the local web UI HTTPS server. |

---

## Running the Demo

To run the application with default embedded configuration:

```bash
go run ./cmd/wschat
```

To run with a custom configuration file:

```bash
go run ./cmd/wschat -config ./demo/config/wschat.toml
```

Upon startup, the console displays the bound URL (e.g. `https://localhost:54321`). Open the URL in your web browser to access the chat interface.

---

## Testing

Run unit tests for the demo package:

```bash
go test -v ./demo/wschat
```
