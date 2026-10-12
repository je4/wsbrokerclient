# Package RESTClient (`pkg/restclient`)

`pkg/restclient` provides an HTTP REST client for interacting with WSBroker's administrative and message dispatch endpoints under `/api/v1`.

## Supported Endpoints

- `GET /api/v1/ping`: Health-check.
- `POST /api/v1/publish`: Publish standard `Event` envelopes to channels or named clients.
- `POST /api/v1/channel/:channel/publish`: Directly publish messages to a channel.
- `GET /api/v1/channels`: List active channels and subscriber counts.
- `GET /api/v1/channel/:channel/clients`: List clients subscribed to a specific channel.
- `GET /api/v1/clients`: List all connected client identifiers.
- `DELETE /api/v1/client/:name`: Forcefully disconnect a client.

## Quick Example

```go
package main

import (
    "context"
    "fmt"

    "github.com/je4/wsbrokerclient/pkg/restclient"
)

func main() {
    ctx := context.Background()

    client, err := restclient.New(restclient.Config{
        BaseURL: "https://localhost:8080",
        Bearer:  "secret-token",
    })
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Publish to channel
    resp, err := client.PublishToChannel(ctx, "alerts", "rest-service", map[string]string{
        "status": "system upgrade starting",
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Published to %d subscribers\n", resp.Subscribers)

    // Inspect channels
    channels, _ := client.GetChannels(ctx)
    for _, ch := range channels {
        fmt.Printf("Channel %s: %d clients\n", ch.Name, ch.Subscribers)
    }
}
```
