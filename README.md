# GolangMQ SDK

A Go client library for [GolangMQ](https://github.com/danielkotsi/golangmq) — a lightweight, AMQP-inspired message broker with JSON-over-TCP protocol and channel multiplexing.

## Features

- **Channel multiplexing** — multiple logical channels over a single TCP connection
- **Exchange/queue routing** — declare exchanges, queues, and bind them with routing keys
- **Publish & Consume** — fire-and-forget publishing, blocking consumption via Go channels
- **Ack/Nack support** — consumer acknowledgment with optional requeue
- **Dead-letter queues** — configure dead-letter exchanges on queue declaration
- **Context-based cancellation** — all blocking operations respect `context.Context`
- **Thread-safe** — write-pump goroutine serializes all outbound frames; no external locking needed
- **Zero dependencies** — pure Go standard library

## SDK Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Client                                                          │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  writePump (goroutine)                                     │  │
│  │  └── reads from buffered writeCh, writes/flushes to TCP    │  │
│  ├────────────────────────────────────────────────────────────┤  │
│  │  readLoop (goroutine)                                      │  │
│  │  └── reads Envelope JSON lines, demux by ChannelID         │  │
│  │       ├── channel.open-ok  →  Client.handleChannelOpenOK() │  │
│  │       ├── channel.close-ok →  Client.handleChannelCloseOK()│  │
│  │       └── other            →  ch.route(env)                │  │
│  ├────────────────────────────────────────────────────────────┤  │
│  │  ClientChannel (×N, each with ID uint16)                   │  │
│  │  ├── pending map[uint16]chan response  (req/resp matching) │  │
│  │  ├── Incoming chan protocol.Deliver    (delivery stream)   │  │
│  │  ├── DeclareExchange / DeclareQueue                        │  │
│  │  ├── BindQueue                                              │  │
│  │  ├── Publish / Consume                                     │  │
│  │  └── Ack / Nack                                            │  │
│  └────────────────────────────────────────────────────────────┘  │
└───────────────────────────┬──────────────────────────────────────┘
                            ↕
          JSON-line Envelope protocol (single TCP conn)
                            ↕
┌──────────────────────────────────────────────────────────────────┐
│  GolangMQ Broker (server)                                       │
└──────────────────────────────────────────────────────────────────┘
```

## Installation

```bash
go get github.com/danielkotsi/golangMQSDK
```

## Quick Start

```go
package main

import (
    "context"
    "encoding/json"
    "log"
    "time"

    gomq "github.com/danielkotsi/golangMQSDK/gomqSDK"
    "github.com/danielkotsi/golangMQSDK/protocol"
)

func main() {
    cfg := gomq.Config{
        ClientName:   "my-app",
        Username:     "daniel",
        Password:     "123456789",
        ChannelMax:   10,
        FrameMax:     10372,
        HeartbeatSec: 10,
    }

    c, err := gomq.Connect("localhost:5672", cfg)
    if err != nil {
        log.Fatal(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    ch, _ := c.OpenChannel(ctx)
    ch.DeclareExchange("emails", ctx)
    ch.DeclareQueue("email_queue", ctx, "", "")
    ch.BindQueue("email_queue", "emails", "email.sent", ctx)

    body, _ := json.Marshal("hello world")
    ch.Publish(protocol.Publish{
        Exchange:   "emails",
        RoutingKey: "email.sent",
        Body:       body,
    })

    incoming, _ := ch.Consume("email_queue", ctx)
    for msg := range incoming {
        log.Printf("Received: %s", msg.Body)
        ch.Ack(msg.DeliveryTag)
    }
}
```

## Usage

### Connecting

`Config` holds connection parameters mirroring the AMQP 0-9-1 tune negotiation:

```go
cfg := gomq.Config{
    ClientName:   "my-client",     // identifier sent in start_ok
    Username:     "daniel",        // auth username
    Password:     "123456789",     // auth password
    ChannelMax:   10,              // max channels to negotiate
    FrameMax:     10372,           // max frame size to negotiate
    HeartbeatSec: 10,              // heartbeat interval
}

c, err := gomq.Connect("localhost:5672", cfg)
```

`Connect` dials TCP, performs the full GOMQ handshake, and starts the read-loop goroutine. The handshake exchange is:

```
Client → Server:  GOMQ/1\n
Server → Client:  connection.start
Client → Server:  connection.start_ok
Server → Client:  connection.tune
Client → Server:  connection.tune_ok
Client → Server:  connection.open
Server → Client:  connection.open_ok
```

### Opening a channel

```go
ch, err := c.OpenChannel(ctx)
```

Channels are identified by auto-incrementing `uint16` IDs. `OpenChannel` sends `channel.open`, blocks on the server's `channel.open-ok` response (or until the context expires).

### Declaring exchanges and queues

```go
ch.DeclareExchange("emails", ctx)

// With a dead-letter exchange bound to a DLQ:
ch.DeclareExchange("dlx", ctx)
ch.DeclareQueue("dlq", ctx, "", "")
ch.BindQueue("dlq", "dlx", "dead_emails", ctx)

ch.DeclareQueue("email_queue", ctx, "dlx", "dead_emails")
ch.BindQueue("email_queue", "emails", "email.sent", ctx)
```

Pass empty strings `""` for `dlxExchange` and `dlxRoutingKey` if you don't need dead-letter routing.

### Publishing messages

```go
err := ch.Publish(protocol.Publish{
    Exchange:   "emails",
    RoutingKey: "email.sent",
    Body:       body,
})
```

`Publish` is fire-and-forget — no server acknowledgment is expected.

### Consuming messages

```go
incoming, err := ch.Consume("email_queue", ctx)
for msg := range incoming {
    log.Println("Received:", string(msg.Body))
    log.Println("  Tag:      ", msg.DeliveryTag)
    log.Println("  Exchange: ", msg.Exchange)
    log.Println("  Routing:  ", msg.RoutingKey)
}
```

`Consume` returns the channel's `Incoming` channel. Messages arrive as `protocol.Deliver` structs. The loop blocks until the channel is closed (on connection shutdown) or the context expires.

### Acknowledging messages

```go
ch.Ack(msg.DeliveryTag)           // acknowledge successful processing
ch.Nack(msg.DeliveryTag, false)   // nack without requeue → dead-letter
ch.Nack(msg.DeliveryTag, true)    // nack with requeue → re-delivered
```

Both `Ack` and `Nack` are fire-and-forget.

## Example Walkthrough

Two example clients ship with the SDK under `exampleClients/`. Together they demonstrate the full lifecycle: declaration, publishing, consumption, acknowledgment, and dead-letter routing.

### Publisher (`exampleClients/publisher/main.go`)

Sets up the infrastructure and publishes three messages:

1. **Connects** as `"publisher"` with a 5-second context
2. **Declares** the `dlx` exchange and `dlq` queue, binds them with routing key `dead_emails`
3. **Declares** the `emails` exchange and `email_queue` queue (configured with DLX → `dlx` / `dead_emails`), binds with routing key `email.sent`
4. **Publishes** 3 JSON messages to `emails` / `email.sent`

Run it:

```bash
go run exampleClients/publisher/main.go
```

### Consumer (`exampleClients/consumer/main.go`)

Three workers share a single connection, each on its own channel:

| Worker | Queue | Behaviour |
|--------|-------|-----------|
| **Worker A** | `email_queue` | Acks messages; nacks (without requeue) every 3rd delivery |
| **Worker B** | `email_queue` | Same pattern — demonstrates concurrent consumption |
| **Worker C** | `dlq` (dead-letter) | Acks all messages — picks up nack'd messages from A & B |

Run it:

```bash
go run exampleClients/consumer/main.go
```

### Running the full example

```bash
# Terminal 1 — start the broker
podman run -it -p 5672:5672 danielkotsi/golangmq:latest

# Terminal 2 — start the consumer
go run exampleClients/consumer/main.go

# Terminal 3 — publish messages (run multiple times)
go run exampleClients/publisher/main.go
```

Watch the consumer output: messages tagged `%3 == 0` land in the DLQ and get picked up by Worker C.

## API Reference

### Package `github.com/danielkotsi/golangMQSDK/gomqSDK`

| Export | Description |
|--------|-------------|
| `Config` | Connection configuration: `ClientName`, `Username`, `Password`, `ChannelMax`, `FrameMax`, `HeartbeatSec` |
| `Connect(addr, cfg)` | Dial TCP, handshake, start read loop. Returns `*Client` |
| `Client.Incoming` | `chan Event` — connection-level asynchronous events |
| `Client.OpenChannel(ctx)` | Open a new logical channel. Returns `*ClientChannel` |
| `ClientChannel.Incoming` | `chan protocol.Deliver` — stream of delivered messages |
| `ClientChannel.DeclareExchange(name, ctx)` | Declare a topic exchange |
| `ClientChannel.DeclareQueue(name, ctx, dlxExchange, dlxRoutingKey)` | Declare a queue with optional dead-letter configuration |
| `ClientChannel.BindQueue(queue, exchange, routingKey, ctx)` | Bind a queue to an exchange |
| `ClientChannel.Publish(publish)` | Publish a message (fire-and-forget) |
| `ClientChannel.Consume(queue, ctx)` | Start consuming a queue. Returns `Incoming` channel |
| `ClientChannel.Ack(deliveryTag)` | Acknowledge a delivery |
| `ClientChannel.Nack(deliveryTag, requeue)` | Negative-acknowledge a delivery |
| `Queue` | Wrapper: `Name string` |
| `Event` | Connection-level event: `Type protocol.Method`, `Data any` |

### Package `github.com/danielkotsi/golangMQSDK/protocol`

| Export | Description |
|--------|-------------|
| `Envelope` | Wire frame: `ChannelID uint16`, `RequestID uint16`, `Type Method`, `Payload json.RawMessage` |
| `Publish`, `Consume`, `Deliver`, `Ack`, `Nack`, `QueueDeclare`, `ExchangeDeclare`, `QueueBind` | Method payload structs |
| `ConnectionStart`, `ConnectionStartOK`, `ConnectionTune`, `ConnectionTuneOK`, `ConnectionOpen`, `ConnectionOpenOK` | Handshake structs |
| `Error` | Broker error response: `Code string`, `Message string` |
| `ReadMessage`, `WriteMessage`, `ReadEnvelope`, `WriteMessage`, `ReadProtocolHeader`, `WriteProtocolHeader` | Low-level wire helpers |
| `NewConnectionStart`, `NewConnectionStartOK`, `NewConnectionTune`, `NewConnectionTuneOK`, `NewConnectionOpen`, `NewConnectionOpenOK` | Handshake constructors |

## Protocol

Custom JSON-over-TCP protocol with newline-delimited framing. Each frame is a single JSON object terminated by `\n`.

**Method types:**

| Category | Methods |
|----------|---------|
| Basic | `basic.publish`, `basic.deliver`, `basic.consume`, `basic.consume-ok`, `basic.ack`, `basic.nack` |
| Channel | `channel.open`, `channel.open-ok`, `channel.close`, `channel.close-ok` |
| Queue | `queue.declare`, `queue.declare-ok`, `queue.bind`, `queue.bind-ok` |
| Exchange | `exchange.declare`, `exchange.declare-ok` |
| Error | `error` |

See the [GolangMQ server repo](https://github.com/danielkotsi/golangmq) for the full protocol specification.

## Technical Stack

| Component | Detail |
|-----------|--------|
| Language | Go 1.22+ |
| Dependencies | None (stdlib only) |
| Wire format | JSON + newline-delimited framing |
| Transport | TCP |
| Concurrency | Goroutines, channels, `sync.Mutex` |
| Channel IDs | `uint16` (up to 65535 per connection) |
