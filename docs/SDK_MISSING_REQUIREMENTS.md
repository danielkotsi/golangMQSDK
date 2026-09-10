# golangMQSDK — Missing Requirements

This document catalogs the **gaps currently missing from the golangMQSDK** that must
be implemented before the integration tests (Phase 1 of the broker's testing plan)
can be written and run. Implement each item, then wire the updated SDK back into the
broker repo (bump the module version in `go.mod` or add a `replace` directive to a
local clone).

---

## B1. Add an exported `Client.Close()` method

**Missing:** in `gomqSDK/client.go`, the `Client` struct holds an unexported
`conn net.Conn`, a private `closed chan struct{}`, and a private `shutdown()`
(called only by `closeOnce`). There is **no exported method** to close the
connection. The `readLoop` only tears down when the server closes the socket or a
read error occurs.

**Why needed:** the disconnect test suites (consumer-with-unacked-messages,
producer-disconnect) must intentionally close a client connection and observe the
broker's re-enqueue/redelivery behavior. Tests currently have no way to close a
`*gomqSDK.Client` cleanly.

**Suggested implementation** — add an exported, idempotent method on `*Client`:

```go
// Close terminates the connection. Safe to call multiple times; idempotent.
func (c *Client) Close() error {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.shutdown()        // closes c.closed via closeOnce
    if c.conn != nil {
        err := c.conn.Close()
        c.conn = nil
        return err
    }
    return nil
}
```

Consider whether `Close()` should also:
- close each channel's `Incoming` channel, and
- drain/signal each `pending` request channel so any caller blocked in
  `DeclareQueue`/`DeclareExchange`/`BindQueue`/`Consume`/`OpenChannel` unblocks with
  an error instead of hanging.

**Acceptance criteria:**
- Calling `Close()` on a connected client closes the underlying TCP socket.
- Calling `Close()` twice is safe (no panic, no double-close).
- `go vet ./...` and `go build ./...` pass.

---

## B2. Add an exported `ClientChannel.Close()` method and implement `handleChannelCloseOK`

**Missing:** the SDK can open channels (`OpenChannel`) but has **no exported method
to close a channel**. `Client.handleChannelCloseOK` in `client.go` is an empty stub
(the broker sends `channel.close-ok` in response, but the SDK never resolves the
request or cleans up the channel).

**Why needed:** the `channel_test.go` suite verifies that closing a channel
unregisters its consumer and re-enqueues pending messages. The test must be able to
close a channel from the client side.

**Suggested implementation** — add an exported method on `*ClientChannel`:

```go
// Close closes the channel on the broker and cleans up local state.
func (ch *ClientChannel) Close(ctx context.Context) error {
    reqID := ch.client.nextRequestID()
    respCh := ch.registerREQ(reqID)
    if err := ch.client.writeEnvelope(protocol.ChannelCloseType, reqID,
        protocol.ChannelClose{ID: ch.id}); err != nil {
        return err
    }
    select {
    case res := <-respCh:
        return res.Err
    case <-ctx.Done():
        ch.unRegisterREQ(reqID)
        return ctx.Err()
    }
}
```

Also implement `handleChannelCloseOK` so the response resolves the request, and
remove the channel from the client's `channels` map on close.

**Acceptance criteria:**
- `handleChannelCloseOK` no longer empty: it resolves the pending request.
- After `channel.Close()`, the channel is removed from `Client.channels`.
- `go vet ./...` and `go build ./...` pass.

---

## B3. Graceful behavior on use-after-close (no hangs)

**Optional but strongly recommended.**

**Missing:** after `Close()`, the `send` path returns `"connection closed"` via
`<-c.closed`, but in-flight blocking calls (e.g. `OpenChannel`, `DeclareQueue`,
`Consume`) that already registered a `pending` request channel may block forever if
that request channel is never resolved.

**Why needed:** the disconnect tests close a client while operations may be
in-flight; without graceful teardown, tests can hang rather than observe an error.

**Suggested implementation:**
- Make `Close()` iterate `c.channels`, and for each channel, signal/close each
  `pending` request channel (or drain it) so blocked callers return an error.
- Confirm `Connect`-returned clients and any in-flight `OpenChannel`/declare/consume
  calls return an error (not a hang) after `Close()`.

**Acceptance criteria:**
- A goroutine blocked in `DeclareQueue`/`Consume`/`OpenChannel` unblocks (with an
  error) when the client is closed from another goroutine.
- No goroutine leaks in tests that close clients.

---

## Summary checklist

| ID | Item | Blocks test suite |
|----|------|-------------------|
| B1 | Export `Client.Close()` (idempotent, closes socket) | `disconnect_test.go` |
| B2 | Export `ClientChannel.Close()` + implement `handleChannelCloseOK` | `channel_test.go` |
| B3 | Graceful use-after-close (unblock pending requests, no hangs) | `disconnect_test.go` |

After implementing B1–B3, publish/bump the module version or use a `replace`
directive in the broker's `go.mod` so the broker repo compiles against the updated
SDK.
