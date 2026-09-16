# BUG: `OpenChannel` nil-pointer panics when the context expires

Status: **OPEN** — pre-existing, NOT introduced by the F-004 PR (v0.3.0).
Found while reviewing the same code path. Introduced by the graceful-close
work in PR #1 (commit `dd221e5`); present in v0.2.0 and v0.3.0.

---

## Finding

`OpenChannel` announces a named return value but the initialized local channel
variable is a different one. The context-cancellation branch dereferences the
wrong variable:

```go
// client.go:185
func (c *Client) OpenChannel(ctx context.Context) (ch *ClientChannel, err error) {
	...
	case <-ctx.Done():                       // client.go:212
		delete(c.channels, id)
		ch.unRegisterREQ(reqID)              // BUG: ch is the still-nil named return
		c.writeEnvelope(protocol.ChannelCloseType, c.nextRequestID(), protocol.ChannelClose{ID: id})
		return nil, ctx.Err()
}
```

`ch` is a named return that is never assigned before this branch, so it is
`nil` at that point; `clientCh` (line 188) is the initialized local channel.
Calling `ch.unRegisterREQ(reqID)` ends up calling `ch.mu.Lock()` on a nil
receiver.

## Reproduction (deterministic)

Call `OpenChannel` against the fake broker (or any broker that does not answer
`channel.open`) with a short context timeout:

```
--- FAIL: TestReview_OpenChannelTimeout (0.05s)
panic: runtime error: invalid memory address or nil pointer dereference
    gomqSDK.(*ClientChannel).unRegisterREQ(0x0, 0x1)
    gomqSDK.(*Client).OpenChannel(0xc00012a000, {0x65d860, 0xc000228070})
        gomqSDK/client.go:214
```

## Impact

High (crash on a routine usage pattern). Any client that uses a context with a
deadline/timeout on `OpenChannel` — normal practice — hits a hard crash instead
of receiving `context.DeadlineExceeded` or a channel-open error.

## Fix plan

One-line fix: call `clientCh.unRegisterREQ(reqID)`.

```go
case <-ctx.Done():
	delete(c.channels, id)
	clientCh.unRegisterREQ(reqID) // was: ch.unRegisterREQ(reqID)
	c.writeEnvelope(protocol.ChannelCloseType, c.nextRequestID(), protocol.ChannelClose{ID: id})
	return nil, ctx.Err()
```

### Files to change

| File | Change |
|------|--------|
| `gomqSDK/client.go:214` | `ch.unRegisterREQ(reqID)` → `clientCh.unRegisterREQ(reqID)`. |

---

## Verification after fix

1. `go build ./...`, `go vet ./...`, `gofmt -l .` — clean.
2. Existing `go test -race ./gomqSDK/` suites still pass.
3. New regression test: `OpenChannel` with an expired context returns
   `context.DeadlineExceeded` (no panic).