# BUG: Two concurrency issues in the F-004 per-consumer channel implementation

Status: **OPEN** — two confirmed bugs introduced by the F-004 per-consumer
delivery channel feature (merged as v0.3.0, PR #2, commit `82b95a2`).
Both live in `gomqSDK/channels.go` in the `Consume` and `route` paths.

---

## Bug 1 — `Consume` panics when a pending request is resolved without data

### Finding

`Consume` performs a hard type assertion on the response payload without a
guard:

```go
// channels.go:336
consumeOK := res.Data.(protocol.ConsumeOK)
```

`Consume` receives its response through a buffered `chan response`. There are
two code paths that write into that channel for a pending `Consume`:

1. `route`'s `BasicConsumeOKType` case — `Data` is a populated `ConsumeOK`
   (normal path, no problem).
2. `handleChannelCloseOK` — resolves with `response{}` (both `Err` and `Data`
   are zero-valued) **then** calls `closeWithError`, which sends
   `response{Err: err}` to remaining pending requests. Only the first send
   lands (buffer capacity 1); the second is silently dropped. The caller
   therefore receives `Data = nil, Err = nil`.

When the second path fires (broker closes the channel while `Consume` is
waiting for `consume-ok`), the type assertion hits `nil` and panics.

### Reproduction (deterministic)

Open a channel, call `Consume` in a goroutine, and have the broker respond
with a `channel.close-ok` (echoing the same `RequestID`) instead of
`basic.consume-ok`. The calling goroutine panics immediately:

```
panic: interface conversion: interface {} is nil, not protocol.ConsumeOK
```

On v0.2.0 this did not panic — `Consume` returned `ch.Incoming` unconditionally
and never inspected `Data`.

### Impact

Any client with a `Consume` in flight (waiting for `consume-ok`) when the
channel is closed — broker crash, network drop, or explicit `ch.Close()` racing
the consume — crashes. This is a regression introduced by v0.3.0.

### Fix plan

Replace the bare type assertion with a guarded type switch so that an
unexpected or nil `Data` surfaces as a proper error instead of a panic:

```go
switch v := res.Data.(type) {
case protocol.ConsumeOK:
    // existing per-consumer / legacy logic, keyed on v.ConsumerTag
default:
    // response carried no ConsumeOK — treat as a fatal consume error
    return nil, fmt.Errorf("broker returned unexpected response to consume")
}
```

Additionally, `handleChannelCloseOK` should resolve the targeted pending
request with an error (e.g. `response{Err: fmt.Errorf("channel closed")}`),
not an empty `response{}`. This makes the close path unambiguous: callers see
an error, not a zero-value payload.

### Files to change

| File | Change |
|------|--------|
| `gomqSDK/channels.go` | Replace bare `res.Data.(protocol.ConsumeOK)` with a guarded type switch. |
| `gomqSDK/client.go` | In `handleChannelCloseOK`, resolve with `response{Err: fmt.Errorf(...)}` instead of `response{}`. |

---

## Bug 2 — Registration race: first delivery after `consume-ok` can fall back to `Incoming`

### Finding

The per-consumer channel is registered in the map **on the caller goroutine**,
*after* it receives the response from `Consume`:

```go
// channels.go:341-346 — caller goroutine (after <-respCh)
perConsumer := &consumerEntry{
    deliveries: make(chan protocol.Deliver, 100),
}
ch.mu.Lock()
ch.consumers[consumeOK.ConsumerTag] = perConsumer
ch.mu.Unlock()
return perConsumer.deliveries, nil
```

Meanwhile the `readLoop` goroutine may already be processing the next
envelope. In `route`:

```go
// channels.go:136-146 — readLoop goroutine
if delivery.ConsumerTag != "" {
    ch.mu.Lock()
    target := ch.consumers[delivery.ConsumerTag]
    ch.mu.Unlock()
    if target != nil {
        target.deliveries <- delivery
        return nil
    }
}
ch.Incoming <- delivery
```

There is no happens-before edge between the map write (caller goroutine) and
the map read (readLoop goroutine) beyond `ch.mu`. After `resolve` releases the
lock in `BasicConsumeOKType`, `readLoop` immediately reads the next envelope —
a pre-queued `basic.deliver` for the newly registered consumer. If `readLoop`
wins the race, the lookup is `nil` → delivery falls through to `ch.Incoming`.

### Reproduction (probabilistic, confirmed at iter 2 of 200)

Back-to-back `consume-ok` + `basic.deliver` (same consumer tag) on the wire.
The `basic.deliver` is already queued before the caller goroutine wakes.
Stress-test result:

```
iter 2: delivery routed to Incoming (registration race), body="first"
```

### Impact

- **Invisible today:** broker doesn't stamp `consumer_tag` on deliveries yet,
  so all deliveries go to `Incoming` anyway. The race is latent.
- **Surfaces at step 3 of F-004:** the moment the broker stamps tags in
  `dispatchLoop`, the first delivery after `consume-ok` can silently land in
  `Incoming` while the rest go to the per-consumer channel. A client range-looping
  over the returned channel will miss it — a hard-to-reproduce "some messages
  disappeared" bug.
- This is the same class of bug F-002 fixed broker-side (map access without
  ordering guarantees between writer and reader goroutines).

### Why the echo is always identical (not a tag issue)

`HandleConsume` echoes `event.ConsumerTag` verbatim (`broker/channel.go:71`),
and the SDK sends an explicit client-authoritative tag (`channels.go:322`).
Tags always match; the bug is purely a **scheduling race** between two
goroutines — not a tag-mismatch or routing-logic error.

### Fix plan

Register the per-consumer entry in `route`'s `BasicConsumeOKType` case — on
the `readLoop` goroutine, **before** `resolve` releases control and **before**
`readLoop` reads the next envelope. This gives a hard happens-before: the map
write is ordered before any subsequent envelope is processed.

Concrete approach:

1. `route`'s `BasicConsumeOKType` case: if `consumeOK.ConsumerTag` is
   non-empty, create the `consumerEntry` and insert it into `ch.consumers`
   under `ch.mu` **before** calling `ch.resolve`.
2. `resolve` now carries the created channel (as `Data`) so the caller
   goroutine in `Consume` can read it directly from `res.Data` — no map write
   needed on the caller side.
3. When `consumeOK.ConsumerTag` is empty (legacy broker), `resolve` carries
   a sentinel value (or nil) so `Consume` knows to return `ch.Incoming`.

Because the insert happens in `route` (single `readLoop` goroutine) and
readLoop processes envelopes serially, the entry is guaranteed to exist before
the next `basic.deliver` is routed. No race window.

### Files to change

| File | Change |
|------|--------|
| `gomqSDK/channels.go:route` | Move per-consumer entry creation into `BasicConsumeOKType` case, under `ch.mu`, before `resolve`. |
| `gomqSDK/channels.go:Consume` | Remove the map insert; read the channel directly from `res.Data` via guarded type switch (same change as Bug 1 fix). |

---

## Summary

| # | Bug | Severity | Reproducible today | Visible after broker stamps tags |
|---|-----|----------|-------------------|----------------------------------|
| 1 | `Consume` panic on nil `Data` | High (crash) | Yes — deterministic on any channel close while consume is pending | Yes |
| 2 | Registration race on first delivery | Medium (silent misroute) | Latent — masked by no-tag deliveries | Yes — becomes a real message-loss bug |

Both are fixed in a single refactoring: move per-consumer registration into
`route`'s `BasicConsumeOKType` case and guard the type assertion in `Consume`.
This also fixes the pre-existing panic path (Bug 1) as a side effect.

### Verification after fix

1. `go build ./...`, `go vet ./...`, `gofmt -l .` — clean.
2. `go test -race -count=3 ./gomqSDK/` — all existing tests + new tests pass.
3. The channel-close-before-consume-ok path returns an error, no panic.
4. 200-iteration stress with back-to-back `consume-ok` + `basic.deliver` —
   zero iterations where delivery lands in `Incoming`.
