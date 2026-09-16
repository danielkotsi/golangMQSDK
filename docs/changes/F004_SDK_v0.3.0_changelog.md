# SDK v0.3.0 — F-004 Implementation & Bug Fixes

**Branch:** `dkotsi/MultipleConsumersPerChannel`  
**PR:** #2 (vs `main`)  
**Intended release tag:** v0.3.0 (post-merge)  
**Commits:** 7 (3 feature + 4 fix/docs)

---

## Overview

This document summarizes all changes made to the SDK repository
(`golangMQSDK`) across PR #2: the per-consumer delivery channel feature
(F-004), two concurrency bugs found during review, a pre-existing
nil-pointer panic fix, and the corresponding tests and documentation.

---

## Changes by file

### `protocol/events.go`

**Added `ConsumerTag` to the `Deliver` wire struct.**

```go
type Deliver struct {
    DeliveryTag uint16 `json:"delivery_tag"`
    ConsumerTag string `json:"consumer_tag,omitempty"` // NEW
    Body        []byte `json:"body"`
    Exchange    string `json:"exchange"`
    RoutingKey  string `json:"routing_key"`
}
```

`omitempty` keeps envelopes written by old brokers (which never stamp
the tag) well-formed. This is the SDK mirror of the identical
broker-side struct — the two repos must stay in sync.

---

### `gomqSDK/channels.go`

Per-consumer delivery channel infrastructure and routing.

**1. `consumerEntry` type** — holds a `chan protocol.Deliver` paired
with its own `sync.Once`, so each channel is closed exactly once even
when `closeWithError` runs concurrently.

**2. `ClientChannel` struct** — gains:
- `consumers map[string]*consumerEntry` — registry keyed by
  broker-echoed consumer tag
- `consumerSeq uint16` — counter for minting client-authoritative tags

**3. `newClientChannel`** — initialises the `consumers` map.

**4. `nextConsumerTag()`** — mints deterministic, channel-unique tags
(`client-<ch>-consumer-<n>`) under `ch.mu`. The broker rejects
duplicate tags, so a fresh tag per `Consume` guarantees uniqueness.

**5. `route` — `BasicConsumeOKType` case** — creates and registers the
per-consumer channel here, on the readLoop goroutine, before `resolve`.
This is the fix for the registration race (Bug 2): readLoop processes
envelopes serially, so a subsequent `basic.deliver` can never be
routed before its entry exists. The channel is passed to the caller
through `res.Data`.

**6. `Consume`** — sends a client-authoritative tag in
`protocol.Consume`, reads the per-consumer channel from `res.Data` via
a guarded type switch:
- `case chan protocol.Deliver` → return it (new path)
- `case nil` → legacy broker, return `ch.Incoming` (v0.2.0 behaviour)
- `default` → return error, not panic (Bug 1 fix)

Signature unchanged (`(chan protocol.Deliver, error)`) —
source-compatible.

**7. `route` — `BasicDeliverType` case** — forwards each delivery to
the per-consumer channel keyed by `delivery.ConsumerTag` under
`ch.mu`. Falls back to `ch.Incoming` for empty/unknown tags.

**8. `closeWithError`** — snapshots and clears `consumers` under
`ch.mu`, closes every per-consumer channel through its own `closeOnce`,
then closes `Incoming` via the existing `closeOnce`. Idempotent and
safe to call concurrently.

---

### `gomqSDK/client.go`

**1. `handleChannelCloseOK`** — resolves the targeted pending request
with `response{Err: fmt.Errorf("channel closed")}` instead of
`response{}`. This ensures no caller (Consume, DeclareQueue,
DeclareExchange, Close) sees a zero-valued `Data` payload. Side effect:
`ClientChannel.Close` now surfaces `"channel closed"` on close-ok.

**2. `OpenChannel`** — the `ctx.Done()` branch now calls
`clientCh.unRegisterREQ(reqID)` instead of `ch.unRegisterREQ(reqID)`,
fixing a nil-pointer panic where `ch` was the uninitialised named
return.

---

### `gomqSDK/smoke_test.go`

- `TestClientChannelClose` — updated to expect the new `"channel closed"`
  error from `ch.Close()` (follows from `handleChannelCloseOK` fix).
- `TestOpenChannelExpiredContextReturnsDeadlineExceeded` — new regression
  test: expired-context `OpenChannel` returns
  `context.DeadlineExceeded` with no panic and no channel leak.

---

### `gomqSDK/multiconsumer_test.go` (new)

Wire-level tests against the `fakeBroker` harness:

| Test | What it covers |
|------|---------------|
| `TestF004_ConsumeReturnsDistinctChannels` | Two `Consume` calls return distinct channels; tag-routed deliveries land correctly |
| `TestF004_LegacyBrokerFallsBackToIncoming` | Empty echoed tag → `ch.Incoming` returned; untagged delivery arrives there |
| `TestF004_UnknownConsumerTagFallsBackToIncoming` | Ghost tag → falls through to `Incoming`, doesn't leak to per-consumer channel |
| `TestF004_DuplicateTagSurfacedAsError` | Broker reject → error surfaced, no residual registration |
| `TestF004_CloseClosesPerConsumerChannels` | `Client.Close` closes every per-consumer channel and `Incoming` |
| `TestF004_CloseWhileConsumePendingReturnsError` | Channel close during pending consume → error, no panic (Bug 1 regression) |
| `TestF004_FirstDeliveryAfterConsumeOKNeverFallsBackToIncoming` | 100 iterations of back-to-back consume-ok + deliver → always routes to per-consumer channel (Bug 2 regression) |

---

### `docs/`

| File | Description |
|------|-------------|
| `F-004.md` | Original finding document |
| `F004_SDK_MULTI_CONSUMER.md` | SDK implementation guide |
| `reviews/BUG_F004_consume_two_bugs.md` | Review identifying two concurrency bugs |
| `reviews/BUG_OpenChannel_timeout_nil_pointer.md` | Review identifying the nil-pointer panic |
| `changes/F004_SDK_v0.3.0_changelog.md` | This file |

---

## Behaviour / compatibility matrix

| Broker | SDK | `basic.deliver` carries tag? | Result |
|--------|-----|------------------------------|--------|
| old | old (v0.2.0) | no | shared `Incoming`; unchanged |
| new | old | yes (ignored) | shared `Incoming`; multi-consumer indistinguishable |
| old | new (v0.3.0) | no | tag `""` → fallback to `Incoming`; unchanged |
| new | new (v0.3.0) | yes | **per-consumer channels; feature fully on** |

---

## Verification

```
gofmt -l .          # clean
go build ./...      # ok
go vet ./...        # ok
go test -race -count=3 ./gomqSDK/   # ok — all tests green
```

---

## Deferred (not in this PR)

- Broker-side wire stamping (`GolangRabbitMQBroker` repo — Step 1)
- `go.mod` SDK v0.3.0 bump + harness refactor + integration tests
  (Step 3)
- Delivery-tag ambiguity across queues on one channel
- Backpressure / client-settable prefetch
