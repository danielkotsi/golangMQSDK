# F-004 — SDK Implementation Guide (per-consumer delivery channels)

Status of this guide: **planning aid for the SDK repo** (`github.com/danielkotsi/golangMQSDK`).

This document covers **only the SDK side** of fixing finding **F-004**
("multiple consumers per channel cannot work end to end"). The broker-side
work is out of scope here and is described in `reports/findings/F-004.md`
(Step 1). This guide maps every SDK file/function that must change, why, and
how, with before/after snippets against **v0.2.0**.

---

## 1. The one-paragraph contract

After the fix, **each `Consume` call returns a dedicated delivery channel, and
every `basic.deliver` is routed to the consumer it belongs to.** Two consumers
on the same channel get two distinct channels; there is no shared merged
stream.

The wire struct `Deliver` gains a `consumer_tag` field. The SDK:

1. sends an explicit (client-authoritative) `consumer_tag` in `basic.consume`;
2. on `basic.consume-ok`, allocates a **fresh per-consumer channel**, registers
   it under the broker-echoed tag, and returns it;
3. in `route`, forwards each `basic.deliver` to the per-consumer channel keyed
   by `delivery.ConsumerTag`, falling back to the shared `Incoming` when the tag
   is absent (old broker / legacy delivery).

**Backwards compatibility rule:** a per-consumer channel is only created when
the broker actually echoes a non-empty tag. When the broker echoes an empty tag
(old broker), `Consume` returns `ch.Incoming` exactly as v0.2.0 did. This is
what keeps every mixed-version broker/SDK combination working (see §6).

---

## 2. Files & functions to change (summary)

| File | Function / symbol | Change |
|------|-------------------|--------|
| `protocol/events.go` | `Deliver` struct (line 92) | add `ConsumerTag` field |
| `gomqSDK/channels.go` | `ClientChannel` struct (line 17) | add `consumers` map + `consumerSeq` |
| `gomqSDK/channels.go` | `newClientChannel` (line 28) | initialise `consumers` |
| `gomqSDK/channels.go` | `Consume` (line 266) | client-authoritative tag + per-consumer chan |
| `gomqSDK/channels.go` | `route` (line 104, `BasicDeliverType` case 106-112) | route by `ConsumerTag`, fallback to `Incoming` |
| `gomqSDK/channels.go` | `closeWithError` (line 68) | close all per-consumer chans exactly once, clear map |
| `gomqSDK/client.go` | (no change) | `Client.Close` already fans out to `closeWithError` |

Version bump: **v0.3.0** (wire field addition ⇒ minor).

---

## 3. Wire struct — `protocol/events.go`

### `Deliver` (currently lines 92-97)

```go
// before
type Deliver struct {
	DeliveryTag uint16 `json:"delivery_tag"`
	Body        []byte `json:"body"`
	Exchange    string `json:"exchange"`
	RoutingKey  string `json:"routing_key"`
}

// after
type Deliver struct {
	DeliveryTag uint16 `json:"delivery_tag"`
	ConsumerTag string `json:"consumer_tag,omitempty"` // NEW
	Body        []byte `json:"body"`
	Exchange    string `json:"exchange"`
	RoutingKey  string `json:"routing_key"`
}
```

Rules:

- Must be byte-identical to the broker's `Deliver` (both repos carry a copy of
  this struct — keep the two in sync).
- Keep `json:"consumer_tag,omitempty"`: an old broker that never stamps the tag
  still produces well-formed envelopes, and the field is simply empty.

No other struct in `protocol/events.go` changes. `Consume` (line ~74) and
`ConsumeOK` (line ~79) **already carry** `ConsumerTag` — good, no change there.

---

## 4. Channel state — `gomqSDK/channels.go`

### 4.1 `ClientChannel` struct (line 17)

Add a per-consumer registry and a tag sequence counter:

```go
// after
type ClientChannel struct {
	id uint16

	mu        sync.Mutex
	pending   map[uint16]chan response
	closeOnce sync.Once

	Incoming    chan protocol.Deliver                    // legacy shared sink (fallback)
	consumers   map[string]chan protocol.Deliver         // tag -> per-consumer chan
	consumerSeq uint16                                   // for client-authoritative tags

	client *Client
}
```

Concurrency contract for `consumers`:

- **written** by `Consume` (caller goroutine), **read** by `route`
  (the client's `readLoop` goroutine) and **cleared** by `closeWithError`
  (can be either goroutine).
- All access to the map **and** `consumerSeq` must hold `ch.mu`. Do not read
  the map in `route` without the lock — this is exactly the class of bug F-002
  fixed broker-side.

### 4.2 `newClientChannel` (line 28)

```go
// after
func newClientChannel(id uint16, client *Client) *ClientChannel {
	return &ClientChannel{
		id:      id,
		pending: make(map[uint16]chan response),
		client:  client,
		// i will need to reconsider the buffer here
		Incoming:  make(chan protocol.Deliver, 100),
		consumers: make(map[string]chan protocol.Deliver), // NEW
	}
}
```

Per-consumer channels use the same buffer size as `Incoming` (100), keeping the
same back-pressure profile as today.

---

## 5. Behaviour changes — `gomqSDK/channels.go`

### 5.1 `Consume` (line 266) — client-authoritative tag + per-consumer chan

Helper to mint a tag (under `ch.mu`):

```go
func (ch *ClientChannel) nextConsumerTag() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.consumerSeq++
	return fmt.Sprintf("client-%d-consumer-%d", ch.id, ch.consumerSeq)
}
```

Why client-authoritative (recommended in F-004): the broker already rejects
duplicate tags on a channel (`broker/broker.go:88-89`), so the client gets a
deterministic, unique tag and duplicate tags surface as errors — no reliance on
broker generation.

Rewritten `Consume` (signature stays `(chan protocol.Deliver, error)` —
source-compatible):

```go
// after
func (ch *ClientChannel) Consume(queuename string, ctx context.Context) (chan protocol.Deliver, error) {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	tag := ch.nextConsumerTag()
	if err := ch.client.writeChannelEnvelope(ch.id, protocol.BasicConsumeType, reqID, protocol.Consume{
		Queue:       queuename,
		ConsumerTag: tag,
	}); err != nil {
		ch.unRegisterREQ(reqID)
		return nil, err
	}

	select {
	case res := <-respCh:
		if res.Err != nil {
			return nil, res.Err
		}
		consumeOK := res.Data.(protocol.ConsumeOK)

		// Old broker / pre-tag protocol: nothing to key on, behave exactly
		// like v0.2.0. This fallback is what makes the compatibility matrix
		// (§6) hold.
		if consumeOK.ConsumerTag == "" {
			return ch.Incoming, nil
		}

		perConsumer := make(chan protocol.Deliver, 100)
		ch.mu.Lock()
		ch.consumers[consumeOK.ConsumerTag] = perConsumer
		ch.mu.Unlock()
		return perConsumer, nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return nil, ctx.Err()
	}
}
```

Notes:

- **Duplicate explicit tag:** if a caller on the same channel ever reuses a tag
  (not possible via this API since tags are auto-minted, but reachable if the
  tag logic changes), the broker rejects the `basic.consume` with an error and
  `Consume` returns it — nothing left registered client-side.
- The map is keyed by the **broker-echoed** tag, not the client tag. They are
  equal for the current broker (it registers and echoes the client-supplied
  tag verbatim), but keying on the echo is the safe choice if the broker ever
  normalises tags.
- On the `ctx.Done()` path, `unRegisterREQ` already exists — keep it.

### 5.2 `route` (line 104) — route by `ConsumerTag`, fall back to `Incoming`

Current `BasicDeliverType` case (lines 106-112):

```go
case protocol.BasicDeliverType:
	var delivery protocol.Deliver
	err := json.Unmarshal(env.Payload, &delivery)
	if err != nil {
		return err
	}
	ch.Incoming <- delivery
	return nil
```

After:

```go
case protocol.BasicDeliverType:
	var delivery protocol.Deliver
	if err := json.Unmarshal(env.Payload, &delivery); err != nil {
		return err
	}

	if delivery.ConsumerTag != "" {
		ch.mu.Lock()
		target := ch.consumers[delivery.ConsumerTag]
		ch.mu.Unlock()
		if target != nil {
			target <- delivery
			return nil
		}
		// Unknown tag: fall through to the legacy shared sink.
	}

	ch.Incoming <- delivery
	return nil
```

Flow for the four combinations:

| `delivery.ConsumerTag` | found in `ch.consumers` | result |
|------------------------|-------------------------|--------|
| `""` (old broker) | — | → `Incoming` (legacy, unchanged) |
| non-empty, present | yes | → per-consumer chan (new path) |
| non-empty, absent | no | → `Incoming` (safe fallback; also the "new SDK + old broker" row) |

A delivery routes to **exactly one** channel — no double delivery by
construction.

### 5.3 `closeWithError` (line 68) — close per-consumer chans exactly once

Current behaviour: resolves pending requests and closes `ch.Incoming` via
`closeOnce`. It is already idempotent and may run on both the `readLoop`
goroutine (channel close) and a caller goroutine (`Client.Close`) — see §7.

After: also close every per-consumer channel **exactly once** and clear the
map. Because `closeWithError` is idempotent and re-entrant, each per-consumer
chan needs its own once-guard. The cleanest way is to make the map value a
small holder:

```go
// option A: per-entry once (recommended)
type consumerEntry struct {
	deliveries chan protocol.Deliver
	closeOnce  sync.Once
}

// map becomes: consumers map[string]*consumerEntry
```

```go
// after
func (ch *ClientChannel) closeWithError(err error) {
	ch.mu.Lock()
	for reqID, respCH := range ch.pending {
		delete(ch.pending, reqID)
		respCH <- response{Err: err}
	}

	entries := make([]*consumerEntry, 0, len(ch.consumers))
	for _, e := range ch.consumers {
		entries = append(entries, e)
	}
	ch.consumers = make(map[string]*consumerEntry) // clear; route sees nil -> Incoming fallback
	ch.mu.Unlock()

	for _, e := range entries {
		e.closeOnce.Do(func() {
			close(e.deliveries)
		})
	}

	ch.closeOnce.Do(func() {
		close(ch.Incoming)
	})
}
```

If you keep the map as `map[string]chan protocol.Deliver` (option B, no holder
struct), ensure the clearing happens under `ch.mu` and accept that a late
`route` that already captured a chan reference may send to a closing chan — see
§7 hazard note.

---

## 6. Compatibility matrix (SDK behaviour per broker version)

| Broker | SDK | `basic.deliver` carries tag? | SDK behaviour after fix |
|--------|-----|------------------------------|-------------------------|
| old | new (v0.3.0) | no | `Consume` echoes empty tag → returns `ch.Incoming`; `route` sees `""` → `Incoming`. **Unchanged from v0.2.0.** |
| new | new (v0.3.0) | yes | per-consumer chans; feature fully on |

The feature activates **only when both sides are new**. The `Consume` empty-tag
fallback (§5.1) and the `route` fallback (§5.2) together guarantee zero change
for any old-broker/new-SDK mixture — there is no broken intermediate state from
the SDK's perspective.

---

## 7. Concurrency & lifecycle notes

These matter because F-002 proved the broker's bookkeeping was racy and the
same patterns apply here.

- **`ch.mu` guards the map.** `route` (readLoop goroutine) and `Consume`
  (caller goroutine) must both take `ch.mu` for any `consumers` access. Never
  iterate or index the map lock-free.
- **`closeWithError` clears the map under `ch.mu`**, so a `route` arriving
  after close sees `nil` and falls back to `Incoming` — and `Incoming` is then
  closed by `closeOnce`, so that send fails only if it already happened. This
  mirrors today's behaviour.
- **Known pre-existing hazard (not introduced here, but be aware):** the SDK
  uses `close(ch.Incoming)` while `route` may still be blocked in
  `ch.Incoming <- delivery`. A send on a closed channel panics. The current
  code already has this shape for `Incoming`; the per-consumer chans inherit
  it. If you want to harden, replace the 100-buffer send with a guarded send,
  e.g.:

  ```go
  // optional hardening for the per-consumer send
  select {
  case e.deliveries <- delivery:
  case <-e.closed: // closed alongside closeOnce; never send on a closed chan
  }
  ```

  This is out of scope for the minimal F-004 fix but worth a follow-up ticket.
- **`Client.Close` needs no change.** It snapshots `c.channels`, calls
  `closeWithError` on each, and clears `c.channels` (`client.go`). Every per-
  consumer chan is closed by the new `closeWithError` body automatically.

---

## 8. What NOT to change (SDK)

- `Consume`'s signature `(chan protocol.Deliver, error)` — stays, for source
  compatibility.
- `Ack` / `Nack` — they already address acks by `DeliveryTag`, and the broker
  matches them per-consumer internally. No consumer tag travels in
  `basic.ack`/`basic.nack`.
- `protocol.Consume` and `protocol.ConsumeOK` — already carry `ConsumerTag`.
- `client.go` (`Client.Close`, `readLoop`, `handleChannelCloseOK`) — no edit
  required (§7).
- `newClientChannel` buffer size of `Incoming` (100) — keep parity for the
  per-consumer chans.

---

## 9. Delivery-tag ambiguity (known limitation — do NOT fix here)

Delivery tags are allocated per queue, so two consumers on different queues of
the same channel can hold the **same delivery tag number** at the same time.
`Ack`/`Nack` match by delivery tag only. This was unreachable with one consumer
per channel; now that multiple consumers per channel is real, it becomes
reachable. Fixing it (consumer tag in `basic.ack`/`basic.nack`, or globally
unique per-channel tags) is a documented follow-up, explicitly **out of scope**
for F-004 (`reports/findings/F-004.md`, "Non-goals / deferred").

---

## 10. Suggested verification (SDK repo)

With only the SDK repo checked out and the fixed broker available:

1. `go build ./...` and `go vet ./...` clean on v0.3.0.
2. Two `Consume` calls on one channel return channels that are `!=` (compare
   with `==`) and distinct **and** never share a delivery.
3. Publish to queue A then queue B on the same channel; each consumer receives
   only its own queue's messages (`Deliver.ConsumerTag` matches the `Consume`).
4. Nack with requeue on consumer A's delivery → the redelivery comes back only
   on consumer A's channel.
5. Duplicate explicit tag → `Consume` returns the broker's error.
6. Closing the channel/client closes every per-consumer channel (range over it
   ends) and `Incoming`.
7. Old-broker regression: with a pre-fix broker, `Consume` still returns
   `ch.Incoming` and delivery behaviour is identical to v0.2.0.
8. Re-run with `-race` — no data races on `consumers`.

---

## 11. Release checklist

1. `protocol/events.go` — `Deliver.ConsumerTag` added (mirror of broker).
2. `channels.go` — struct + `newClientChannel` + `Consume` + `route` +
   `closeWithError` changed per §§4-5.
3. `go test -race` green in the SDK repo.
4. Tag and publish **v0.3.0** (wire-field addition ⇒ minor bump).
5. Then (separate repo, separate task): bump the broker repo's `go.mod` to
   v0.3.0 and adjust the test harness — see `reports/findings/F-004.md`, Step 3.