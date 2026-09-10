# Future Improvements

General issues, technical debt, and enhancements discovered during code reviews. This file is updated as the project progresses — items are added and removed as they are addressed.

---

## Concurrency & Safety

### F1 — Race on `Client.Close()` (concurrent / double-call)

`Close()` calls `c.shutdown()` (guarded by `closeOnce`), then grabs `c.mu` to snapshot the channel map. Between the `shutdown()` call and the `mu.Lock`, a second concurrent `Close()` can enter and snapshot the same channels. The `closeOnce` on each `ClientChannel` prevents double-closing `Incoming`, and the `c.mu`-guarded `c.conn = nil` prevents double-closing the socket, so the implementation is **correct today** — but subtle.

**Recommendation:** Add an `atomic.Bool` guard at the top of `Close()` for clarity and future-proofing.
**Severity:** Low

### F2 — `closeWithError` sends on response channel while holding `ch.mu`

In `channels.go:68-74`, `closeWithError` holds `ch.mu` while iterating `ch.pending` and sending `response{Err: err}` on each buffered response channel. Since the channels are buffered (capacity 1), this will not block. However, if the buffer were ever changed to 0 or if a different caller is simultaneously trying to read from that channel without holding the lock, this could deadlock.

**Recommendation:** Snapshot pending under the lock, release, then send on the snapshot.
**Severity:** Low (fragile but correct today)

---

## Error Handling

### F3 — `readLoop` returns silently on unknown channel

In `client.go:337-339`, if an envelope arrives for an unknown channel ID, the `readLoop` returns immediately — terminating the entire read loop and shutting down the client. A single malformed or stale envelope from the broker kills the whole connection.

**Recommendation:** Log and skip the envelope instead of returning, or send an error to the client event channel.
**Severity:** Medium

### F4 — Missing `ChannelOpenType` method constant handling in `readLoop`

`ChannelOpenOKType` and `ChannelCloseOKType` are handled as special cases in `readLoop`, but `ChannelOpenType` (if the server ever sends one back) would fall through to the `default` case and look up the channel by `env.ChannelID`. This is a pre-existing protocol design decision.

**Recommendation:** Clarify whether `ChannelOpenType` should ever arrive from the server. If not, add a log for unexpected types.
**Severity:** None (informational)

