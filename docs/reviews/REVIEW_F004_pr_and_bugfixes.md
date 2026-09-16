# Review — F-004 per-consumer delivery channels and bug fixes

Status: **COMPLETE** — reviewed, fixes verified, merged to `main` as `2acc1d6`,
released as **v0.3.1**.

Reviewed: PR #2 (`dkotsi/MultipleConsumersPerChannel`) as it stands at `adf0a26`,
covering the original F-004 feature (merged earlier at `82b95a2`) plus the five
follow-up commits (`9eefe06`, `7b22e3e`, `478528d`, `79dca70`, `adf0a26`).

---

## Scope

| Commit | Contents |
|--------|----------|
| `9eefe06` | fix: Bugs 1 & 2 (consume error on channel close; first-delivery registration race) |
| `7b22e3e` | test: regression tests for both F-004 bugs |
| `478528d` | docs: two-bug review + fix plan |
| `79dca70` | fix: Bug 3 (`OpenChannel` nil-pointer on ctx timeout) |
| `adf0a26` | docs: v0.3.0 changelog (naming note below) |

## Verification performed

- `gofmt -l .` — clean
- `go build ./...` — ok
- `go vet ./...` — ok
- `go test -race -count=3 ./gomqSDK/` — **11/11 pass** across three runs, including:
  - `TestF004_CloseWhileConsumePendingReturnsError` (Bug 1 regression)
  - `TestF004_FirstDeliveryAfterConsumeOKNeverFallsBackToIncoming` (Bug 2, 100 iterations)
  - `TestOpenChannelExpiredContextReturnsDeadlineExceeded` (Bug 3 regression)

## Findings during review

Three bugs were found in the original PR and confirmed by reproduction:

1. **`Consume` panic on nil `Data`** — `channels.go:336` performed a bare
   `res.Data.(protocol.ConsumeOK)`; `handleChannelCloseOK` resolved a pending
   consume with an empty `response{}`, panicking any in-flight `Consume` on
   channel close. **Fixed** in `9eefe06` (guarded type switch + close-ok now
   resolves with a `channel closed` error).
2. **Registration race on first delivery** — the per-consumer channel was
   registered on the caller goroutine *after* the response returned; the
   `readLoop` could route a pre-queued `basic.deliver` before the map insert,
   sending it to `Incoming` instead of the per-consumer channel. Reproduced at
   iteration 2 of a 200-iteration stress. **Fixed** in `9eefe06` (registration
   moved into `route`'s `BasicConsumeOKType` case, under `ch.mu`, before
   resolving — guaranteed happens-before via serial envelope processing).
3. **`OpenChannel` nil-pointer panic on ctx expiry** — `client.go:214` called
   `ch.unRegisterREQ` on the nil named return instead of `clientCh`
   (pre-existing since PR #1). **Fixed** in `79dca70`.

All three fixes are correct and covered by regression tests.

## Transition-state caveat (broker not stamped yet)

`Deliver` gains `consumer_tag` SDK-side, but the **broker has not been updated
to stamp it** (`dispatchLoop` still writes tag-less envelopes). Because the SDK
now always sends a client-authoritative tag and the broker echoes it verbatim,
`Consume` returns a per-consumer channel that currently receives **no**
deliveries — they fall back to `ch.Incoming`.

- Clients that read `ch.Incoming` (v0.2.0 pattern) are unaffected.
- Clients that adopt the new returned-channel API before the broker stamps
  tags will silently see no messages.

This is accepted for now under the documented rollout plan: the feature is
fully active only when both sides are updated (F-004 dependency order — broker
first, then SDK). The broker stamping is the remaining step.

## Decisions / notes

- **`ch.Close()` now returns a "channel closed" error** on a normal close-ok.
  Deliberate (avoids the nil-`Data` resolution for every caller) and reflected
  in `TestClientChannelClose`. Confirm this contract before relying on it.
- **Concurrent `Client.Close()` racing a consume round-trip** could leave a
  just-registered per-consumer channel unclosed (its entry was inserted after
  `closeWithError` snapshotted the map). Low-severity corner; not covered by
  tests; follow-up candidate.
- **Versioning:** the fixes ship as **v0.3.1** because `v0.3.0` was published
  at `82b95a2` *without* them (and tags are immutable once on the module
  proxy). The merged changelog doc is named `F004_SDK_v0.3.0_changelog.md` but
  its content first appears in v0.3.1 — a cosmetic mismatch worth correcting
  in a later docs pass.
- Send-on-closed-channel hazard for per-consumer channels (and `Incoming`)
  remains a pre-existing follow-up (see `future_improvements.md`).

## Result

Main is at `2acc1d6`. v0.3.1 tagged and pushed. Cross-repo follow-up: stamp
`ConsumerTag` in the broker's `dispatchLoop` and re-run broker integration
tests against per-consumer channels.