package gomqSDK

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

// f004Broker wraps fakeBroker with a wire loop that forwards every inbound
// envelope to the test goroutine, so the test controls responses and never
// calls t.Fatal from a background goroutine.
type f004Broker struct {
	*fakeBroker
	reqs chan protocol.Envelope
}

func newF004Broker(t *testing.T) *f004Broker {
	return &f004Broker{
		fakeBroker: newFakeBroker(t),
		reqs:       make(chan protocol.Envelope, 16),
	}
}

func (b *f004Broker) serve() {
	go func() {
		for {
			line, err := b.r.ReadBytes('\n')
			if err != nil {
				return
			}
			var env protocol.Envelope
			if err := json.Unmarshal(line, &env); err != nil {
				return
			}
			b.reqs <- env
		}
	}()
}

// openChannel drives the channel.open <-> channel.open-ok round trip.
func (b *f004Broker) openChannel(t *testing.T, c *Client, ctx context.Context, name string) *ClientChannel {
	t.Helper()

	type result struct {
		ch  *ClientChannel
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		ch, err := c.OpenChannel(ctx)
		resCh <- result{ch: ch, err: err}
	}()

	env := b.nextEnvelope(t, protocol.ChannelOpenType, name)

	// The client's channel id travels in the channel.open payload; the
	// open-ok must echo it so handleChannelOpenOK can find the channel.
	var open protocol.ChannelOpen
	if err := json.Unmarshal(env.Payload, &open); err != nil {
		t.Fatalf("%s: unmarshal channel.open: %v", name, err)
	}
	b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.ChannelOpenOKType, protocol.ChannelOpenOK{ID: open.ID}))

	res := <-resCh
	if res.err != nil {
		t.Fatalf("%s: open channel: %v", name, res.err)
	}
	return res.ch
}

// nextEnvelope asserts the next request received is of typ.
func (b *f004Broker) nextEnvelope(t *testing.T, typ protocol.Method, what string) protocol.Envelope {
	t.Helper()
	select {
	case env := <-b.reqs:
		if env.Type != typ {
			t.Fatalf("%s: expected %s, got %s (payload %s)", what, typ, env.Type, env.Payload)
		}
		return env
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: timed out waiting for %s", what, typ)
		return protocol.Envelope{}
	}
}

// consumeOn drives Consume in a goroutine and echoes the requested consumer tag
// back via basic.consume-ok, returning the tag the SDK keyed on and the
// resulting delivery channel.
func (b *f004Broker) consumeOn(t *testing.T, ch *ClientChannel, queue string, ctx context.Context) (chan protocol.Deliver, string) {
	t.Helper()

	type result struct {
		c   chan protocol.Deliver
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		c, err := ch.Consume(queue, ctx)
		resCh <- result{c: c, err: err}
	}()

	env := b.nextEnvelope(t, protocol.BasicConsumeType, "consume "+queue)

	var req protocol.Consume
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		t.Fatalf("consume %s: unmarshal request: %v", queue, err)
	}
	if req.ConsumerTag == "" {
		t.Fatalf("consume %s: expected a client-authoritative consumer tag, got empty", queue)
	}

	b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.BasicConsumeOKType, protocol.ConsumeOK{
		ConsumerTag: req.ConsumerTag,
	}))

	res := <-resCh
	if res.err != nil {
		t.Fatalf("consume %s: %v", queue, res.err)
	}
	return res.c, req.ConsumerTag
}

// deliver pushes a basic.deliver envelope onto the wire for ch.
func (b *f004Broker) deliver(t *testing.T, ch *ClientChannel, d protocol.Deliver) {
	t.Helper()
	b.send(prepareEnvelope(t, ch.id, 0, protocol.BasicDeliverType, d))
}

func recvDeliver(t *testing.T, c <-chan protocol.Deliver, what string) (protocol.Deliver, bool) {
	t.Helper()
	select {
	case d, ok := <-c:
		return d, ok
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: timed out waiting for delivery", what)
		return protocol.Deliver{}, false
	}
}

// TestF004_ConsumeReturnsDistinctChannels asserts two Consume calls on one
// channel yield distinct per-consumer channels and that deliveries carrying
// each consumer's tag land only on that consumer's channel.
func TestF004_ConsumeReturnsDistinctChannels(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "f004")

	chanA, tagA := b.consumeOn(t, ch, "qa", ctx)
	chanB, tagB := b.consumeOn(t, ch, "qb", ctx)

	if chanA == chanB {
		t.Fatal("F-004: two Consume calls returned the same channel")
	}
	if chanA == ch.Incoming || chanB == ch.Incoming {
		t.Fatal("F-004: per-consumer channel must not be the shared Incoming")
	}

	b.deliver(t, ch, protocol.Deliver{
		DeliveryTag: 1, ConsumerTag: tagA, Body: []byte("msg-A"), Exchange: "ex-a", RoutingKey: "k.a",
	})
	b.deliver(t, ch, protocol.Deliver{
		DeliveryTag: 1, ConsumerTag: tagB, Body: []byte("msg-B"), Exchange: "ex-b", RoutingKey: "k.b",
	})

	gotA, ok := recvDeliver(t, chanA, "consumer A")
	if !ok {
		t.Fatal("consumer A channel closed unexpectedly")
	}
	gotB, ok := recvDeliver(t, chanB, "consumer B")
	if !ok {
		t.Fatal("consumer B channel closed unexpectedly")
	}

	if string(gotA.Body) != "msg-A" || gotA.ConsumerTag != tagA {
		t.Errorf("consumer A got body=%q tag=%q, want body=msg-A tag=%q", gotA.Body, gotA.ConsumerTag, tagA)
	}
	if string(gotB.Body) != "msg-B" || gotB.ConsumerTag != tagB {
		t.Errorf("consumer B got body=%q tag=%q, want body=msg-B tag=%q", gotB.Body, gotB.ConsumerTag, tagB)
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.consumers) != 2 {
		t.Errorf("expected 2 registered consumers, got %d", len(ch.consumers))
	}
}

// TestF004_LegacyBrokerFallsBackToIncoming asserts that when the broker echoes
// an empty consumer tag, Consume returns the shared Incoming and untagged
// deliveries arrive there unchanged (v0.2.0 behaviour).
func TestF004_LegacyBrokerFallsBackToIncoming(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "legacy")

	type result struct {
		c   chan protocol.Deliver
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		c, err := ch.Consume("qa", ctx)
		resCh <- result{c: c, err: err}
	}()

	env := b.nextEnvelope(t, protocol.BasicConsumeType, "consume legacy")
	b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.BasicConsumeOKType, protocol.ConsumeOK{
		ConsumerTag: "",
	}))

	res := <-resCh
	if res.err != nil {
		t.Fatalf("consume: %v", res.err)
	}
	if res.c != ch.Incoming {
		t.Fatalf("legacy broker: expected Consume to return ch.Incoming, got a different channel")
	}

	b.deliver(t, ch, protocol.Deliver{
		DeliveryTag: 7, Body: []byte("legacy-msg"), Exchange: "ex", RoutingKey: "k",
	})

	got, ok := recvDeliver(t, ch.Incoming, "legacy delivery")
	if !ok {
		t.Fatal("Incoming closed unexpectedly")
	}
	if string(got.Body) != "legacy-msg" {
		t.Errorf("got body=%q, want legacy-msg", got.Body)
	}

	ch.mu.Lock()
	n := len(ch.consumers)
	ch.mu.Unlock()
	if n != 0 {
		t.Errorf("legacy broker must not register per-consumer channels, got %d", n)
	}
}

// TestF004_UnknownConsumerTagFallsBackToIncoming asserts a delivery whose tag
// is not registered falls through to Incoming instead of being dropped.
func TestF004_UnknownConsumerTagFallsBackToIncoming(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "unknown-tag")
	perConsumer, _ := b.consumeOn(t, ch, "qa", ctx)

	b.deliver(t, ch, protocol.Deliver{
		DeliveryTag: 3, ConsumerTag: "ghost-consumer", Body: []byte("orphan"), Exchange: "ex", RoutingKey: "k",
	})

	got, ok := recvDeliver(t, ch.Incoming, "fallback delivery")
	if !ok {
		t.Fatal("Incoming closed unexpectedly")
	}
	if string(got.Body) != "orphan" {
		t.Errorf("got body=%q, want orphan", got.Body)
	}

	select {
	case d := <-perConsumer:
		t.Errorf("orphan tag leaked to per-consumer channel: %+v", d)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestF004_DeliveryBeforeConsumeOKRoutesToPerConsumerChannel covers the F-004.1
// pre-queued-delivery ordering: the broker's dispatchLoop can write a
// basic.deliver before the connection goroutine writes basic.consume-ok. The
// per-consumer entry is pre-registered by Consume, so such an early delivery
// must route to the per-consumer channel, never fall through to Incoming.
func TestF004_DeliveryBeforeConsumeOKRoutesToPerConsumerChannel(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "pre-queued")

	type result struct {
		c   chan protocol.Deliver
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		got, err := ch.Consume("preq", ctx)
		resCh <- result{c: got, err: err}
	}()

	env := b.nextEnvelope(t, protocol.BasicConsumeType, "consume preq")
	var req protocol.Consume
	if err := json.Unmarshal(env.Payload, &req); err != nil {
		t.Fatalf("unmarshal consume request: %v", err)
	}
	if req.ConsumerTag == "" {
		t.Fatal("expected a client-authoritative consumer tag, got empty")
	}

	// Deliver BEFORE consume-ok: the exact F-004.1 ordering.
	b.send(prepareEnvelope(t, env.ChannelID, 0, protocol.BasicDeliverType, protocol.Deliver{
		DeliveryTag: 1,
		ConsumerTag: req.ConsumerTag,
		Body:        []byte("pre-queued"),
	}))
	b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.BasicConsumeOKType,
		protocol.ConsumeOK{ConsumerTag: req.ConsumerTag}))

	res := <-resCh
	if res.err != nil {
		t.Fatalf("consume: %v", res.err)
	}
	if res.c == ch.Incoming {
		t.Fatal("Consume returned Incoming, expected per-consumer channel")
	}

	got, ok := recvDeliver(t, res.c, "pre-queued delivery")
	if !ok {
		t.Fatal("per-consumer channel closed unexpectedly")
	}
	if string(got.Body) != "pre-queued" {
		t.Errorf("got body=%q, want pre-queued", got.Body)
	}

	select {
	case d := <-ch.Incoming:
		t.Fatalf("pre-queued delivery leaked to Incoming: %q", d.Body)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestF004_DuplicateTagSurfacedAsError asserts that when the broker rejects a
// basic.consume (e.g. duplicate tag), Consume returns the broker error and
// nothing is left registered.
func TestF004_DuplicateTagSurfacedAsError(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "dup")

	type result struct {
		c   chan protocol.Deliver
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		c, err := ch.Consume("qa", ctx)
		resCh <- result{c: c, err: err}
	}()

	env := b.nextEnvelope(t, protocol.BasicConsumeType, "consume dup")
	b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.ErrorType, protocol.Error{
		Code:    "connection_error",
		Message: "consumer tag already in use on channel",
	}))

	res := <-resCh
	if res.err == nil {
		t.Fatal("expected Consume to surface the broker's duplicate-tag error")
	}
	if res.c != nil {
		t.Fatal("expected no channel from a rejected Consume")
	}

	ch.mu.Lock()
	n := len(ch.consumers)
	ch.mu.Unlock()
	if n != 0 {
		t.Errorf("rejected Consume left %d consumer entries behind", n)
	}
}

// TestF004_CloseClosesPerConsumerChannels asserts Client.Close closes every
// per-consumer channel (and Incoming) so range loops terminate.
func TestF004_CloseClosesPerConsumerChannels(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "close")
	chanA, _ := b.consumeOn(t, ch, "qa", ctx)
	chanB, _ := b.consumeOn(t, ch, "qb", ctx)

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	for _, cc := range []chan protocol.Deliver{chanA, chanB, ch.Incoming} {
		if _, ok := <-cc; ok {
			t.Error("channel still open after Close")
		}
	}
}

// TestF004_CloseWhileConsumePendingReturnsError verifies that when the broker
// closes the channel while a Consume is waiting for basic.consume-ok, the
// caller sees an error instead of a panic (F-004 review, Bug 1).
func TestF004_CloseWhileConsumePendingReturnsError(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "close-pending")

	type result struct {
		c   chan protocol.Deliver
		err error
	}
	resCh := make(chan result, 1)
	go func() {
		got, err := ch.Consume("qa", ctx)
		resCh <- result{c: got, err: err}
	}()

	env := b.nextEnvelope(t, protocol.BasicConsumeType, "consume canceled by close")

	// Respond with channel.close-ok echoing the consume's RequestID — the
	// real broker can do this during an unsolicited channel close.
	b.send(prepareEnvelope(t, ch.id, env.RequestID, protocol.ChannelCloseOKType,
		protocol.ChannelCloseOK{ID: ch.id}))

	res := <-resCh
	if res.err == nil {
		t.Fatal("expected Consume to return an error when the channel closes before consume-ok")
	}
	if res.c != nil {
		t.Fatal("expected no channel from a Consume canceled by channel close")
	}
}

// TestF004_FirstDeliveryAfterConsumeOKNeverFallsBackToIncoming stress-tests the
// ordering where basic.deliver immediately follows basic.consume-ok. Registration
// is pre-created by Consume before the request is sent (F-004.1), so the first
// delivery must always route to the per-consumer channel, never to Incoming.
func TestF004_FirstDeliveryAfterConsumeOKNeverFallsBackToIncoming(t *testing.T) {
	b := newF004Broker(t)
	defer b.close()

	c := connectTestClient(t, b.fakeBroker)
	b.serve()
	defer c.Close()

	ctx := context.Background()
	ch := b.openChannel(t, c, ctx, "first-delivery")

	const iterations = 100
	for i := 0; i < iterations; i++ {
		queue := fmt.Sprintf("q-%d", i)

		type result struct {
			c   chan protocol.Deliver
			err error
		}
		resCh := make(chan result, 1)
		go func() {
			got, err := ch.Consume(queue, ctx)
			resCh <- result{c: got, err: err}
		}()

		env := b.nextEnvelope(t, protocol.BasicConsumeType, fmt.Sprintf("consume %d", i))

		// The broker echoes the tag the client requested. Consume pre-registers
		// its own client-authoritative tag, so the echoed tag must be that
		// requested tag (F-004.1), not an arbitrary one.
		var req protocol.Consume
		if err := json.Unmarshal(env.Payload, &req); err != nil {
			t.Fatalf("iteration %d: unmarshal consume request: %v", i, err)
		}
		if req.ConsumerTag == "" {
			t.Fatalf("iteration %d: expected a client-authoritative consumer tag, got empty", i)
		}
		tag := req.ConsumerTag

		// Respond with the echoed consumer tag, then immediately queue the first
		// delivery with that same tag. The entry is pre-registered by Consume
		// before the request is sent, so the first delivery must always land on
		// the per-consumer channel, never on Incoming.
		b.send(prepareEnvelope(t, env.ChannelID, env.RequestID, protocol.BasicConsumeOKType,
			protocol.ConsumeOK{ConsumerTag: tag}))
		b.send(prepareEnvelope(t, env.ChannelID, 0, protocol.BasicDeliverType,
			protocol.Deliver{
				DeliveryTag: uint16(i + 1),
				ConsumerTag: tag,
				Body:        []byte("first"),
			}))

		res := <-resCh
		if res.err != nil {
			t.Fatalf("iteration %d: consume: %v", i, res.err)
		}
		if res.c == ch.Incoming {
			t.Fatalf("iteration %d: Consume returned Incoming, expected per-consumer channel", i)
		}

		// The delivery must arrive on the per-consumer channel.
		select {
		case d, ok := <-res.c:
			if !ok {
				t.Fatalf("iteration %d: per-consumer channel closed", i)
			}
			if string(d.Body) != "first" {
				t.Fatalf("iteration %d: got body=%q, want %q", i, d.Body, "first")
			}
			if d.ConsumerTag != tag {
				t.Fatalf("iteration %d: got tag=%q, want %q", i, d.ConsumerTag, tag)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: delivery never arrived on per-consumer channel", i)
		}

		// Incoming must not have received the delivery.
		select {
		case d := <-ch.Incoming:
			t.Fatalf("iteration %d: delivery leaked to Incoming: %q", i, d.Body)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
