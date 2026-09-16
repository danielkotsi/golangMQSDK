package gomqSDK

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/danielkotsi/golangMQSDK/protocol"
)

type response struct {
	Data any
	Err  error
}

// consumerEntry ties a per-consumer delivery channel to its close guard so the
// channel can be closed exactly once even when closeWithError runs concurrently
// on multiple goroutines.
type consumerEntry struct {
	deliveries chan protocol.Deliver
	closeOnce  sync.Once
}

type ClientChannel struct {
	id uint16

	mu        sync.Mutex
	pending   map[uint16]chan response
	closeOnce sync.Once

	Incoming    chan protocol.Deliver     // legacy shared sink (fallback)
	consumers   map[string]*consumerEntry // broker-echoed tag -> per-consumer chan
	consumerSeq uint16                    // for client-authoritative tags
	client      *Client
}

func newClientChannel(id uint16, client *Client) *ClientChannel {
	return &ClientChannel{
		id:      id,
		pending: make(map[uint16]chan response),
		client:  client,
		//i will need to reconsider the buffer here
		Incoming:  make(chan protocol.Deliver, 100),
		consumers: make(map[string]*consumerEntry),
	}
}
func (ch *ClientChannel) registerREQ(reqID uint16) chan response {
	respCH := make(chan response, 1)
	ch.mu.Lock()
	ch.pending[reqID] = respCH
	ch.mu.Unlock()

	return respCH
}

func (ch *ClientChannel) unRegisterREQ(reqID uint16) {
	ch.mu.Lock()
	delete(ch.pending, reqID)
	ch.mu.Unlock()
}

func (ch *ClientChannel) resolve(reqID uint16, res response) {
	ch.mu.Lock()
	respCH, ok := ch.pending[reqID]
	if ok {
		delete(ch.pending, reqID)
	}
	ch.mu.Unlock()

	if ok {
		respCH <- res
	}
}

// closeWithError signals every pending request with err and closes the
// channel's Incoming delivery channel and every per-consumer delivery channel.
// It is idempotent and safe to call concurrently. It is invoked by the owning
// client when it is closed.
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

// Close closes the channel on the broker and cleans up local state. The caller
// is unblocked once the broker acknowledges with channel.close-ok or ctx
// expires. After Close the channel must not be used any further.
func (ch *ClientChannel) Close(ctx context.Context) error {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	if err := ch.client.writeChannelEnvelope(ch.id, protocol.ChannelCloseType, reqID, protocol.ChannelClose{
		ID: ch.id,
	}); err != nil {
		ch.unRegisterREQ(reqID)
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

func (ch *ClientChannel) route(env protocol.Envelope) error {
	switch env.Type {
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
				target.deliveries <- delivery
				return nil
			}
			// Unknown tag: fall through to the legacy shared sink.
		}

		ch.Incoming <- delivery
		return nil
	case protocol.BasicConsumeOKType:
		var consumeOK protocol.ConsumeOK
		err := json.Unmarshal(env.Payload, &consumeOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, response{
			Data: consumeOK,
		})
		return nil
	case protocol.QueueDeclareOKType:
		var declareOK protocol.QueueDeclareOK
		err := json.Unmarshal(env.Payload, &declareOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, response{
			Data: declareOK,
		})
		return nil
	case protocol.ExchangeDeclareOKType:
		var exchangeOK protocol.ExchangeDeclareOK
		err := json.Unmarshal(env.Payload, &exchangeOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, response{
			Data: exchangeOK,
		})
		return nil
	case protocol.QueueBindOKType:
		var bindOK protocol.QueueBindOK
		err := json.Unmarshal(env.Payload, &bindOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, response{
			Data: bindOK,
		})
		return nil
	case protocol.ErrorType:
		var brokerError protocol.Error
		err := json.Unmarshal(env.Payload, &brokerError)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, response{
			Err: fmt.Errorf("code:%s Message:%s", brokerError.Code, brokerError.Message),
		})
		return nil
	}
	return fmt.Errorf("Envelope Type does not match protocol")
}

// if no dead-letter-queue/dead-letter-routingkey is wanted put "" on these parameters
// in this case i just cancel with the timeout
// but server still creates the queue if responds delayed
// needs fixing
func (ch *ClientChannel) DeclareQueue(name string, ctx context.Context, dlxExchange, dlxRoutingKey string) (*Queue, error) {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	qd := protocol.QueueDeclare{
		Name:                 name,
		DeadLetterExchange:   dlxExchange,
		DeadLetterRoutingKey: dlxRoutingKey,
	}

	if err := ch.client.writeChannelEnvelope(ch.id, protocol.QueueDeclareType, reqID, qd); err != nil {
		return nil, err
	}

	select {
	case res := <-respCh:
		if res.Err != nil {
			return nil, res.Err
		}
		return &Queue{
			Name: res.Data.(protocol.QueueDeclareOK).Name,
		}, nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return nil, ctx.Err()
	}
}
func (ch *ClientChannel) DeclareExchange(name string, ctx context.Context) (exchange string, err error) {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	if err := ch.client.writeChannelEnvelope(ch.id, protocol.ExchangeDeclareType, reqID, protocol.ExchangeDeclare{
		Name: name,
	}); err != nil {
		return "", err
	}

	select {
	case res := <-respCh:
		if res.Err != nil {
			return "", res.Err
		}
		return res.Data.(protocol.ExchangeDeclareOK).Name, nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return "", ctx.Err()
	}
}
func (ch *ClientChannel) BindQueue(queue, exchange, routingKey string, ctx context.Context) error {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	if err := ch.client.writeChannelEnvelope(ch.id, protocol.QueueBindType, reqID, protocol.QueueBind{
		Queue:      queue,
		Exchange:   exchange,
		RoutingKey: routingKey,
	}); err != nil {
		return err
	}

	select {
	case res := <-respCh:
		if res.Err != nil {
			return res.Err
		}
		return nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return ctx.Err()
	}
}

func (ch *ClientChannel) Publish(event protocol.Publish) error {
	reqID := ch.client.nextRequestID()

	if err := ch.client.writeChannelEnvelope(ch.id, protocol.BasicPublishType, reqID, event); err != nil {
		return err
	}
	return nil
}

func (ch *ClientChannel) Ack(deliveryTag uint16) error {
	return ch.client.writeChannelEnvelope(ch.id, protocol.BasicAckType, 0, protocol.Ack{
		DeliveryTag: deliveryTag,
	})
}

func (ch *ClientChannel) Nack(deliveryTag uint16, requeue bool) error {
	r := requeue
	return ch.client.writeChannelEnvelope(ch.id, protocol.BasicNackType, 0, protocol.Nack{
		DeliveryTag: deliveryTag,
		Requeue:     &r,
	})
}

// nextConsumerTag mints a deterministic, channel-unique consumer tag. The
// broker rejects duplicate tags on a channel, so a fresh tag per Consume call
// guarantees uniqueness without relying on broker-side generation.
func (ch *ClientChannel) nextConsumerTag() string {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.consumerSeq++
	return fmt.Sprintf("client-%d-consumer-%d", ch.id, ch.consumerSeq)
}

// Consume starts consuming queuename and returns the delivery channel for the
// consumer. When the broker echoes a non-empty consumer tag, a dedicated
// per-consumer channel is allocated, registered under the echoed tag, and
// returned, so every basic.deliver carrying that tag lands here. When the
// broker echoes an empty tag (pre-tag or legacy broker), the shared ch.Incoming
// is returned, exactly as before, preserving backwards compatibility.
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
		// like v0.2.0. This fallback is what keeps mixed-version combinations
		// (guide §6) working.
		if consumeOK.ConsumerTag == "" {
			return ch.Incoming, nil
		}

		// Key on the broker-echoed tag (not the client tag) so the routing in
		// route() stays correct even if a broker ever normalises tags.
		perConsumer := &consumerEntry{
			deliveries: make(chan protocol.Deliver, 100),
		}
		ch.mu.Lock()
		ch.consumers[consumeOK.ConsumerTag] = perConsumer
		ch.mu.Unlock()
		return perConsumer.deliveries, nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return nil, ctx.Err()
	}
}
