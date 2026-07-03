package client

import (
	"GolangRabbitMQBroker/protocol"
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type Response struct {
	Data any
	Err  error
}

type ClientChannel struct {
	id uint16

	mu      sync.Mutex
	pending map[uint16]chan Response

	Incoming chan protocol.Deliver
	client   *Client
}

func NewClientChannel(id uint16, client *Client) *ClientChannel {
	return &ClientChannel{
		id:      id,
		pending: make(map[uint16]chan Response),
		client:  client,
		//i will need to reconsider the buffer here
		Incoming: make(chan protocol.Deliver, 100),
	}
}
func (ch *ClientChannel) registerREQ(reqID uint16) chan Response {
	respCH := make(chan Response, 1)
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

func (ch *ClientChannel) resolve(reqID uint16, res Response) {
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

func (ch *ClientChannel) route(env protocol.Envelope) error {
	switch env.Type {
	case protocol.BasicDeliverType:
		var delivery protocol.Deliver
		err := json.Unmarshal(env.Payload, &delivery)
		if err != nil {
			return err
		}
		ch.Incoming <- delivery
		return nil
	case protocol.BasicConsumeOKType:
		var consumeOK protocol.ConsumeOK
		err := json.Unmarshal(env.Payload, &consumeOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, Response{
			Data: consumeOK,
		})
		return nil
	case protocol.QueueDeclareOKType:
		var declareOK protocol.QueueDeclareOK
		err := json.Unmarshal(env.Payload, &declareOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, Response{
			Data: declareOK,
		})
		return nil
	case protocol.ExchangeDeclareOKType:
		var exchangeOK protocol.ExchangeDeclareOK
		err := json.Unmarshal(env.Payload, &exchangeOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, Response{
			Data: exchangeOK,
		})
		return nil
	case protocol.QueueBindOKType:
		var bindOK protocol.QueueBindOK
		err := json.Unmarshal(env.Payload, &bindOK)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, Response{
			Data: bindOK,
		})
		return nil
	case protocol.ErrorType:
		var brokerError protocol.Error
		err := json.Unmarshal(env.Payload, &brokerError)
		if err != nil {
			return err
		}
		ch.resolve(env.RequestID, Response{
			Err: fmt.Errorf("code:%s Message:%s", brokerError.Code, brokerError.Message),
		})
		return nil
	}
	return fmt.Errorf("Envelope Type does not match protocol")
}

// in this case i just cancel with the timeout
// but server still creates the queue if responds delayed
// needs fixing
func (ch *ClientChannel) DeclareQueue(name string, ctx context.Context) (*Queue, error) {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	if err := ch.client.WriteChannelEnvelope(ch.id, protocol.QueueDeclareType, reqID, protocol.QueueDeclare{
		Name: name,
	}); err != nil {
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

	if err := ch.client.WriteChannelEnvelope(ch.id, protocol.ExchangeDeclareType, reqID, protocol.ExchangeDeclare{
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

	if err := ch.client.WriteChannelEnvelope(ch.id, protocol.QueueBindType, reqID, protocol.QueueBind{
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

	if err := ch.client.WriteChannelEnvelope(ch.id, protocol.BasicPublishType, reqID, event); err != nil {
		return err
	}
	return nil
}

func (ch *ClientChannel) Ack(deliveryTag uint16) error {
	return ch.client.WriteChannelEnvelope(ch.id, protocol.BasicAckType, 0, protocol.Ack{
		DeliveryTag: deliveryTag,
	})
}
func (ch *ClientChannel) Consume(queuename string, ctx context.Context) (chan protocol.Deliver, error) {
	reqID := ch.client.nextRequestID()
	respCh := ch.registerREQ(reqID)

	if err := ch.client.WriteChannelEnvelope(ch.id, protocol.BasicConsumeType, reqID, protocol.Consume{
		Queue: queuename,
	}); err != nil {
		return nil, err
	}

	select {
	case res := <-respCh:
		if res.Err != nil {
			return nil, res.Err
		}
		return ch.Incoming, nil
	case <-ctx.Done():
		ch.unRegisterREQ(reqID)
		return nil, ctx.Err()
	}
}
