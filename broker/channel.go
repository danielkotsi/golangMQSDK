package broker

import (
	"GolangRabbitMQBroker/protocol"
	"encoding/json"
	"log"
)

type Channel struct {
	id        uint16
	conn      *Connection
	broker    *Broker
	consumers map[string]*Consumer
}

func (ch *Channel) cleanup() {
	for _, consumer := range ch.consumers {
		consumer.queue.unregisterConsumer(consumer.tag)
	}
}

func (ch *Channel) route(env protocol.Envelope) {
	switch env.Type {
	case protocol.BasicPublishType:
		ch.HandlePublish(env)
	case protocol.BasicConsumeType:
		ch.HandleConsume(env)
	case protocol.BasicAckType:
		ch.HandleAck(env)
	case protocol.QueueDeclareType:
		ch.HandleQueueDeclare(env)
	case protocol.ExchangeDeclareType:
		ch.HandleExchangeDeclare(env)
	case protocol.QueueBindType:
		ch.HandleQueueBind(env)
	}
}

func (ch *Channel) HandlePublish(env protocol.Envelope) {
	var event protocol.Publish
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		log.Println(err)
		return
	}

	err = ch.broker.Publish(event.Exchange, event.RoutingKey, event.Body)
	if err != nil {
		ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
			Message: err.Error(),
		})
		return
	}
}

func (ch *Channel) HandleConsume(env protocol.Envelope) {
	var event protocol.Consume
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
			Message: err.Error(),
		})
		return
	}

	ch.broker.RegisterConsumer(event.ConsumerTag, event.Queue, ch)

	ch.conn.WriteEnvelope(env.ChannelID, protocol.BasicConsumeOKType, env.RequestID, protocol.ConsumeOK{
		ConsumerTag: event.ConsumerTag,
	})
}

func (ch *Channel) HandleAck(env protocol.Envelope) {
	var event protocol.Ack
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
			Message: err.Error(),
		})
		return
	}

	for _, consumer := range ch.consumers {
		if _, ok := consumer.inflightTags[event.DeliveryTag]; ok {
			ch.broker.Ack(consumer.queue.name, event.DeliveryTag)
			return
		}
	}
}

func (ch *Channel) HandleExchangeDeclare(env protocol.Envelope) {
	var event protocol.ExchangeDeclare
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		log.Println(err)
	}

	ch.broker.DeclareExchange(event.Name)

	ch.conn.WriteEnvelope(env.ChannelID, protocol.ExchangeDeclareOKType, env.RequestID, protocol.ExchangeDeclareOK{
		Name: event.Name,
	})
}

func (ch *Channel) HandleQueueDeclare(env protocol.Envelope) {
	var event protocol.QueueDeclare
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		log.Println(err)
	}
	ch.broker.DeclareQueue(event.Name)

	ch.conn.WriteEnvelope(env.ChannelID, protocol.QueueDeclareOKType, env.RequestID, protocol.QueueDeclareOK{
		Name: event.Name,
	})
}

func (ch *Channel) HandleQueueBind(env protocol.Envelope) {
	var event protocol.QueueBind
	err := json.Unmarshal(env.Payload, &event)
	if err != nil {
		ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
			Message: err.Error(),
		})
		return
	}
	ch.broker.BindQueue(event.Exchange, event.Queue, event.RoutingKey)

	ch.conn.WriteEnvelope(env.ChannelID, protocol.QueueBindOKType, env.RequestID, protocol.QueueBindOK{})
}
