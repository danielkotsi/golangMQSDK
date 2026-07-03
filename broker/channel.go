package broker

import (
	"GolangRabbitMQBroker/protocol"
	"encoding/json"
	"fmt"
	"log"
)

type Channel struct {
	id        uint16
	conn      *Connection
	server    *Server
	consumers map[string]*Consumer
}

func (ch *Channel) route(env protocol.Envelope) {
	switch env.Type {
	case protocol.BasicPublishType:
		var event protocol.Publish
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			log.Println(err)
		}
		ch.HandlePublish(env.ChannelID, env.RequestID, ch.conn, &event)
	case protocol.BasicConsumeType:
		var event protocol.Consume
		fmt.Println("we are here")
		fmt.Println("this is the channelID:", env.ChannelID)
		fmt.Println("this is the reqID:", env.RequestID)
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleConsume(env.ChannelID, env.RequestID, ch.conn, &event)
	case protocol.BasicAckType:
		var event protocol.Ack
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleAck(ch.conn, &event)
	case protocol.QueueDeclareType:
		fmt.Println("this is the channelID:", env.ChannelID)
		var event protocol.QueueDeclare
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			log.Println(err)
		}
		ch.HandleQueueDeclare(ch.conn, &event)
	case protocol.QueueBindType:
		var event protocol.QueueBind
		err := json.Unmarshal(env.Payload, &event)
		if err != nil {
			ch.conn.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
				Message: err.Error(),
			})
		}
		ch.HandleQueueBind(ch.conn, &event)
	}
}

func (ch *Channel) HandlePublish(channelID uint16, reqID uint16, conn *Connection, event *protocol.Publish) {
	conn.WriteEnvelope(channelID, protocol.BasicDeliverType, reqID, protocol.Deliver{
		Queue: event.Queue,
		Body:  event.Body,
	})
}

func (ch *Channel) HandleConsume(channelID uint16, reqID uint16, conn *Connection, event *protocol.Consume) {
	fmt.Println("hello we got into the handle consume")
	conn.WriteEnvelope(channelID, protocol.BasicConsumeOKType, reqID, protocol.ConsumeOK{
		ConsumerTag: "daniel",
	})
}

func (ch *Channel) HandleAck(conn *Connection, event *protocol.Ack) {
}

func (ch *Channel) HandleQueueDeclare(conn *Connection, event *protocol.QueueDeclare) {
	log.Println("this is the queue declare request")
}

func (ch *Channel) HandleQueueBind(conn *Connection, event *protocol.QueueBind) {
}
