package protocol

import "encoding/json"

type Method string

const (

	//basic
	//client
	BasicPublishType Method = "basic.publish"
	BasicConsumeType Method = "basic.consume"
	BasicAckType     Method = "basic.ack"
	BasicNackType    Method = "basic.nack"

	//server
	BasicDeliverType   Method = "basic.deliver"
	BasicConsumeOKType Method = "basic.consume-ok"

	//AMQP channel
	//client
	ChannelOpenType  Method = "channel.open"
	ChannelCloseType Method = "channel.close"
	//server
	ChannelOpenOKType  Method = "channel.open-ok"
	ChannelCloseOKType Method = "channel.close-ok"

	//Queue
	//client
	QueueDeclareType    Method = "queue.declare"
	ExchangeDeclareType Method = "exchange.declare"
	QueueBindType       Method = "queue.bind"
	//server
	QueueDeclareOKType    Method = "queue.declare-ok"
	ExchangeDeclareOKType Method = "exchange.declare-ok"
	QueueBindOKType       Method = "queue.bind-ok"

	//error
	ErrorType Method = "error"
)

// each of the events will be passed from inside an envelope and based on the type the payload will be different
type Envelope struct {
	ChannelID uint16
	RequestID uint16
	Type      Method
	Payload   json.RawMessage
}

// for example this is the payload of an envelope with type "channel.open"
type ChannelOpen struct {
	ID uint16 `json:"id"`
}

type ChannelClose struct {
	ID uint16 `json:"id"`
}

// and this is going to be "channel.open-ok"
type ChannelOpenOK struct {
	ID uint16 `json:"id"`
}

type Publish struct {
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
	Body       []byte `json:"body"`
}

type Consume struct {
	Queue       string `json:"queue"`
	ConsumerTag string `json:"consumer_tag"`
}

type ConsumeOK struct {
	ConsumerTag string `json:"consumer_tag"`
}

type Ack struct {
	DeliveryTag uint16 `json:"delivery_tag"`
}

type Nack struct {
	DeliveryTag uint16 `json:"delivery_tag"`
	Requeue     *bool  `json:"requeue,omitempty"`
}

type Deliver struct {
	DeliveryTag uint16 `json:"delivery_tag"`
	Body        []byte `json:"body"`
	Exchange    string `json:"exchange"`
	RoutingKey  string `json:"routing_key"`
}

type QueueDeclare struct {
	Name                string `json:"name"`
	DeadLetterExchange  string `json:"dead_letter_exchange,omitempty"`
	DeadLetterRoutingKey string `json:"dead_letter_routing_key,omitempty"`
}

type QueueDeclareOK struct {
	Name string `json:"name"`
}

type ExchangeDeclare struct {
	Name string `json:"name"`
}

type ExchangeDeclareOK struct {
	Name string `json:"name"`
}

type QueueBindOK struct {
}

type QueueBind struct {
	Queue      string `json:"queue"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
