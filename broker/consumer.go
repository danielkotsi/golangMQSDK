package broker

import (
	"GolangRabbitMQBroker/protocol"
	"fmt"
)

type Consumer struct {
	tag   string
	queue *Queue

	channel *Channel
	ch      chan protocol.Deliver

	prefetch        int
	inflight        int
	inflightTags    map[uint16]struct{}
	pendingMessages map[uint16]Message
}

func NewConsumer(tag string, queue *Queue, ch *Channel) *Consumer {
	if tag == "" {
		tag = fmt.Sprintf("ch.%d.consumer.%d", ch.id, len(ch.consumers)+1)
	}
	return &Consumer{
		tag:             tag,
		queue:           queue,
		channel:         ch,
		ch:              make(chan protocol.Deliver, 100),
		prefetch:        10,
		inflightTags:    make(map[uint16]struct{}),
		pendingMessages: make(map[uint16]Message),
	}
}
