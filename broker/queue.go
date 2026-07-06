package broker

import (
	"GolangRabbitMQBroker/protocol"
	"fmt"
	"log"
	"sync"
)

type Queue struct {
	name string
	mu   sync.Mutex

	messages  []Message
	consumers map[string]*Consumer

	cond    *sync.Cond
	nextTag uint16
}

type Message struct {
	DeliveryTag uint16
	Body        []byte
	Exchange    string
	RoutingKey  string
}

func NewQueue(name string) *Queue {
	q := &Queue{
		name:      name,
		consumers: make(map[string]*Consumer),
		nextTag:   1,
	}
	q.cond = sync.NewCond(&q.mu)
	go q.dispatchLoop()
	return q
}

func (q *Queue) registerConsumer(c *Consumer) {
	q.mu.Lock()
	q.consumers[c.tag] = c
	q.mu.Unlock()
	q.cond.Signal()
}

func (q *Queue) unregisterConsumer(tag string) {
	q.mu.Lock()
	c, ok := q.consumers[tag]
	if !ok {
		q.mu.Unlock()
		return
	}
	delete(q.consumers, tag)

	for _, msg := range c.pendingMessages {
		q.messages = append([]Message{msg}, q.messages...)
		delete(c.inflightTags, msg.DeliveryTag)
		c.inflight--
	}
	c.pendingMessages = make(map[uint16]Message)

	q.mu.Unlock()
	q.cond.Signal()
}

func (q *Queue) selectConsumer() *Consumer {
	for _, c := range q.consumers {
		fmt.Println("this is the consumer id:", c.channel.id)
		fmt.Println("this is the consumer tag:", c.tag)
		fmt.Println("inflights:", c.inflight)
		fmt.Println("prefetch:", c.prefetch)

		if c.inflight < c.prefetch {
			fmt.Println("this is the chosen consumer id:", c.channel.id)
			fmt.Println("this is the chosen consumer tag:", c.tag)
			fmt.Println("inflights:", c.inflight)
			fmt.Println("prefetch:", c.prefetch)
			return c
		}
	}
	return nil
}

func (q *Queue) dispatchLoop() {
	for {
		q.mu.Lock()

		for len(q.messages) == 0 {
			q.cond.Wait()
		}

		consumer := q.selectConsumer()
		if consumer == nil {
			q.cond.Wait()
			q.mu.Unlock()
			continue
		}
		fmt.Println("this is the consumers channelID:", consumer.channel.id)

		msg := q.messages[0]
		q.messages = q.messages[1:]
		consumer.inflight++
		consumer.inflightTags[msg.DeliveryTag] = struct{}{}
		consumer.pendingMessages[msg.DeliveryTag] = msg

		q.mu.Unlock()

		err := consumer.channel.conn.WriteEnvelope(
			consumer.channel.id,
			protocol.BasicDeliverType,
			0,
			protocol.Deliver{
				DeliveryTag: msg.DeliveryTag,
				Body:        msg.Body,
				Exchange:    msg.Exchange,
				RoutingKey:  msg.RoutingKey,
			},
		)
		if err != nil {
			log.Println("deliver error, re-enqueueing:", err)
			q.mu.Lock()
			q.messages = append([]Message{msg}, q.messages...)
			consumer.inflight--
			delete(consumer.inflightTags, msg.DeliveryTag)
			delete(consumer.pendingMessages, msg.DeliveryTag)
			q.mu.Unlock()
		}
	}
}

func (q *Queue) enqueue(msg Message) {
	q.mu.Lock()
	q.messages = append(q.messages, msg)
	q.mu.Unlock()

	q.cond.Signal()
}
