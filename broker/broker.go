package broker

import (
	"fmt"
	"sync"
)

type Broker struct {
	mu        sync.Mutex
	queues    map[string]*Queue
	exchanges map[string]*Exchange
}

func NewBroker() *Broker {
	return &Broker{
		queues:    make(map[string]*Queue),
		exchanges: make(map[string]*Exchange),
	}
}

func (b *Broker) DeclareQueue(name string) {
	b.mu.Lock()
	if _, ok := b.queues[name]; !ok {
		b.queues[name] = NewQueue(name)
	}
	b.mu.Unlock()
}

func (b *Broker) DeclareExchange(name string) {
	b.mu.Lock()
	if _, ok := b.exchanges[name]; !ok {
		b.exchanges[name] = NewExchange(name)
	}
	b.mu.Unlock()
}

func (b *Broker) BindQueue(exchangeName, queueName, routingKey string) error {
	b.mu.Lock()
	ex, ok1 := b.exchanges[exchangeName]
	q, ok2 := b.queues[queueName]
	b.mu.Unlock()

	if !ok1 {
		return fmt.Errorf("exchange not found")
	}
	if !ok2 {
		return fmt.Errorf("queue not found")
	}

	ex.bind(q, routingKey)
	return nil
}

func (b *Broker) Publish(exchangeName, routingKey string, body []byte) error {
	b.mu.Lock()
	ex, ok := b.exchanges[exchangeName]
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}

	queues := ex.getQueues(routingKey)
	for _, q := range queues {
		q.mu.Lock()
		tag := q.nextTag
		q.nextTag++
		q.mu.Unlock()

		q.enqueue(Message{
			DeliveryTag: tag,
			Body:        body,
			Exchange:    exchangeName,
			RoutingKey:  routingKey,
		})
	}
	return nil
}

func (b *Broker) RegisterConsumer(consumetTag, queue string, ch *Channel) error {
	b.mu.Lock()
	q, ok := b.queues[queue]
	b.mu.Unlock()
	if !ok {
		return fmt.Errorf("queue not found")
	}
	c := NewConsumer(consumetTag, q, ch)
	q.registerConsumer(c)
	ch.consumers[consumetTag] = c

	return nil
}

func (b *Broker) Ack(queueName string, deliveryTag uint16) error {
	b.mu.Lock()
	q, ok := b.queues[queueName]
	b.mu.Unlock()

	if !ok {
		return fmt.Errorf("queue %q not found", queueName)
	}

	q.mu.Lock()
	for _, c := range q.consumers {
		if _, found := c.inflightTags[deliveryTag]; found {
			delete(c.inflightTags, deliveryTag)
			delete(c.pendingMessages, deliveryTag)
			c.inflight--
			q.mu.Unlock()
			q.cond.Signal()
			return nil
		}
	}
	q.mu.Unlock()

	return fmt.Errorf("delivery tag %d not found in any consumer on queue %q", deliveryTag, queueName)
}
