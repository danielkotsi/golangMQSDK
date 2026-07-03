package main

import (
	"GolangRabbitMQBroker/client"
	"GolangRabbitMQBroker/protocol"
	"context"
	"encoding/json"
	"log"
	"time"
)

func main() {
	cfg := client.Config{
		ClientName:   "publisher",
		Username:     "daniel",
		Password:     "123456789",
		ChannelMax:   10,
		FrameMax:     10372,
		HeartbeatSec: 10,
	}

	c, err := client.Dial("localhost:5672", cfg)
	if err != nil {
		log.Println(err)
		return
	}
	err = c.Handshake()
	if err != nil {
		log.Println(err)
		return
	}
	go c.ReadLoop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	channel, err := c.OpenChannel(ctx)
	if err != nil {
		log.Println("open channel error:", err)
	}

	q, err := channel.DeclareQueue("newqueue", ctx)
	if err != nil {
		log.Println("declare queue error:", err)
	}
	q = &client.Queue{}
	consumectx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	incoming, err := channel.Consume(q.Name, consumectx)
	if err != nil {
		log.Println("consume error:", err)
	}

	bytes, err := json.Marshal("this is the first message that goes through the broker")
	if err != nil {
		log.Println(err)
	}
	err = channel.Publish(protocol.Publish{
		Queue: "newquue",
		Body:  bytes,
	})
	if err != nil {
		log.Println("publish error:", err)
	}

	for msg := range incoming {
		log.Println("hello this is the message that i recieved")
		log.Println("This is the queue:", msg.Queue)
		log.Println("And this is the body:", string(msg.Body))
	}

	log.Println("Connection was opened")
}
