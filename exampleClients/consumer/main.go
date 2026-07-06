package main

import (
	"GolangRabbitMQBroker/client"
	"context"
	"log"
	"time"
)

func main() {
	cfg := client.Config{
		ClientName:   "consumer",
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

	go workerB(c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	channel, err := c.OpenChannel(ctx)
	if err != nil {
		log.Println("open channel error:", err)
		return
	}

	log.Println("Waiting for messages on email_queue...")

	incoming, err := channel.Consume("email_queue", ctx)
	if err != nil {
		log.Println("consume error:", err)
		return
	}

	for msg := range incoming {
		log.Println("workerA, Received:")
		log.Println("workerA, Tag:     ", msg.DeliveryTag)
		log.Println("workerA,  Body:    ", string(msg.Body))
		time.Sleep(5 * time.Second)

		err = channel.Ack(msg.DeliveryTag)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}

func workerB(c *client.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	channel, err := c.OpenChannel(ctx)
	if err != nil {
		log.Println("open channel error:", err)
		return
	}

	log.Println("Waiting for messages on email_queue...")

	incoming, err := channel.Consume("email_queue", ctx)
	if err != nil {
		log.Println("consume error:", err)
		return
	}

	for msg := range incoming {
		log.Println("workerB, Received:")
		log.Println("workerB, Tag:     ", msg.DeliveryTag)
		log.Println("workerB,  Body:    ", string(msg.Body))
		time.Sleep(3 * time.Second)

		err = channel.Ack(msg.DeliveryTag)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}
