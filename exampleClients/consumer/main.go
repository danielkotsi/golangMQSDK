package main

import (
	"context"
	"log"
	"time"

	gomq "github.com/danielkotsi/golangMQSDK/gomqSDK"
)

func main() {
	cfg := gomq.Config{
		ClientName:   "consumer",
		Username:     "daniel",
		Password:     "123456789",
		ChannelMax:   10,
		FrameMax:     10372,
		HeartbeatSec: 10,
	}

	c, err := gomq.Connect("localhost:5672", cfg)
	if err != nil {
		log.Println(err)
		return
	}

	go workerB(c)
	go workerC(c)
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
		if msg.DeliveryTag%3 == 0 {
			err = channel.Nack(msg.DeliveryTag, false)
			if err != nil {
				log.Println("ack error:", err)
			}
			continue
		}
		log.Println("workerA, Received:")
		log.Println("workerA, Tag:     ", msg.DeliveryTag)
		log.Println("workerA,  Body:    ", string(msg.Body))

		if msg.DeliveryTag%3 == 0 {
			err = channel.Nack(msg.DeliveryTag, false)
			if err != nil {
				log.Println("ack error:", err)
			}
			continue
		}
		err = channel.Ack(msg.DeliveryTag)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}

func workerB(c *gomq.Client) {
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
		if msg.DeliveryTag%3 == 0 {
			err = channel.Nack(msg.DeliveryTag, false)
			if err != nil {
				log.Println("ack error:", err)
			}
			continue
		}
		log.Println("workerB, Received:")
		log.Println("workerB, Tag:     ", msg.DeliveryTag)
		log.Println("workerB,  Body:    ", string(msg.Body))

		err = channel.Ack(msg.DeliveryTag)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}

func workerC(c *gomq.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	channel, err := c.OpenChannel(ctx)
	if err != nil {
		log.Println("open channel error:", err)
		return
	}

	log.Println("Waiting for messages on dlq...")

	incoming, err := channel.Consume("dlq", ctx)
	if err != nil {
		log.Println("consume error:", err)
		return
	}

	for msg := range incoming {
		log.Println("workerC, Received:")
		log.Println("workerC, Tag:     ", msg.DeliveryTag)
		log.Println("workerC,  Body:    ", string(msg.Body))

		err = channel.Ack(msg.DeliveryTag)
		if err != nil {
			log.Println("ack error:", err)
		}
	}
}
