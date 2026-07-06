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

	c, err := client.Connect("localhost:5672", cfg)
	if err != nil {
		log.Println(err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel, err := c.OpenChannel(ctx)
	if err != nil {
		log.Println("open channel error:", err)
		return
	}

	_, err = channel.DeclareExchange("dlx", ctx)
	if err != nil {
		log.Println("declare exchange error:", err)
		return
	}

	_, err = channel.DeclareQueue("dlq", ctx, "", "")
	if err != nil {
		log.Println("declare queue error:", err)
		return
	}

	err = channel.BindQueue("dlq", "dlx", "dead_emails", ctx)
	if err != nil {
		log.Println("bind queue error:", err)
		return
	}
	_, err = channel.DeclareExchange("emails", ctx)
	if err != nil {
		log.Println("declare exchange error:", err)
		return
	}

	_, err = channel.DeclareQueue("email_queue", ctx, "dlx", "dead_emails")
	if err != nil {
		log.Println("declare queue error:", err)
		return
	}

	err = channel.BindQueue("email_queue", "emails", "email.sent", ctx)
	if err != nil {
		log.Println("bind queue error:", err)
		return
	}

	for i := 0; i < 3; i++ {
		body, _ := json.Marshal("message number " + string('0'+rune(i)))
		err = channel.Publish(protocol.Publish{
			Exchange:   "emails",
			RoutingKey: "email.sent",
			Body:       body,
		})
		if err != nil {
			log.Println("publish error:", err)
		}
	}

	log.Println("Published 3 messages")
}
