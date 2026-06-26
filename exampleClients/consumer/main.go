package main

import (
	"GolangRabbitMQBroker/client"
	"log"
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

	for msg := range c.Incoming {
		log.Println("this is a message recieved", msg)
	}

	log.Println("Connection was opened")
}
