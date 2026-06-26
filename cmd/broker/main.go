package main

import (
	"GolangRabbitMQBroker/broker"
	"log"
)

func main() {
	serverconfig := &broker.ServerConfig{
		ChannelMax:   10,
		FramesMax:    10372,
		HeartbeatSec: 10,
	}
	server := broker.NewServer(":5672", *serverconfig)
	log.Printf("MQ server started on :5672")
	if err := server.ListenAndServe(); err != nil {
		log.Println(err)
		return
	}
}
