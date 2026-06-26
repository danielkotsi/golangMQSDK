package client

import (
	"GolangRabbitMQBroker/protocol"
	"bufio"
	"encoding/json"
	"log"
	"net"
)

type Config struct {
	ClientName   string
	Username     string
	Password     string
	ChannelMax   int
	FrameMax     int
	HeartbeatSec int
}

type Client struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	clientName   string
	username     string
	password     string
	channelMax   int
	framesMax    int
	heartbeatSec int
	Incoming     chan any
}

func Dial(address string, cfg Config) (*Client, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	c := &Client{
		conn:       conn,
		r:          bufio.NewReader(conn),
		w:          bufio.NewWriter(conn),
		clientName: cfg.ClientName,
		username:   cfg.Username,
		password:   cfg.Password,
		Incoming:   make(chan any, 100),
	}

	return c, nil
}

// Follow Protocol Rules to do Handshake or Return an Error
func (c *Client) Handshake() error {
	//send header
	err := c.WriteProtocolHeader()
	if err != nil {
		return err
	}
	//read connection.start
	var start protocol.ConnectionStart
	err = c.ReadMessage(&start)
	if err != nil {
		return err
	}
	//Send Connection.start_ok
	startOK := protocol.NewConnectionStartOK(c.clientName, c.username, c.password)
	err = c.WriteMessage(startOK)
	if err != nil {
		return err
	}
	//Read Connection.tune
	var connectionTune protocol.ConnectionTune
	err = c.ReadMessage(&connectionTune)
	if err != nil {
		return err
	}
	//Send Connection.tune_ok
	connectionTuneOK := protocol.NewConnectionTuneOK(c.channelMax, c.framesMax, c.heartbeatSec)
	err = c.WriteMessage(connectionTuneOK)
	if err != nil {
		return err
	}
	//Send Connection.Open
	connectionOpen := protocol.NewConnectionOpen()
	err = c.WriteMessage(connectionOpen)
	if err != nil {
		return err
	}
	//Read Connectin.Open_ok
	var connectionOpenOK protocol.ConnectionOpenOK
	err = c.ReadMessage(&connectionOpenOK)

	return nil
}

func (c *Client) WriteMessage(data any) error {
	return protocol.WriteMessage(c.w, data)
}

func (c *Client) WriteProtocolHeader() error {
	return protocol.WriteProtocolHeader(c.w)
}

func (c *Client) ReadMessage(pointer any) error {
	return protocol.ReadMessage(c.r, pointer)
}

func (c *Client) ReadEnvelope(env *protocol.Envelope) error {
	return protocol.ReadEnvelope(c.r, env)
}

func (c *Client) ReadLoop() {
	for {
		var env protocol.Envelope
		if err := c.ReadEnvelope(&env); err != nil {
			log.Println(err)
			close(c.Incoming)
		}
		event, err := c.decode(env)
		if err != nil {
			log.Println(err)
		}
		c.Incoming <- event
	}
}

func (c *Client) decode(env protocol.Envelope) (Event, error) {
	switch env.Type {
	case "basic.deliver":
		var delivery Delivery
		err := json.Unmarshal(env.Payload, &delivery)
		if err != nil {
			return Event{}, err
		}
		return Event{
			Type: env.Type,
			Data: delivery,
		}, nil
	case "something else":
		//case other cases:
		//etc
	}
	return Event{}, nil
}
