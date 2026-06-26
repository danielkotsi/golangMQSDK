package broker

import (
	"GolangRabbitMQBroker/protocol"
	"bufio"
	"log"
	"net"
)

type ConnectionState string

const (
	StateAwaitProtocolHeader ConnectionState = "await_protocol_header"
	StateAwaitStartOK        ConnectionState = "await_start_ok"
	StateAwaitTuneOK         ConnectionState = "await_tune_ok"
	StateAwaitOpen           ConnectionState = "await_open"
	StateOpen                ConnectionState = "open"
	StateClosed              ConnectionState = "closed"
)

type Connection struct {
	server *Server

	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	state ConnectionState

	clientName string
	username   string

	channelMax   int
	frameMax     int
	heartbeatSec int

	channels map[uint16]*Channel
}

func NewConnection(server *Server, netConn net.Conn) *Connection {
	return &Connection{
		server: server,
		conn:   netConn,
		r:      bufio.NewReader(netConn),
		w:      bufio.NewWriter(netConn),

		state:    StateAwaitProtocolHeader,
		channels: make(map[uint16]*Channel),
	}
}

func (c *Connection) Serve() error {
	for {
		c.WriteMessage("hello")
		log.Println("serving now")
		var str string
		if err := c.ReadMessage(&str); err != nil {
			return err
		}
	}
}
func (c *Connection) RunHandshake() error {
	//read Header
	err := c.ReadProtocolHeader()
	if err != nil {
		return err
	}
	log.Println("Header Recieved")
	c.state = StateAwaitStartOK
	//send connectionStart
	connectionStart := protocol.NewConnectionStart()
	err = c.WriteMessage(connectionStart)
	if err != nil {
		return err
	}
	//read connectionStartOk
	var connectionStartOk protocol.ConnectionStartOK
	err = c.ReadMessage(&connectionStartOk)
	if err != nil {
		return err
	}
	c.state = StateAwaitTuneOK
	log.Println("connection.start_ok received")
	//send connectionTune
	connectionTune := protocol.NewConnectionTune(c.server.config.ChannelMax, c.server.config.FramesMax, c.server.config.HeartbeatSec)
	c.WriteMessage(connectionTune)
	//read connectionTuneOK
	var connectionTuneOK protocol.ConnectionTuneOK
	err = c.ReadMessage(&connectionTuneOK)
	if err != nil {
		return err
	}
	log.Println("connection.tune_ok received")
	c.state = StateAwaitOpen
	//read connectionOpen
	var connectionOpen protocol.ConnectionOpen
	err = c.ReadMessage(&connectionOpen)
	if err != nil {
		return err
	}
	log.Println("connection.open received")

	//send connectionOpenOK
	connectionOpenOK := protocol.NewConnectionOpenOK()
	err = c.WriteMessage(connectionOpenOK)
	if err != nil {
		return err
	}
	c.state = StateOpen
	return nil
}

func (c *Connection) WriteMessage(data any) error {
	return protocol.WriteMessage(c.w, data)
}

func (c *Connection) ReadProtocolHeader() error {
	return protocol.ReadProtocolHeader(c.r)
}

func (c *Connection) ReadMessage(pointer any) error {
	return protocol.ReadMessage(c.r, pointer)
}
