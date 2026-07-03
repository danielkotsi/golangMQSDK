package broker

import (
	"GolangRabbitMQBroker/protocol"
	"bufio"
	"encoding/json"
	"log"
	"net"
	"sync"
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

	mu       sync.Mutex
	conn     net.Conn
	r        *bufio.Reader
	w        *bufio.Writer
	channels map[uint16]*Channel

	state ConnectionState

	clientName   string
	username     string
	channelMax   int
	frameMax     int
	heartbeatSec int
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
		log.Println("serving now")
		env, err := c.ReadEnvelope()
		if err != nil {
			return err
		}

		c.Handle(env)
	}
}

func (c *Connection) Handle(env protocol.Envelope) {
	switch env.Type {
	case protocol.ChannelOpenType:
		c.ChannelOpen(env)
	case protocol.ChannelCloseType:
		c.ChannelClose(env)
	default:
		c.routeToChannel(env)
	}
}

func (c *Connection) routeToChannel(env protocol.Envelope) {
	c.mu.Lock()
	ch, ok := c.channels[env.ChannelID]
	c.mu.Unlock()
	if !ok {
		c.WriteEnvelope(env.ChannelID, protocol.ErrorType, env.RequestID, protocol.Error{
			Message: "channel with requested channelID not open for communication",
		})
	}
	ch.route(env)
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

func (c *Connection) WriteEnvelope(channelID uint16, envType protocol.Method, reqID uint16, msg any) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	env := protocol.Envelope{
		ChannelID: channelID,
		RequestID: reqID,
		Type:      envType,
		Payload:   payload,
	}
	return protocol.WriteMessage(c.w, env)
}

func (c *Connection) ReadProtocolHeader() error {
	return protocol.ReadProtocolHeader(c.r)
}

func (c *Connection) ReadMessage(pointer any) error {
	return protocol.ReadMessage(c.r, pointer)
}

func (c *Connection) ReadEnvelope() (protocol.Envelope, error) {
	var env protocol.Envelope
	if err := protocol.ReadEnvelope(c.r, &env); err != nil {
		return protocol.Envelope{}, err
	}
	return env, nil
}

func (c *Connection) ChannelOpen(env protocol.Envelope) {
	var channelOpen protocol.ChannelOpen
	err := json.Unmarshal(env.Payload, &channelOpen)
	if err != nil {
		log.Println("error unmarshalling")
	}
	id := channelOpen.ID
	c.mu.Lock()
	c.channels[id] = &Channel{
		id:        id,
		conn:      c,
		broker:    c.server.Broker,
		consumers: make(map[string]*Consumer),
	}
	c.mu.Unlock()
	c.WriteEnvelope(0, protocol.ChannelOpenOKType, env.RequestID, &protocol.ChannelOpenOK{
		ID: id,
	})
}

func (c *Connection) ChannelClose(env protocol.Envelope) {
	id := env.ChannelID
	c.mu.Lock()
	delete(c.channels, id)
	c.mu.Unlock()
}
