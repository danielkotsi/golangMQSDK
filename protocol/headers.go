package protocol

import "encoding/json"

//for now until it is more clear to me the AuthMechanism is set to "plain" and it is hardcoded, might inspect later other options

const (
	ProtocolName       = "GOMQ"
	ProtocolVersion    = "1"
	ProtocolHeader     = ProtocolName + "/" + ProtocolVersion
	ServerName         = ProtocolName + "-Broker"
	AuthMechanismPlain = "plain"

	//handshake
	TypeConnectionStart   = "connection.start"
	TypeConnectionStartOK = "connection.start_ok"
	TypeConnectionTune    = "connection.tune"
	TypeConnectionTuneOK  = "connection.tune_ok"
	TypeConnectionOpen    = "connection.open"
	TypeConnectionOpenOK  = "connection.open_ok"

	//basic
	//client
	BasicPublish = "basic.publish"
	BasicConsume = "basic.consume"
	BasicAck     = "basic.ack"

	//server
	BasicDeliver   = "basic.deliver"
	BasicConsumeOK = "basic.consume-ok"

	//AMQP channel
	//client
	ChannelOpen = "channel.open"
	//server
	ChannelOpenOK = "channel.open-ok"

	//Queue
	//client
	QueueDeclare = "queue.declare"
	QueueBind    = "queue.bind"
	//server
	QueueDeclareOK = "queue.declare-ok"
	QueueBindOK    = "queue.bind-ok"
)

// this is the protocol overview
// TCP connect
// → protocol header	     (client → server)
// → connection.start        (server → client)
// → connection.start_ok     (client → server)
// → connection.tune         (server → client)
// → connection.tune_ok      (client → server)
// → connection.open         (client → server)
// → connection.open_ok      (server → client)
// → connection is now open
// server
type ConnectionStart struct {
	Type            string       `json:"type"`
	ServerName      string       `json:"server_name"`
	ProtocolVersion string       `json:"protocol_version"`
	AuthMechanism   string       `json:"auth_mechanism"`
	Capabilities    Capabilities `json:"capabilities,omitempty"`
}

type Capabilities struct {
	Heartbeats bool `json:"heartbeats,omitempty"`
}

// client
type ConnectionStartOK struct {
	Type          string `json:"type"`
	ClientName    string `json:"client_name"`
	AuthMechanism string `json:"auth_mechanism"`
	Username      string `json:"username"`
	Password      string `json:"password"`
}

// server
type ConnectionTune struct {
	Type         string `json:"type"`
	ChannelMax   int    `json:"channel_max"`
	FrameMax     int    `json:"frame_max"`
	HeartbeatSec int    `json:"heartbeat_sec"`
}

// client
type ConnectionTuneOK struct {
	Type         string `json:"type"`
	ChannelMax   int    `json:"channel_max"`
	FrameMax     int    `json:"frame_max"`
	HeartbeatSec int    `json:"heartbeat_sec"`
}

// client
type ConnectionOpen struct {
	Type string `json:"type"`
}

// server
type ConnectionOpenOK struct {
	Type string `json:"type"`
}

type Envelope struct {
	Type    string
	Payload json.RawMessage
}
