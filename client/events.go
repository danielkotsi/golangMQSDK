package gomq

import "golangMQSDK/protocol"

type Event struct {
	Type protocol.Method
	Data any
}
