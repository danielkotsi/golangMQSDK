package gomq

import "github.com/danielkotsi/golangMQSDK/protocol"

type Event struct {
	Type protocol.Method
	Data any
}
