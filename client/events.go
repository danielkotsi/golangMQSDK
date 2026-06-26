package client

type Event struct {
	Type string
	Data any
}
type Delivery struct {
	queue string
	//other missing parts as well
}
