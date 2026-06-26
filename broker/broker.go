package broker

import (
	"log"
	"net"
	"sync"
)

type Server struct {
	addr        string
	listener    net.Listener
	mu          sync.Mutex
	connections map[*Connection]struct{}
	queues      map[string]*Queue
	config      ServerConfig
}
type ServerConfig struct {
	ChannelMax   int
	FramesMax    int
	HeartbeatSec int
}

func NewServer(addr string, serverconfig ServerConfig) *Server {
	return &Server{
		addr:        addr,
		config:      serverconfig,
		connections: make(map[*Connection]struct{}),
		queues:      make(map[string]*Queue),
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}

		go s.HandleConnection(conn)
	}
}

func (s *Server) HandleConnection(c net.Conn) {
	conn := NewConnection(s, c)

	s.mu.Lock()
	s.connections[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.connections, conn)
		s.mu.Unlock()
		c.Close()
	}()

	err := conn.RunHandshake()
	if err != nil {
		log.Println(err)
		return
	}

	err = conn.Serve()
	if err != nil {
		log.Println(err)
		return
	}
}
