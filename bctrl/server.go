package bctrl

import (
	"fmt"
	"io"
	"log"
)

type Server struct {
	bindHost       string
	bindPort       int
	fromClient     chan *message
	toClient       chan *message
	routerSocket   *RouterSocket
	shutdownSignal chan bool
}

func newTestServer(bindHost string, bindPort int) (server *Server) {
	return &Server{
		bindHost:       bindHost,
		bindPort:       bindPort,
		fromClient:     make(chan *message, 100),
		toClient:       make(chan *message, 100),
		shutdownSignal: make(chan bool, 1),
		routerSocket:   NewRouterSocket("BoomerController"),
	}
}

// Recv ...
func (s *Server) recv() *message {
	msg := <-s.fromClient
	//fmt.Println("Recv: ", msg)
	return msg
}

// Send ...
func (s *Server) send(msg *message) {
	//fmt.Println("Send: ", msg)
	s.toClient <- msg
}

func (s *Server) Start() {
	go s.routerSocket.Bind(fmt.Sprintf("tcp://%s:%d", s.bindHost, s.bindPort))
	go func() {
		for {
			select {
			case <-s.shutdownSignal:
				s.routerSocket.Close()
				return
			case msg := <-s.toClient:
				serializedMessage, err := msg.serialize()
				if err != nil {
					log.Println("Msgpack encode fail:", err)
				}

				if conn, err := s.routerSocket.GetConnection(msg.NodeID); err == nil {
					conn.Send(serializedMessage)
				} else {
					log.Println("NodeID not exist")
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-s.shutdownSignal:
				s.routerSocket.Close()
				return
			default:
				msg := <-s.routerSocket.RecvChannel()
				if msg != nil && msg.Err == io.EOF {
					s.fromClient <- &message{
						Type:   "eof",
						Data:   nil,
						NodeID: msg.Name,
					}
				}
				if msg.Body == nil || len(msg.Body) < 1 {
					continue
				}

				msg2, err := newMessageFromBytes(msg.Body[0])
				if err != nil {
					log.Println(err)
				}
				s.fromClient <- msg2
			}
		}
	}()
}

func (s *Server) Close() {
	close(s.shutdownSignal)
}
