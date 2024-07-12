package zeromq

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"

	"github.com/zeromq/gomq"
	"github.com/zeromq/gomq/zmtp"
)

// Router ...
type Router interface {
	gomq.ZeroMQSocket
	Bind(endpoint string)
}

// RouterSocket ...
type RouterSocket struct {
	*gomq.Socket
}

// NewRouterSocket ...
func NewRouterSocket(id string) *RouterSocket {
	return &RouterSocket{
		Socket: gomq.NewSocket(false, zmtp.RouterSocketType, zmtp.SocketIdentity(id), zmtp.NewSecurityNull()),
	}
}

// Bind ...
func (s *RouterSocket) Bind(endpoint string) {
	parts := strings.Split(endpoint, "://")

	ln, err := net.Listen(parts[0], parts[1])
	if err != nil {
		return
	}

	for {
		netConn, err := ln.Accept()
		if err != nil {
			log.Println("debug:", err)
			return
		}

		go func() {
			zmtpConn := zmtp.NewConnection(netConn)
			_, err := zmtpConn.Prepare(s.SecurityMechanism(), s.SocketType(), s.SocketIdentity(), true, nil)
			identity, _ := zmtpConn.GetIdentity()

			if err != nil {
				return
			}

			// remove conn, if identity already exist
			s.Socket.RemoveConnection(identity)

			log.Println(fmt.Sprintf("New conn: %s.", identity))
			s.Socket.AddConnection(gomq.NewConnection(netConn, zmtpConn))

			zmtpMsgs := make(chan *zmtp.Message, 10)
			zmtpConn.Recv(zmtpMsgs)
			for {
				msg := <-zmtpMsgs

				if msg != nil && msg.Err == io.EOF {
					log.Println(fmt.Sprintf("Conn lost: %s.", identity))
					s.RecvChannel() <- &zmtp.Message{
						Name: identity, //TODO
						Err:  io.EOF,
					}
					s.Socket.RemoveConnection(identity)
					break
				}

				s.RecvChannel() <- msg
			}
		}()
	}
}
