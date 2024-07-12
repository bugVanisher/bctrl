package zeromq

import (
	"github.com/ugorji/go/codec"
)

const (
	ClientReady      = "client_ready"
	ClientStopped    = "client_stopped"
	Heartbeat        = "heartbeat"
	Spawning         = "spawning"
	SpawningComplete = "spawning_complete"
	Quit             = "quit"
	Exception        = "exception"
)

var (
	mh codec.MsgpackHandle
)

type Message struct {
	Type   string                 `codec:"type"`
	Data   map[string]interface{} `codec:"data"`
	NodeID string                 `codec:"node_id"`
}

func NewMessage(t string, data map[string]interface{}, nodeID string) (msg *Message) {
	return &Message{
		Type:   t,
		Data:   data,
		NodeID: nodeID,
	}
}

func (m *Message) Serialize() (out []byte, err error) {
	mh.StructToArray = true
	enc := codec.NewEncoderBytes(&out, &mh)
	err = enc.Encode(m)
	return out, err
}

func NewMessageFromBytes(raw []byte) (newMsg *Message, err error) {
	mh.StructToArray = true
	dec := codec.NewDecoderBytes(raw, &mh)
	newMsg = &Message{}
	err = dec.Decode(newMsg)
	return newMsg, err
}
