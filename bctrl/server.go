package bctrl

import (
	"boomer_web/bctrl/zeromq"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"sync"
	"sync/atomic"
)

type Server struct {
	bindHost         string
	bindPort         int
	fromClient       chan *zeromq.Message
	toClient         chan *zeromq.Message
	routerSocket     *zeromq.RouterSocket
	shutdownSignal   chan bool
	disconnectedChan chan bool
	wg               sync.WaitGroup
	clients          *sync.Map
}

func newServer(bindHost string, bindPort int) (server *Server) {
	return &Server{
		bindHost:         bindHost,
		bindPort:         bindPort,
		fromClient:       make(chan *zeromq.Message, 100),
		toClient:         make(chan *zeromq.Message, 100),
		shutdownSignal:   make(chan bool, 1),
		disconnectedChan: make(chan bool, 1),
		routerSocket:     zeromq.NewRouterSocket("MasterRunner"),
	}
}

// Recv ...
func (s *Server) recv() *zeromq.Message {
	msg := <-s.fromClient
	//fmt.Println("Recv: ", msg)
	return msg
}

// Send ...
func (s *Server) send(msg *zeromq.Message) {
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
				serializedMessage, err := msg.Serialize()
				if err != nil {
					log.Err(err).Msg("Msgpack encode fail")
				}

				if conn, err := s.routerSocket.GetConnection(msg.NodeID); err == nil {
					err := conn.Send(serializedMessage)
					if err != nil {

						return
					}
				} else {
					log.Error().Msg("NodeID not exist")
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
					s.fromClient <- &zeromq.Message{
						Type:   "eof",
						Data:   nil,
						NodeID: msg.Name,
					}
				}
				if msg.Body == nil || len(msg.Body) < 1 {
					continue
				}

				msg2, err := zeromq.NewMessageFromBytes(msg.Body[0])
				if err != nil {
					log.Err(err).Msg("NewMessageFromBytes err...")
				}
				s.fromClient <- msg2
			}
		}
	}()
}

func (s *Server) getClients() *sync.Map {
	return s.clients
}

func (s *Server) Close() {
	close(s.shutdownSignal)
}

func (s *Server) getAllWorkers() (wns []WorkerNode) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			wns = append(wns, workerInfo.getWorkerInfo())
		}
		return true
	})
	return wns
}

func (s *Server) getAvailableClientsLength() (l int) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.isAvailable() {
				l++
			}
		}
		return true
	})
	return
}

func (s *Server) getReadyClientsLength() (l int) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.isReady() {
				l++
			}
		}
		return true
	})
	return
}

func (s *Server) getStartingClientsLength() (l int) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.isStarting() {
				l++
			}
		}
		return true
	})
	return
}

func (s *Server) getWorkersLengthByState(state int32) (l int) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.getState() == state {
				l++
			}
		}
		return true
	})
	return
}

func (s *Server) getCurrentUsers() (l int) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.isStarting() {
				l += int(workerInfo.getUserCount())
			}
		}
		return true
	})
	return
}

type WorkerNode struct {
	ID                string  `json:"id"`
	IP                string  `json:"ip"`
	State             int32   `json:"state"`
	Heartbeat         int32   `json:"heartbeat"`
	UserCount         int64   `json:"user_count"`
	WorkerCPUUsage    float64 `json:"worker_cpu_usage"`
	CPUUsage          float64 `json:"cpu_usage"`
	CPUWarningEmitted bool    `json:"cpu_warning_emitted"`
	WorkerMemoryUsage float64 `json:"worker_memory_usage"`
	MemoryUsage       float64 `json:"memory_usage"`
	mutex             sync.RWMutex
	disconnectedChan  chan bool
}

func newWorkerNode(id string) *WorkerNode {
	return &WorkerNode{State: StateInit, ID: id, Heartbeat: 3, disconnectedChan: make(chan bool)}
}

func (w *WorkerNode) getState() int32 {
	return atomic.LoadInt32(&w.State)
}

func (w *WorkerNode) setState(state int32) {
	atomic.StoreInt32(&w.State, state)
}

func (w *WorkerNode) isStarting() bool {
	return w.getState() == StateRunning || w.getState() == StateSpawning
}

func (w *WorkerNode) isStopping() bool {
	return w.getState() == StateStopping
}

func (w *WorkerNode) isAvailable() bool {
	state := w.getState()
	return state != StateMissing && state != StateQuitting
}

func (w *WorkerNode) isReady() bool {
	state := w.getState()
	return state == StateInit || state == StateStopped
}

func (w *WorkerNode) updateHeartbeat(heartbeat int32) {
	atomic.StoreInt32(&w.Heartbeat, heartbeat)
}

func (w *WorkerNode) getHeartbeat() int32 {
	return atomic.LoadInt32(&w.Heartbeat)
}

func (w *WorkerNode) updateUserCount(spawnCount int64) {
	atomic.StoreInt64(&w.UserCount, spawnCount)
}

func (w *WorkerNode) getUserCount() int64 {
	return atomic.LoadInt64(&w.UserCount)
}

func (w *WorkerNode) updateCPUUsage(cpuUsage float64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.CPUUsage = cpuUsage
}

func (w *WorkerNode) getCPUUsage() float64 {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.CPUUsage
}

func (w *WorkerNode) updateWorkerCPUUsage(workerCPUUsage float64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.WorkerCPUUsage = workerCPUUsage
}

func (w *WorkerNode) getWorkerCPUUsage() float64 {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.WorkerCPUUsage
}

func (w *WorkerNode) updateCPUWarningEmitted(cpuWarningEmitted bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.CPUWarningEmitted = cpuWarningEmitted
}

func (w *WorkerNode) getCPUWarningEmitted() bool {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.CPUWarningEmitted
}

func (w *WorkerNode) updateWorkerMemoryUsage(workerMemoryUsage float64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.WorkerMemoryUsage = workerMemoryUsage
}

func (w *WorkerNode) getWorkerMemoryUsage() float64 {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.WorkerMemoryUsage
}

func (w *WorkerNode) updateMemoryUsage(memoryUsage float64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.MemoryUsage = memoryUsage
}

func (w *WorkerNode) getMemoryUsage() float64 {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return w.MemoryUsage
}

func (w *WorkerNode) getWorkerInfo() WorkerNode {
	w.mutex.RLock()
	defer w.mutex.RUnlock()
	return WorkerNode{
		ID:                w.ID,
		IP:                w.IP,
		State:             w.getState(),
		Heartbeat:         w.getHeartbeat(),
		UserCount:         w.getUserCount(),
		WorkerCPUUsage:    w.getWorkerCPUUsage(),
		CPUUsage:          w.getCPUUsage(),
		CPUWarningEmitted: w.getCPUWarningEmitted(),
		WorkerMemoryUsage: w.getWorkerMemoryUsage(),
		MemoryUsage:       w.getMemoryUsage(),
	}
}

func (s *Server) sendBroadcasts(msg *zeromq.Message) {
	s.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {

			if !workerInfo.isAvailable() {
				return true
			}
			s.send(&zeromq.Message{
				Type:   msg.Type,
				Data:   msg.Data,
				NodeID: workerInfo.ID,
			})
		}
		return true
	})
}
