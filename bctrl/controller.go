package bctrl

import (
	"log"
	"time"
)

type BoomerController struct {
	workerMng *WorkerManager
	server    *Server
}

var DefaultBoomerController = NewBoomerController()

func Start() {
	DefaultBoomerController.server.Start()
	DefaultBoomerController.clientListener()
}

func NewBoomerController() *BoomerController {
	return &BoomerController{
		workerMng: newWorkerMng(),
		server:    newTestServer("0.0.0.0", 5557),
	}
}

func (bc *BoomerController) clientListener() {
	for {
		msg := bc.server.recv()
		log.Println(msg)
		switch msg.Type {
		case "client_ready": //worker ready
			bc.addWorker(msg)
		case "heartbeat":
		case "hatching":
		case "hatch_complete":
		case "client_stopped":
		case "stats":
		case "exception":
		case "eof":
			bc.workerMng.delWorker(msg.NodeID)
		}
	}
}

func (bc *BoomerController) addWorker(msg *message) {
	worker := &Worker{
		NodeId:        msg.NodeID,
		LastHeartbeat: time.Now(),
	}
	bc.workerMng.addWorker(worker)
}
