package bctrl

import (
	"boomer_web/bctrl/zeromq"
	"errors"
	"log"
	"time"
)

type Profile struct {
	SpawnCount               int64         `json:"spawn-count,omitempty" yaml:"spawn-count,omitempty" mapstructure:"spawn-count,omitempty"`
	SpawnRate                float64       `json:"spawn-rate,omitempty" yaml:"spawn-rate,omitempty" mapstructure:"spawn-rate,omitempty"`
	RunTime                  int64         `json:"run-time,omitempty" yaml:"run-time,omitempty" mapstructure:"run-time,omitempty"`
	MaxRPS                   int64         `json:"max-rps,omitempty" yaml:"max-rps,omitempty" mapstructure:"max-rps,omitempty"`
	LoopCount                int64         `json:"loop-count,omitempty" yaml:"loop-count,omitempty" mapstructure:"loop-count,omitempty"`
	RequestIncreaseRate      string        `json:"request-increase-rate,omitempty" yaml:"request-increase-rate,omitempty" mapstructure:"request-increase-rate,omitempty"`
	MemoryProfile            string        `json:"memory-profile,omitempty" yaml:"memory-profile,omitempty" mapstructure:"memory-profile,omitempty"`
	MemoryProfileDuration    time.Duration `json:"memory-profile-duration,omitempty" yaml:"memory-profile-duration,omitempty" mapstructure:"memory-profile-duration,omitempty"`
	CPUProfile               string        `json:"cpu-profile,omitempty" yaml:"cpu-profile,omitempty" mapstructure:"cpu-profile,omitempty"`
	CPUProfileDuration       time.Duration `json:"cpu-profile-duration,omitempty" yaml:"cpu-profile-duration,omitempty" mapstructure:"cpu-profile-duration,omitempty"`
	PrometheusPushgatewayURL string        `json:"prometheus-gateway,omitempty" yaml:"prometheus-gateway,omitempty" mapstructure:"prometheus-gateway,omitempty"`
	DisableConsoleOutput     bool          `json:"disable-console-output,omitempty" yaml:"disable-console-output,omitempty" mapstructure:"disable-console-output,omitempty"`
	DisableCompression       bool          `json:"disable-compression,omitempty" yaml:"disable-compression,omitempty" mapstructure:"disable-compression,omitempty"`
	DisableKeepalive         bool          `json:"disable-keepalive,omitempty" yaml:"disable-keepalive,omitempty" mapstructure:"disable-keepalive,omitempty"`
}

type MasterRunner struct {
	workerMng *WorkerManager
	server    *Server
}

var DefaultMasterRunner = NewMasterRunnerController()

func Start() {
	DefaultMasterRunner.server.Start()
	DefaultMasterRunner.clientListener()
}

func NewMasterRunnerController() *MasterRunner {
	return &MasterRunner{
		workerMng: newWorkerMng(),
		server:    newServer("0.0.0.0", 5557),
	}
}

func (bc *MasterRunner) clientListener() {
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
			onWorkerReport(msg.Data)
		case "exception":
		case "eof":
			bc.workerMng.delWorker(msg.NodeID)
		}
	}
}

func (bc *MasterRunner) addWorker(msg *zeromq.Message) {
	worker := &Worker{
		NodeId:        msg.NodeID,
		LastHeartbeat: time.Now(),
	}
	bc.workerMng.addWorker(worker)
}

func (bc *MasterRunner) spawn(userCount uint64, rate float64) error {
	if userCount <= 0 || rate <= 0 {
		return errors.New("invalid params")
	}
	for _, worker := range bc.workerMng.workers {
		msg := &zeromq.Message{
			NodeID: worker.NodeId,
			Type:   "spawn",
			Data: map[string]interface{}{
				"num_users":  userCount,
				"spawn_rate": rate,
			},
		}
		bc.server.send(msg)
	}
	return nil
}
