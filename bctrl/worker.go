package bctrl

import (
	"sync"
	"time"
)

type Worker struct {
	NodeId          string
	Status          int
	LastHeartbeat   time.Time
	CurrentCPUUsage float64
	CurrentRamUsage float64
}

type WorkerManager struct {
	workers map[string]*Worker
	lock    *sync.RWMutex
}

func newWorkerMng() *WorkerManager {
	return &WorkerManager{
		workers: make(map[string]*Worker, 3),
		lock:    &sync.RWMutex{},
	}
}

func (w *WorkerManager) addWorker(worker *Worker) bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	if worker == nil || worker.NodeId == "" {
		return false
	}
	w.workers[worker.NodeId] = worker
	return true
}

func (w *WorkerManager) delWorker(nodeId string) bool {
	w.lock.Lock()
	defer w.lock.Unlock()
	if _, ok := w.workers[nodeId]; !ok {
		return false
	}
	delete(w.workers, nodeId)
	return true
}
