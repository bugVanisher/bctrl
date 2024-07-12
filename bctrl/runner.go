package bctrl

import (
	"boomer_web/bctrl/zeromq"
	"errors"
	"fmt"
	"github.com/jinzhu/copier"
	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StateInit     = iota + 1 // initializing
	StateSpawning            // spawning
	StateRunning             // running
	StateStopping            // stopping
	StateStopped             // stopped
	StateQuitting            // quitting
	StateMissing             // missing
)

func getStateName(state int32) (stateName string) {
	switch state {
	case StateInit:
		stateName = "initializing"
	case StateSpawning:
		stateName = "spawning"
	case StateRunning:
		stateName = "running"
	case StateStopping:
		stateName = "stopping"
	case StateStopped:
		stateName = "stopped"
	case StateQuitting:
		stateName = "quitting"
	case StateMissing:
		stateName = "missing"
	}
	return
}

const (
	reportStatsInterval  = 3 * time.Second
	heartbeatInterval    = 1 * time.Second
	heartbeatLiveness    = 3 * time.Second
	stateMachineInterval = 1 * time.Second
)

type Loop struct {
	loopCount     int64 // more than 0
	acquiredCount int64 // count acquired of load testing
	finishedCount int64 // count finished of load testing
}

func (l *Loop) isFinished() bool {
	// return true when there are no remaining loop count to test
	return atomic.LoadInt64(&l.finishedCount) == l.loopCount
}

func (l *Loop) acquire() bool {
	// get one ticket when there are still remaining loop count to test
	// return true when getting ticket successfully
	if atomic.LoadInt64(&l.acquiredCount) < l.loopCount {
		atomic.AddInt64(&l.acquiredCount, 1)
		return true
	}
	return false
}

func (l *Loop) increaseFinishedCount() {
	atomic.AddInt64(&l.finishedCount, 1)
}

type Controller struct {
	mutex             sync.RWMutex
	once              sync.Once
	currentClientsNum int64 // current clients count
	spawnCount        int64 // target clients to spawn
	spawnRate         float64
	rebalance         chan bool // dynamically balance boomer running parameters
	spawnDone         chan struct{}
}

func (c *Controller) setSpawn(spawnCount int64, spawnRate float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if spawnCount > 0 {
		atomic.StoreInt64(&c.spawnCount, spawnCount)
	}
	if spawnRate > 0 {
		c.spawnRate = spawnRate
	}
}

func (c *Controller) setSpawnCount(spawnCount int64) {
	if spawnCount > 0 {
		atomic.StoreInt64(&c.spawnCount, spawnCount)
	}
}

func (c *Controller) setSpawnRate(spawnRate float64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if spawnRate > 0 {
		c.spawnRate = spawnRate
	}
}

func (c *Controller) getSpawnCount() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return atomic.LoadInt64(&c.spawnCount)
}

func (c *Controller) getSpawnRate() float64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.spawnRate
}

func (c *Controller) getSpawnDone() chan struct{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.spawnDone
}

func (c *Controller) getCurrentClientsNum() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return atomic.LoadInt64(&c.currentClientsNum)
}

func (c *Controller) spawnCompete() {
	close(c.spawnDone)
}

func (c *Controller) getRebalanceChan() chan bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.rebalance
}

func (c *Controller) isFinished() bool {
	// return true when workers acquired
	return atomic.LoadInt64(&c.currentClientsNum) == atomic.LoadInt64(&c.spawnCount)
}

func (c *Controller) acquire() bool {
	// get one ticket when there are still remaining spawn count to test
	// return true when getting ticket successfully
	if atomic.LoadInt64(&c.currentClientsNum) < atomic.LoadInt64(&c.spawnCount) {
		atomic.AddInt64(&c.currentClientsNum, 1)
		return true
	}
	return false
}

func (c *Controller) erase() bool {
	// return true if acquiredCount > spawnCount
	if atomic.LoadInt64(&c.currentClientsNum) > atomic.LoadInt64(&c.spawnCount) {
		atomic.AddInt64(&c.currentClientsNum, -1)
		return true
	}
	return false
}

func (c *Controller) increaseFinishedCount() {
	atomic.AddInt64(&c.currentClientsNum, -1)
}

func (c *Controller) reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	atomic.StoreInt64(&c.spawnCount, 0)
	c.spawnRate = 0
	atomic.StoreInt64(&c.currentClientsNum, 0)
	c.spawnDone = make(chan struct{})
	c.rebalance = make(chan bool)
	c.once = sync.Once{}
}

type runner struct {
	state int32

	totalTaskWeight int
	mutex           sync.RWMutex

	rateLimitEnabled bool
	stats            *requestStats

	spawnCount int64 // target clients to spawn
	spawnRate  float64
	runTime    int64

	controller *Controller
	loop       *Loop // specify loop count for testcase, count = loopCount * spawnCount

	// stop signals the run goroutine should shutdown.
	stopChan chan bool
	// all running workers(goroutines) will select on this channel.
	// stopping is closed by run goroutine on shutdown.
	stoppingChan chan bool
	// done is closed when all goroutines from start() complete.
	doneChan chan bool
	// when this channel is closed, all statistics are reported successfully
	reportedChan chan bool

	// close this channel will stop all goroutines used in runner.
	closeChan chan bool

	// wgMu blocks concurrent waitgroup mutation while boomer stopping
	wgMu sync.RWMutex
	// wg is used to wait for all running workers(goroutines) that depends on the boomer state
	// to exit when stopping the boomer.
	wg sync.WaitGroup

	outputs []Output
}

func (r *runner) setSpawnRate(spawnRate float64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if spawnRate > 0 {
		r.spawnRate = spawnRate
	}
}

func (r *runner) getSpawnRate() float64 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.spawnRate
}

func (r *runner) setRunTime(runTime int64) {
	atomic.StoreInt64(&r.runTime, runTime)
}

func (r *runner) getRunTime() int64 {
	return atomic.LoadInt64(&r.runTime)
}

func (r *runner) getSpawnCount() int64 {
	return atomic.LoadInt64(&r.spawnCount)
}

func (r *runner) setSpawnCount(spawnCount int64) {
	atomic.StoreInt64(&r.spawnCount, spawnCount)
}

// safeRun runs fn and recovers from unexpected panics.
// it prevents panics from Task.Fn crashing boomer.
func (r *runner) safeRun(fn func()) {
	defer func() {
		// don't panic
		err := recover()
		if err != nil {
			stackTrace := debug.Stack()
			errMsg := fmt.Sprintf("%v", err)
			os.Stderr.Write([]byte(errMsg))
			os.Stderr.Write([]byte("\n"))
			os.Stderr.Write(stackTrace)
		}
	}()
	fn()
}

func (r *runner) addOutput(o Output) {
	r.outputs = append(r.outputs, o)
}

func (r *runner) outputOnStart() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStart()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnEvent(data map[string]interface{}) {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnEvent(data)
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnStop() {
	defer func() {
		r.outputs = make([]Output, 0)
	}()
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStop()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) reportStats() {
	data := r.stats.collectReportData()
	data["user_count"] = r.controller.getCurrentClientsNum()
	data["state"] = atomic.LoadInt32(&r.state)
	r.outputOnEvent(data)
}

func (r *runner) reportTestResult() {
	// convert stats in total
	var statsTotal interface{} = r.stats.total.serialize()
	entryTotalOutput, err := deserializeStatsEntry(statsTotal)
	if err != nil {
		return
	}
	duration := time.Duration(entryTotalOutput.LastRequestTimestamp-entryTotalOutput.StartTime) * time.Millisecond
	currentTime := time.Now()
	println(fmt.Sprint("=========================================== Statistics Summary =========================================="))
	println(fmt.Sprintf("Current time: %s, Users: %v, Duration: %v, Accumulated Transactions: %d Passed, %d Failed",
		currentTime.Format("2006/01/02 15:04:05"), r.controller.getCurrentClientsNum(), duration, r.stats.transactionPassed, r.stats.transactionFailed))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Name", "# requests", "# fails", "Median", "Average", "Min", "Max", "Content Size", "# reqs/sec", "# fails/sec"})
	row := make([]string, 10)
	row[0] = entryTotalOutput.Name
	row[1] = strconv.FormatInt(entryTotalOutput.NumRequests, 10)
	row[2] = strconv.FormatInt(entryTotalOutput.NumFailures, 10)
	row[3] = strconv.FormatInt(entryTotalOutput.medianResponseTime, 10)
	row[4] = strconv.FormatFloat(entryTotalOutput.avgResponseTime, 'f', 2, 64)
	row[5] = strconv.FormatInt(entryTotalOutput.MinResponseTime, 10)
	row[6] = strconv.FormatInt(entryTotalOutput.MaxResponseTime, 10)
	row[7] = strconv.FormatInt(entryTotalOutput.avgContentLength, 10)
	row[8] = strconv.FormatFloat(entryTotalOutput.currentRps, 'f', 2, 64)
	row[9] = strconv.FormatFloat(entryTotalOutput.currentFailPerSec, 'f', 2, 64)
	table.Append(row)
	table.Render()
	println()
}

func (r *runner) reset() {
	r.controller.reset()
	r.stats.clearAll()
	r.stoppingChan = make(chan bool)
	r.doneChan = make(chan bool)
	r.reportedChan = make(chan bool)
}

func (r *runner) runTimeCheck(runTime int64) {
	if runTime <= 0 {
		return
	}
	stopTime := time.Now().Unix() + runTime

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-r.stopChan:
			return
		case <-ticker.C:
			if time.Now().Unix() > stopTime {
				r.stop()
				return
			}
		}
	}
}

// goAttach creates a goroutine on a given function and tracks it using
// the runner waitgroup.
// The passed function should interrupt on r.stoppingNotify().
func (r *runner) goAttach(f func()) {
	r.wgMu.RLock() // this blocks with ongoing close(s.stopping)
	defer r.wgMu.RUnlock()
	select {
	case <-r.stoppingChan:
		log.Warn().Msg("runner has stopped; skipping GoAttach")
		return
	default:
	}

	// now safe to add since waitgroup wait has not started yet
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		f()
	}()
}

func (r *runner) statsStart() {
	ticker := time.NewTicker(reportStatsInterval)
	for {
		select {
		// record stats
		case t := <-r.stats.transactionChan:
			r.stats.logTransaction(t.name, t.success, t.elapsedTime, t.contentSize)
		case m := <-r.stats.requestSuccessChan:
			r.stats.logRequest(m.requestType, m.name, m.responseTime, m.responseLength)
		case n := <-r.stats.requestFailureChan:
			r.stats.logRequest(n.requestType, n.name, n.responseTime, 0)
			r.stats.logError(n.requestType, n.name, n.errMsg)
		// report stats
		case <-ticker.C:
			r.reportStats()
			// close reportedChan and return if the last stats is reported successfully
			if !r.isStarting() && !r.isStopping() {
				close(r.reportedChan)
				log.Info().Msg("Quitting statsStart")
				return
			}
		}
	}
}

func (r *runner) stop() {
	// stop previous goroutines without blocking
	// those goroutines will exit when r.safeRun returns
	r.gracefulStop()

	r.updateState(StateStopped)
}

// gracefulStop stops the boomer gracefully, and shuts down the running goroutine.
// gracefulStop should be called after a start(), otherwise it will block forever.
// When stopping leader, Stop transfers its leadership to one of its peers
// before stopping the boomer.
// gracefulStop terminates the boomer and performs any necessary finalization.
// Do and Process cannot be called after Stop has been invoked.
func (r *runner) gracefulStop() {
	select {
	case r.stopChan <- true:
	case <-r.doneChan:
		return
	}
	<-r.doneChan
}

// stopNotify returns a channel that receives a bool type value
// when the runner is stopped.
func (r *runner) stopNotify() <-chan bool { return r.doneChan }

func (r *runner) getState() int32 {
	return atomic.LoadInt32(&r.state)
}

func (r *runner) updateState(state int32) {
	log.Debug().Int32("from", atomic.LoadInt32(&r.state)).Int32("to", state).Msg("update runner state")
	atomic.StoreInt32(&r.state, state)
}

func (r *runner) isStarting() bool {
	return r.getState() == StateRunning || r.getState() == StateSpawning
}

func (r *runner) isStopping() bool {
	return r.getState() == StateStopping
}

// masterRunner controls worker to spawn goroutines and collect stats.
type masterRunner struct {
	runner

	masterBindHost string
	masterBindPort int
	server         *Server

	autoStart            bool
	expectWorkers        int
	expectWorkersMaxWait int

	profile *Profile

	parseTestCasesChan chan bool
	testCaseBytesChan  chan []byte
	testCasesBytes     []byte
}

func newMasterRunner(masterBindHost string, masterBindPort int) *masterRunner {
	return &masterRunner{
		runner: runner{
			state:        StateInit,
			stoppingChan: make(chan bool),
			doneChan:     make(chan bool),
			closeChan:    make(chan bool),
			wg:           sync.WaitGroup{},
			wgMu:         sync.RWMutex{},
		},
		masterBindHost:     masterBindHost,
		masterBindPort:     masterBindPort,
		server:             newServer(masterBindHost, masterBindPort),
		parseTestCasesChan: make(chan bool),
		testCaseBytesChan:  make(chan []byte),
	}
}

func (r *masterRunner) setExpectWorkers(expectWorkers int, expectWorkersMaxWait int) {
	r.expectWorkers = expectWorkers
	r.expectWorkersMaxWait = expectWorkersMaxWait
}

func (r *masterRunner) heartbeatWorker() {
	log.Info().Msg("heartbeatWorker, listen and record heartbeat from worker")
	heartBeatTicker := time.NewTicker(heartbeatInterval)
	reportTicker := time.NewTicker(heartbeatLiveness)
	for {
		select {
		case <-r.closeChan:
			return
		case <-heartBeatTicker.C:
			r.server.clients.Range(func(key, value interface{}) bool {
				workerInfo, ok := value.(*WorkerNode)
				if !ok {
					log.Error().Msg("failed to get worker information")
				}
				go func() {
					if atomic.LoadInt32(&workerInfo.Heartbeat) < 0 {
						if workerInfo.getState() != StateMissing {
							workerInfo.setState(StateMissing)
						}
					} else {
						atomic.AddInt32(&workerInfo.Heartbeat, -1)
					}
				}()
				return true
			})
		case <-reportTicker.C:
			r.reportStats()
		}
	}
}

func (r *masterRunner) clientListener() {
	log.Info().Msg("clientListener, start to deal message from worker")
	for {
		select {
		case <-r.closeChan:
			return
		case msg := <-r.server.fromClient:
			worker, ok := r.server.getClients().Load(msg.NodeID)
			if !ok {
				r.server.clients.Store(msg.NodeID, newWorkerNode(msg.NodeID))
				continue
			}
			workerInfo, ok := worker.(*WorkerNode)
			if !ok {
				continue
			}
			go func() {
				switch msg.Type {
				case zeromq.ClientReady:
					workerInfo.setState(StateInit)
				case zeromq.ClientStopped:
					workerInfo.setState(StateStopped)
				case zeromq.Heartbeat:
					if workerInfo.getState() == StateMissing {
						workerInfo.setState(msg.Data["state"].(int32))
					}
					workerInfo.updateHeartbeat(3)
					currentCPUUsage, ok := msg.Data["current_cpu_usage"].(float64)
					if ok {
						workerInfo.updateCPUUsage(currentCPUUsage)
					}
					currentPidCpuUsage, ok := msg.Data["current_pid_cpu_usage"].(float64)
					if ok {
						workerInfo.updateWorkerCPUUsage(currentPidCpuUsage)
					}
					currentMemoryUsage, ok := msg.Data["current_memory_usage"].(float64)
					if ok {
						workerInfo.updateMemoryUsage(currentMemoryUsage)
					}
					currentPidMemoryUsage, ok := msg.Data["current_pid_memory_usage"].(float64)
					if ok {
						workerInfo.updateWorkerMemoryUsage(currentPidMemoryUsage)
					}
					currentUsers, ok := msg.Data["current_users"].(int64)
					if ok {
						workerInfo.updateUserCount(currentUsers)
					}
				case zeromq.Spawning:
					workerInfo.setState(StateSpawning)
				case zeromq.SpawningComplete:
					workerInfo.setState(StateRunning)
				case zeromq.Quit:
					if workerInfo.getState() == StateQuitting {
						break
					}
					workerInfo.setState(StateQuitting)
				case zeromq.Exception:
					// Todo
				default:
				}
			}()
		}
	}
}

func (r *masterRunner) stateMachine() {
	ticker := time.NewTicker(stateMachineInterval)
	for {
		select {
		case <-r.closeChan:
			return
		case <-ticker.C:
			switch r.getState() {
			case StateSpawning:
				if r.server.getCurrentUsers() == int(r.getSpawnCount()) {
					log.Warn().Msg("all workers spawn done, setting state as running")
					r.updateState(StateRunning)
				}
			case StateRunning:
				if r.server.getStartingClientsLength() == 0 {
					r.updateState(StateStopped)
					continue
				}
				if r.server.getWorkersLengthByState(StateInit) != 0 {
					err := r.rebalance()
					if err != nil {
						log.Error().Err(err).Msg("failed to rebalance")
					}
				}
			case StateStopping:
				if r.server.getReadyClientsLength() == r.server.getAvailableClientsLength() {
					r.updateState(StateStopped)
				}
			default:
				log.Info().Msgf("unknown runner state: %d", r.getState())
			}

		}
	}
}

func (r *masterRunner) run() {
	r.updateState(StateInit)

	// start mq server
	r.server.Start()
	defer func() {
		// close server
		r.server.Close()
	}()

	// master state machine
	r.goAttach(r.stateMachine)

	// listen and deal message from worker
	r.goAttach(r.clientListener)

	// listen and record heartbeat from worker
	r.heartbeatWorker()
	<-r.closeChan
}

func (r *masterRunner) start() error {
	numWorkers := r.server.getAvailableClientsLength()
	if numWorkers == 0 {
		return errors.New("current available workers: 0")
	}

	workerProfile := &Profile{}
	if err := copier.Copy(workerProfile, r.profile); err != nil {
		log.Error().Err(err).Msg("copy workerProfile failed")
		return err
	}

	// spawn count
	spawnCounts := SplitInteger(int(r.profile.SpawnCount), numWorkers)

	// spawn rate
	spawnRate := workerProfile.SpawnRate / float64(numWorkers)
	if spawnRate < 1 {
		spawnRate = 1
	}

	r.updateState(StateSpawning)
	log.Info().Msg("send spawn data to worker")

	cur := 0
	r.server.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.getState() == StateQuitting || workerInfo.getState() == StateMissing {
				return true
			}

			data := make(map[string]interface{})
			data["spawn_rate"] = spawnRate
			data["num_users"] = int64(spawnCounts[cur])

			r.server.send(&zeromq.Message{
				Type:   "spawn",
				Data:   data,
				NodeID: workerInfo.ID,
			})
			cur++
		}
		return true
	})

	log.Warn().Interface("profile", r.profile).Msg("send spawn data to worker successfully")
	return nil
}

func (r *masterRunner) rebalance() error {
	numWorkers := r.server.getAvailableClientsLength()
	if numWorkers == 0 {
		return errors.New("current available workers: 0")
	}
	workerProfile := &Profile{}
	if err := copier.Copy(workerProfile, r.profile); err != nil {
		log.Error().Err(err).Msg("copy workerProfile failed")
		return err
	}

	// spawn count
	spawnCounts := SplitInteger(int(r.profile.SpawnCount), numWorkers)

	// spawn rate
	spawnRate := workerProfile.SpawnRate / float64(numWorkers)
	if spawnRate < 1 {
		spawnRate = 1
	}

	cur := 0
	log.Info().Msg("send spawn data to worker")
	r.server.clients.Range(func(key, value interface{}) bool {
		if workerInfo, ok := value.(*WorkerNode); ok {
			if workerInfo.getState() == StateQuitting || workerInfo.getState() == StateMissing {
				return true
			}

			data := make(map[string]interface{})
			data["spawn_rate"] = spawnRate
			data["num_users"] = int64(spawnCounts[cur])

			if workerInfo.getState() == StateInit {
				r.server.send(&zeromq.Message{
					Type:   "spawn",
					Data:   data,
					NodeID: workerInfo.ID,
				})
			} else {
				r.server.send(&zeromq.Message{
					Type:   "rebalance",
					Data:   data,
					NodeID: workerInfo.ID,
				})
			}
		}
		cur++
		return true
	})

	log.Warn().Msg("send rebalance data to worker successfully")
	return nil
}

func (r *masterRunner) fetchTestCases() ([]byte, error) {
	ticker := time.NewTicker(30 * time.Second)
	if len(r.testCaseBytesChan) > 0 {
		<-r.testCaseBytesChan
	}
	r.parseTestCasesChan <- true
	select {
	case <-ticker.C:
		return nil, errors.New("parse testcases timeout")
	case testCasesBytes := <-r.testCaseBytesChan:
		r.testCasesBytes = testCasesBytes
		return testCasesBytes, nil
	}
}

func (r *masterRunner) stop() error {
	if r.isStarting() {
		r.updateState(StateStopping)
		r.server.sendBroadcasts(&zeromq.Message{Type: "stop"})
		return nil
	} else {
		return errors.New("already stopped")
	}
}

func (r *masterRunner) onQuiting() {
	if r.getState() != StateQuitting {
		r.server.sendBroadcasts(&zeromq.Message{
			Type: "quit",
		})
	}
	r.updateState(StateQuitting)
}

func (r *masterRunner) close() {
	r.onQuiting()
	close(r.closeChan)
}

func (r *masterRunner) reportStats() {
	currentTime := time.Now()
	println()
	println("==================================== Master for Distributed Load Testing ==================================== ")
	println(fmt.Sprintf("Current time: %s, State: %v, Current Available Workers: %v, Target Users: %v, Current Users: %v",
		currentTime.Format("2006/01/02 15:04:05"), getStateName(r.getState()), r.server.getAvailableClientsLength(), r.getSpawnCount(), r.server.getCurrentUsers()))
	table := tablewriter.NewWriter(os.Stdout)
	table.SetColMinWidth(0, 40)
	table.SetColMinWidth(1, 10)
	table.SetColMinWidth(2, 10)
	table.SetHeader([]string{"Worker ID", "IP", "State", "Current Users", "CPU Usage (%)", "Memory Usage (%)"})

	for _, worker := range r.server.getAllWorkers() {
		row := make([]string, 6)
		row[0] = worker.ID
		row[1] = worker.IP
		row[2] = fmt.Sprintf("%v", getStateName(worker.State))
		row[3] = fmt.Sprintf("%v", worker.UserCount)
		row[4] = fmt.Sprintf("%.2f", worker.CPUUsage)
		row[5] = fmt.Sprintf("%.2f", worker.MemoryUsage)
		table.Append(row)
	}
	table.Render()
	println()
}
