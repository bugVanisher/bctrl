package bctrl

import (
	"time"
)

var gatherStats = newRequestStats()

type requestSuccess struct {
	requestType    string
	name           string
	responseTime   int64
	responseLength int64
}

type requestFailure struct {
	requestType  string
	name         string
	responseTime int64
	error        string
}

type requestStats struct {
	entries   map[string]*statsEntry
	errors    map[string]*statsError
	total     *statsEntry
	startTime int64

	requestSuccessChan  chan *requestSuccess
	requestFailureChan  chan *requestFailure
	clearStatsChan      chan bool
	messageToRunnerChan chan map[string]interface{}
	shutdownChan        chan bool
}

func newRequestStats() (stats *requestStats) {
	entries := make(map[string]*statsEntry)
	errors := make(map[string]*statsError)

	stats = &requestStats{
		entries: entries,
		errors:  errors,
	}
	stats.requestSuccessChan = make(chan *requestSuccess, 100)
	stats.requestFailureChan = make(chan *requestFailure, 100)
	stats.clearStatsChan = make(chan bool)
	stats.messageToRunnerChan = make(chan map[string]interface{}, 10)
	stats.shutdownChan = make(chan bool)

	stats.total = &statsEntry{
		name:   "Total",
		method: "",
	}
	stats.total.reset()

	return stats
}

func (s *requestStats) get(name string, method string) (entry *statsEntry) {
	entry, ok := s.entries[name+method]
	if !ok {
		newEntry := &statsEntry{
			name:          name,
			method:        method,
			numReqsPerSec: make(map[int64]int64),
			responseTimes: make(map[int64]int64),
		}
		newEntry.reset()
		s.entries[name+method] = newEntry
		return newEntry
	}
	return entry
}

func (s *requestStats) clearAll() {
	s.total = &statsEntry{
		name:   "Total",
		method: "",
	}
	s.total.reset()

	s.entries = make(map[string]*statsEntry)
	s.errors = make(map[string]*statsError)
	s.startTime = time.Now().Unix()
}

func (s *requestStats) serializeStats() []interface{} {
	entries := make([]interface{}, 0, len(s.entries))
	for _, v := range s.entries {
		if !(v.numRequests == 0 && v.numFailures == 0) {
			entries = append(entries, v.getStrippedReport())
		}
	}
	return entries
}

func (s *requestStats) serializeErrors() map[string]map[string]interface{} {
	errors := make(map[string]map[string]interface{})
	for k, v := range s.errors {
		errors[k] = v.toMap()
	}
	return errors
}

// close is used by unit tests to avoid leakage of goroutines
func (s *requestStats) close() {
	close(s.shutdownChan)
}

type statsEntry struct {
	name                 string
	method               string
	numRequests          int64
	numFailures          int64
	totalResponseTime    int64
	minResponseTime      int64
	maxResponseTime      int64
	numReqsPerSec        map[int64]int64
	numFailPerSec        map[int64]int64
	responseTimes        map[int64]int64
	totalContentLength   int64
	startTime            int64
	lastRequestTimestamp int64
}

func (s *statsEntry) reset() {
	s.startTime = time.Now().Unix()
	s.numRequests = 0
	s.numFailures = 0
	s.totalResponseTime = 0
	s.responseTimes = make(map[int64]int64)
	s.minResponseTime = 0
	s.maxResponseTime = 0
	s.lastRequestTimestamp = time.Now().Unix()
	s.numReqsPerSec = make(map[int64]int64)
	s.numFailPerSec = make(map[int64]int64)
	s.totalContentLength = 0
}

func (s *statsEntry) deserialize(data map[string]interface{}) {
	s.name = data["name"].(string)
	s.method = data["method"].(string)
	s.lastRequestTimestamp = data["last_request_timestamp"].(int64)
	s.startTime = data["start_time"].(int64)
	s.numRequests = data["num_requests"].(int64)
	s.numFailures = data["num_failures"].(int64)
	s.totalResponseTime = data["total_response_time"].(int64)
	s.maxResponseTime = data["max_response_time"].(int64)
	s.minResponseTime = data["min_response_time"].(int64)
	s.totalContentLength = data["total_content_length"].(int64)
	s.responseTimes = data["response_times"].(map[int64]int64)
	s.numReqsPerSec = data["num_reqs_per_sec"].(map[int64]int64)
	s.numFailPerSec = data["num_fail_per_sec"].(map[int64]int64)
}

func (s *statsEntry) serialize() map[string]interface{} {
	result := make(map[string]interface{})
	result["name"] = s.name
	result["method"] = s.method
	result["last_request_timestamp"] = s.lastRequestTimestamp
	result["start_time"] = s.startTime
	result["num_requests"] = s.numRequests
	// Boomer doesn't allow None response time for requests like locust.
	// num_none_requests is added to keep compatible with locust.
	result["num_none_requests"] = 0
	result["num_failures"] = s.numFailures
	result["total_response_time"] = s.totalResponseTime
	result["max_response_time"] = s.maxResponseTime
	result["min_response_time"] = s.minResponseTime
	result["total_content_length"] = s.totalContentLength
	result["response_times"] = s.responseTimes
	result["num_reqs_per_sec"] = s.numReqsPerSec
	result["num_fail_per_sec"] = s.numFailPerSec
	return result
}

func (s *statsEntry) getStrippedReport() map[string]interface{} {
	report := s.serialize()
	s.reset()
	return report
}

func (s *statsEntry) extend(other *statsEntry) {
	if s.lastRequestTimestamp > 0 && other.lastRequestTimestamp > 0 {
		s.lastRequestTimestamp = Max(s.lastRequestTimestamp, other.lastRequestTimestamp)
	} else if other.lastRequestTimestamp > 0 {
		s.lastRequestTimestamp = other.lastRequestTimestamp
	}
	s.startTime = Min(s.startTime, other.startTime)
	s.numRequests += other.numRequests
	s.numFailures += other.numFailures
	s.totalResponseTime += other.totalResponseTime
	s.maxResponseTime = Max(s.maxResponseTime, other.maxResponseTime)
	s.minResponseTime = Min(s.minResponseTime, other.minResponseTime)
	s.totalContentLength += other.totalContentLength

	for key := range other.responseTimes {
		s.responseTimes[key] = s.responseTimes[key] + other.responseTimes[key]
	}

	for key := range other.numReqsPerSec {
		s.numReqsPerSec[key] = s.numReqsPerSec[key] + other.numReqsPerSec[key]
	}

	for key := range other.numFailPerSec {
		s.numFailPerSec[key] = s.numFailPerSec[key] + other.numFailPerSec[key]
	}
}

type statsError struct {
	name        string
	method      string
	error       string
	occurrences int64
}

func (err *statsError) occured() {
	err.occurrences++
}

func (err *statsError) toMap() map[string]interface{} {
	m := make(map[string]interface{})
	m["method"] = err.method
	m["name"] = err.name
	m["error"] = err.error
	m["occurrences"] = err.occurrences
	return m
}

func (err *statsError) deserialize(e map[string]interface{}) {
	err.name = e["name"].(string)
	err.method = e["method"].(string)
	err.error = e["error"].(string)
	err.occurrences = e["occurrences"].(int64)
}

func onWorkerReport(data map[string]interface{}) {
	for _, statsData := range data["stats"].([]interface{}) {
		entry := &statsEntry{}
		entry.deserialize(statsData.(map[string]interface{}))
		requestKey := entry.name + entry.method
		if _, ok := gatherStats.entries[requestKey]; ok {
			gatherStats.entries[requestKey] = gatherStats.get(entry.name, entry.method)
		}
		gatherStats.entries[requestKey].extend(entry)
	}

	for key, err := range data["errors"].(map[string]map[string]interface{}) {
		e := &statsError{}
		e.deserialize(err)
		if _, ok := gatherStats.errors[key]; !ok {
			gatherStats.errors[key] = e
		} else {
			gatherStats.errors[key].occurrences += e.occurrences
		}
	}
	total := &statsEntry{}
	total.deserialize(data["stats_total"].(map[string]interface{}))
	gatherStats.total.extend(total)

}
