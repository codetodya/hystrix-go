package hystrix

import (
	"bytes"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/afex/hystrix-go/hystrix/rolling"
	"github.com/afex/hystrix-go/hystrix/metric_collector"
)

const (
	streamEventBufferSize = 10
)

// NewStreamHandler returns a server capable of exposing dashboard metrics via HTTP.
func NewStreamHandler() *StreamHandler {
	return &StreamHandler{}
}

// StreamHandler publishes metrics for each command and each pool once a second to all connected HTTP client.
type StreamHandler struct {
	requests map[*http.Request]chan []byte
	mu       sync.RWMutex
	done     chan struct{}
}

// Start begins watching the in-memory circuit breakers for metrics
func (sh *StreamHandler) Start() {
	sh.requests = make(map[*http.Request]chan []byte)
	sh.done = make(chan struct{})
	go sh.loop()
}

// Stop shuts down the metric collection routine
func (sh *StreamHandler) Stop() {
	close(sh.done)
}

var _ http.Handler = (*StreamHandler)(nil)

func (sh *StreamHandler) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	// Make sure that the writer supports flushing.
	f, ok := rw.(http.Flusher)
	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}
	events := sh.register(req)
	defer sh.unregister(req)

	notify := rw.(http.CloseNotifier).CloseNotify()

	rw.Header().Add("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	for {
		select {
		case <-notify:
			// client is gone
			return
		case event := <-events:
			_, err := rw.Write(event)
			if err != nil {
				return
			}
			f.Flush()
		}
	}
}

func (sh *StreamHandler) loop() {
	tick := time.Tick(1 * time.Second)
	for {
		select {
		case <-tick:
			circuitBreakersMutex.RLock()
			for _, cb := range circuitBreakers {
				sh.publishMetrics(cb)
				sh.publishThreadPools(cb.executorPool)
			}
			circuitBreakersMutex.RUnlock()
		case <-sh.done:
			return
		}
	}
}

func (sh *StreamHandler) publishMetrics(cb *CircuitBreaker) error {
	eventBytes, err := json.Marshal(BuildStreamCmdMetric(cb))
	if err != nil {
		return err
	}
	err = sh.writeToRequests(eventBytes)
	if err != nil {
		return err
	}

	return nil
}

func (sh *StreamHandler) publishThreadPools(pool *executorPool) error {
	eventBytes, err := json.Marshal(BuildStreamThreadPoolMetric(pool))
	if err != nil {
		return err
	}
	err = sh.writeToRequests(eventBytes)

	return nil
}

func (sh *StreamHandler) writeToRequests(eventBytes []byte) error {
	var b bytes.Buffer
	_, err := b.Write([]byte("data:"))
	if err != nil {
		return err
	}

	_, err = b.Write(eventBytes)
	if err != nil {
		return err
	}
	_, err = b.Write([]byte("\n\n"))
	if err != nil {
		return err
	}
	dataBytes := b.Bytes()
	sh.mu.RLock()

	for _, requestEvents := range sh.requests {
		select {
		case requestEvents <- dataBytes:
		default:
		}
	}
	sh.mu.RUnlock()

	return nil
}

func (sh *StreamHandler) register(req *http.Request) <-chan []byte {
	sh.mu.RLock()
	events, ok := sh.requests[req]
	sh.mu.RUnlock()
	if ok {
		return events
	}

	events = make(chan []byte, streamEventBufferSize)
	sh.mu.Lock()
	sh.requests[req] = events
	sh.mu.Unlock()
	return events
}

func (sh *StreamHandler) unregister(req *http.Request) {
	sh.mu.Lock()
	delete(sh.requests, req)
	sh.mu.Unlock()
}

func generateLatencyTimings(r *rolling.Timing) metricCollector.StreamCmdLatency {
	return metricCollector.StreamCmdLatency{
		Timing0:   r.Percentile(0),
		Timing25:  r.Percentile(25),
		Timing50:  r.Percentile(50),
		Timing75:  r.Percentile(75),
		Timing90:  r.Percentile(90),
		Timing95:  r.Percentile(95),
		Timing99:  r.Percentile(99),
		Timing995: r.Percentile(99.5),
		Timing100: r.Percentile(100),
	}
}

func currentTime() int64 {
	return time.Now().UnixNano() / int64(1000000)
}
