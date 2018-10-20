package hystrix

import (
	"sync"
	"time"

	"github.com/afex/hystrix-go/hystrix/metric_collector"
	"github.com/afex/hystrix-go/hystrix/rolling"
)

type commandExecution struct {
	Types            []string      `json:"types"`
	Start            time.Time     `json:"start_time"`
	RunDuration      time.Duration `json:"run_duration"`
	ConcurrencyInUse float64       `json:"concurrency_inuse"`
}

type metricExchange struct {
	Name    string
	Updates chan *commandExecution
	Mutex   *sync.RWMutex

	metricCollectors []metricCollector.MetricCollector
}

func newMetricExchange(name string) *metricExchange {
	m := &metricExchange{}
	m.Name = name

	m.Updates = make(chan *commandExecution, 2000)
	m.Mutex = &sync.RWMutex{}
	m.metricCollectors = metricCollector.Registry.InitializeMetricCollectors(name)
	m.Reset()

	go m.Monitor()

	return m
}

// The Default Collector function will panic if collectors are not setup to specification.
func (m *metricExchange) DefaultCollector() *metricCollector.DefaultMetricCollector {
	if len(m.metricCollectors) < 1 {
		panic("No Metric Collectors Registered.")
	}
	collection, ok := m.metricCollectors[0].(*metricCollector.DefaultMetricCollector)
	if !ok {
		panic("Default metric collector is not registered correctly. The default metric collector must be registered first.")
	}
	return collection
}

func (m *metricExchange) Monitor() {
	for update := range m.Updates {
		// we only grab a read lock to make sure Reset() isn't changing the numbers.
		m.Mutex.RLock()

		totalDuration := time.Since(update.Start)
		wg := &sync.WaitGroup{}
		for _, collector := range m.metricCollectors {
			wg.Add(1)
			go m.IncrementMetrics(wg, collector, update, totalDuration)
		}
		wg.Wait()

		m.Mutex.RUnlock()
	}
}

func (m *metricExchange) IncrementMetrics(wg *sync.WaitGroup, collector metricCollector.MetricCollector, update *commandExecution, totalDuration time.Duration) {
	// granular metrics
	var cmdMetric *metricCollector.StreamCmdMetric
	var poolMetric *metricCollector.StreamThreadPoolMetric

	circuitBreakersMutex.RLock()
	for _, cb := range circuitBreakers {
		cmdMetric = BuildStreamCmdMetric(cb)
		poolMetric = BuildStreamThreadPoolMetric(cb.executorPool)
	}
	circuitBreakersMutex.RUnlock()

	r := metricCollector.MetricResult{
		Attempts:         1,
		TotalDuration:    totalDuration,
		RunDuration:      update.RunDuration,
		ConcurrencyInUse: update.ConcurrencyInUse,
		CMDMetric: cmdMetric,
		ThreadPoolMetric: poolMetric,
	}

	switch update.Types[0] {
	case "success":
		r.Successes = 1
	case "failure":
		r.Failures = 1
		r.Errors = 1
	case "rejected":
		r.Rejects = 1
		r.Errors = 1
	case "short-circuit":
		r.ShortCircuits = 1
		r.Errors = 1
	case "timeout":
		r.Timeouts = 1
		r.Errors = 1
	case "context_canceled":
		r.ContextCanceled = 1
	case "context_deadline_exceeded":
		r.ContextDeadlineExceeded = 1
	}

	if len(update.Types) > 1 {
		// fallback metrics
		if update.Types[1] == "fallback-success" {
			r.FallbackSuccesses = 1
		}
		if update.Types[1] == "fallback-failure" {
			r.FallbackFailures = 1
		}
	}

	collector.Update(r)

	wg.Done()
}

func (m *metricExchange) Reset() {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	for _, collector := range m.metricCollectors {
		collector.Reset()
	}
}

func (m *metricExchange) Requests() *rolling.Number {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()
	return m.requestsLocked()
}

func (m *metricExchange) requestsLocked() *rolling.Number {
	return m.DefaultCollector().NumRequests()
}

func (m *metricExchange) ErrorPercent(now time.Time) int {
	m.Mutex.RLock()
	defer m.Mutex.RUnlock()

	var errPct float64
	reqs := m.requestsLocked().Sum(now)
	errs := m.DefaultCollector().Errors().Sum(now)

	if reqs > 0 {
		errPct = (float64(errs) / float64(reqs)) * 100
	}

	return int(errPct + 0.5)
}

func (m *metricExchange) IsHealthy(now time.Time) bool {
	return m.ErrorPercent(now) < getSettings(m.Name).ErrorPercentThreshold
}

func BuildStreamCmdMetric(cb *CircuitBreaker) *metricCollector.StreamCmdMetric {
	now := time.Now()
	reqCount := cb.metrics.Requests().Sum(now)
	errCount := cb.metrics.DefaultCollector().Errors().Sum(now)
	errPct := cb.metrics.ErrorPercent(now)
	metric := &metricCollector.StreamCmdMetric{
		Type:           "HystrixCommand",
		Name:           cb.Name,
		Group:          cb.Name,
		Time:           currentTime(),
		ReportingHosts: 1,

		RequestCount:       uint32(reqCount),
		ErrorCount:         uint32(errCount),
		ErrorPct:           uint32(errPct),
		CircuitBreakerOpen: cb.IsOpen(),

		RollingCountSuccess:            uint32(cb.metrics.DefaultCollector().Successes().Sum(now)),
		RollingCountFailure:            uint32(cb.metrics.DefaultCollector().Failures().Sum(now)),
		RollingCountThreadPoolRejected: uint32(cb.metrics.DefaultCollector().Rejects().Sum(now)),
		RollingCountShortCircuited:     uint32(cb.metrics.DefaultCollector().ShortCircuits().Sum(now)),
		RollingCountTimeout:            uint32(cb.metrics.DefaultCollector().Timeouts().Sum(now)),
		RollingCountFallbackSuccess:    uint32(cb.metrics.DefaultCollector().FallbackSuccesses().Sum(now)),
		RollingCountFallbackFailure:    uint32(cb.metrics.DefaultCollector().FallbackFailures().Sum(now)),

		LatencyTotal:       generateLatencyTimings(cb.metrics.DefaultCollector().TotalDuration()),
		LatencyTotalMean:   cb.metrics.DefaultCollector().TotalDuration().Mean(),
		LatencyExecute:     generateLatencyTimings(cb.metrics.DefaultCollector().RunDuration()),
		LatencyExecuteMean: cb.metrics.DefaultCollector().RunDuration().Mean(),

		// TODO: all hard-coded values should become configurable settings, per circuit

		RollingStatsWindow:         10000,
		ExecutionIsolationStrategy: "THREAD",

		CircuitBreakerEnabled:                true,
		CircuitBreakerForceClosed:            false,
		CircuitBreakerForceOpen:              cb.forceOpen,
		CircuitBreakerErrorThresholdPercent:  uint32(getSettings(cb.Name).ErrorPercentThreshold),
		CircuitBreakerSleepWindow:            uint32(getSettings(cb.Name).SleepWindow.Seconds() * 1000),
		CircuitBreakerRequestVolumeThreshold: uint32(getSettings(cb.Name).RequestVolumeThreshold),
	}
	return metric
}

func BuildStreamThreadPoolMetric(pool *executorPool) *metricCollector.StreamThreadPoolMetric {
	now := time.Now()
	metric := &metricCollector.StreamThreadPoolMetric{
		Type:           "HystrixThreadPool",
		Name:           pool.Name,
		ReportingHosts: 1,

		CurrentActiveCount:        uint32(pool.ActiveCount()),
		CurrentTaskCount:          0,
		CurrentCompletedTaskCount: 0,

		RollingCountThreadsExecuted: uint32(pool.Metrics.Executed.Sum(now)),
		RollingMaxActiveThreads:     uint32(pool.Metrics.MaxActiveRequests.Max(now)),

		CurrentPoolSize:        uint32(pool.Max),
		CurrentCorePoolSize:    uint32(pool.Max),
		CurrentLargestPoolSize: uint32(pool.Max),
		CurrentMaximumPoolSize: uint32(pool.Max),

		RollingStatsWindow:          10000,
		QueueSizeRejectionThreshold: 0,
		CurrentQueueSize:            0,
	}
	return metric
}

