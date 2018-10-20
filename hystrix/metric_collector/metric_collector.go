package metricCollector

import (
	"sync"
	"time"
)

// Registry is the default metricCollectorRegistry that circuits will use to
// collect statistics about the health of the circuit.
var Registry = metricCollectorRegistry{
	lock: &sync.RWMutex{},
	registry: []func(name string) MetricCollector{
		newDefaultMetricCollector,
	},
}

type metricCollectorRegistry struct {
	lock     *sync.RWMutex
	registry []func(name string) MetricCollector
}

// InitializeMetricCollectors runs the registried MetricCollector Initializers to create an array of MetricCollectors.
func (m *metricCollectorRegistry) InitializeMetricCollectors(name string) []MetricCollector {
	m.lock.RLock()
	defer m.lock.RUnlock()

	metrics := make([]MetricCollector, len(m.registry))
	for i, metricCollectorInitializer := range m.registry {
		metrics[i] = metricCollectorInitializer(name)
	}
	return metrics
}

// Register places a MetricCollector Initializer in the registry maintained by this metricCollectorRegistry.
func (m *metricCollectorRegistry) Register(initMetricCollector func(string) MetricCollector) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.registry = append(m.registry, initMetricCollector)
}

type MetricResult struct {
	Attempts                float64
	Errors                  float64
	Successes               float64
	Failures                float64
	Rejects                 float64
	ShortCircuits           float64
	Timeouts                float64
	FallbackSuccesses       float64
	FallbackFailures        float64
	ContextCanceled         float64
	ContextDeadlineExceeded float64
	TotalDuration           time.Duration
	RunDuration             time.Duration
	ConcurrencyInUse        float64
	CMDMetric               *StreamCmdMetric
	ThreadPoolMetric 		*StreamThreadPoolMetric
}

type StreamCmdMetric struct {
	Type           string `json:"type"`
	Name           string `json:"name"`
	Group          string `json:"group"`
	Time           int64  `json:"currentTime"`
	ReportingHosts uint32 `json:"reportingHosts"`

	// Health
	RequestCount       uint32 `json:"requestCount"`
	ErrorCount         uint32 `json:"errorCount"`
	ErrorPct           uint32 `json:"errorPercentage"`
	CircuitBreakerOpen bool   `json:"isCircuitBreakerOpen"`

	RollingCountCollapsedRequests  uint32 `json:"rollingCountCollapsedRequests"`
	RollingCountExceptionsThrown   uint32 `json:"rollingCountExceptionsThrown"`
	RollingCountFailure            uint32 `json:"rollingCountFailure"`
	RollingCountFallbackFailure    uint32 `json:"rollingCountFallbackFailure"`
	RollingCountFallbackRejection  uint32 `json:"rollingCountFallbackRejection"`
	RollingCountFallbackSuccess    uint32 `json:"rollingCountFallbackSuccess"`
	RollingCountResponsesFromCache uint32 `json:"rollingCountResponsesFromCache"`
	RollingCountSemaphoreRejected  uint32 `json:"rollingCountSemaphoreRejected"`
	RollingCountShortCircuited     uint32 `json:"rollingCountShortCircuited"`
	RollingCountSuccess            uint32 `json:"rollingCountSuccess"`
	RollingCountThreadPoolRejected uint32 `json:"rollingCountThreadPoolRejected"`
	RollingCountTimeout            uint32 `json:"rollingCountTimeout"`

	CurrentConcurrentExecutionCount uint32 `json:"currentConcurrentExecutionCount"`

	LatencyExecuteMean uint32           `json:"latencyExecute_mean"`
	LatencyExecute     StreamCmdLatency `json:"latencyExecute"`
	LatencyTotalMean   uint32           `json:"latencyTotal_mean"`
	LatencyTotal       StreamCmdLatency `json:"latencyTotal"`

	// Properties
	CircuitBreakerRequestVolumeThreshold             uint32 `json:"propertyValue_circuitBreakerRequestVolumeThreshold"`
	CircuitBreakerSleepWindow                        uint32 `json:"propertyValue_circuitBreakerSleepWindowInMilliseconds"`
	CircuitBreakerErrorThresholdPercent              uint32 `json:"propertyValue_circuitBreakerErrorThresholdPercentage"`
	CircuitBreakerForceOpen                          bool   `json:"propertyValue_circuitBreakerForceOpen"`
	CircuitBreakerForceClosed                        bool   `json:"propertyValue_circuitBreakerForceClosed"`
	CircuitBreakerEnabled                            bool   `json:"propertyValue_circuitBreakerEnabled"`
	ExecutionIsolationStrategy                       string `json:"propertyValue_executionIsolationStrategy"`
	ExecutionIsolationThreadTimeout                  uint32 `json:"propertyValue_executionIsolationThreadTimeoutInMilliseconds"`
	ExecutionIsolationThreadInterruptOnTimeout       bool   `json:"propertyValue_executionIsolationThreadInterruptOnTimeout"`
	ExecutionIsolationThreadPoolKeyOverride          string `json:"propertyValue_executionIsolationThreadPoolKeyOverride"`
	ExecutionIsolationSemaphoreMaxConcurrentRequests uint32 `json:"propertyValue_executionIsolationSemaphoreMaxConcurrentRequests"`
	FallbackIsolationSemaphoreMaxConcurrentRequests  uint32 `json:"propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests"`
	RollingStatsWindow                               uint32 `json:"propertyValue_metricsRollingStatisticalWindowInMilliseconds"`
	RequestCacheEnabled                              bool   `json:"propertyValue_requestCacheEnabled"`
	RequestLogEnabled                                bool   `json:"propertyValue_requestLogEnabled"`
}

type StreamCmdLatency struct {
	Timing0   uint32 `json:"0"`
	Timing25  uint32 `json:"25"`
	Timing50  uint32 `json:"50"`
	Timing75  uint32 `json:"75"`
	Timing90  uint32 `json:"90"`
	Timing95  uint32 `json:"95"`
	Timing99  uint32 `json:"99"`
	Timing995 uint32 `json:"99.5"`
	Timing100 uint32 `json:"100"`
}

type StreamThreadPoolMetric struct {
	Type           string `json:"type"`
	Name           string `json:"name"`
	ReportingHosts uint32 `json:"reportingHosts"`

	CurrentActiveCount        uint32 `json:"currentActiveCount"`
	CurrentCompletedTaskCount uint32 `json:"currentCompletedTaskCount"`
	CurrentCorePoolSize       uint32 `json:"currentCorePoolSize"`
	CurrentLargestPoolSize    uint32 `json:"currentLargestPoolSize"`
	CurrentMaximumPoolSize    uint32 `json:"currentMaximumPoolSize"`
	CurrentPoolSize           uint32 `json:"currentPoolSize"`
	CurrentQueueSize          uint32 `json:"currentQueueSize"`
	CurrentTaskCount          uint32 `json:"currentTaskCount"`

	RollingMaxActiveThreads     uint32 `json:"rollingMaxActiveThreads"`
	RollingCountThreadsExecuted uint32 `json:"rollingCountThreadsExecuted"`

	RollingStatsWindow          uint32 `json:"propertyValue_metricsRollingStatisticalWindowInMilliseconds"`
	QueueSizeRejectionThreshold uint32 `json:"propertyValue_queueSizeRejectionThreshold"`
}


// MetricCollector represents the contract that all collectors must fulfill to gather circuit statistics.
// Implementations of this interface do not have to maintain locking around thier data stores so long as
// they are not modified outside of the hystrix context.
type MetricCollector interface {
	// Update accepts a set of metrics from a command execution for remote instrumentation
	Update(MetricResult)
	// Reset resets the internal counters and timers.
	Reset()
}
