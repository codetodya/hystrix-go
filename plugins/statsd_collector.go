package plugins

import (
	"log"
	"strings"
	"time"

	"github.com/afex/hystrix-go/hystrix/metric_collector"
	"github.com/cactus/go-statsd-client/statsd"
)

// StatsdCollector fulfills the metricCollector interface allowing users to ship circuit
// stats to a Statsd backend. To use users must call InitializeStatsdCollector before
// circuits are started. Then register NewStatsdCollector with metricCollector.Registry.Register(NewStatsdCollector).
//
// This Collector uses https://github.com/cactus/go-statsd-client/ for transport.
type StatsdCollector struct {
	prefix                  string
	client                  statsd.Statter
	circuitOpenPrefix       string
	attemptsPrefix          string
	errorsPrefix            string
	successesPrefix         string
	failuresPrefix          string
	rejectsPrefix           string
	shortCircuitsPrefix     string
	timeoutsPrefix          string
	fallbackSuccessesPrefix string
	fallbackFailuresPrefix  string
	canceledPrefix          string
	deadlinePrefix          string
	totalDurationPrefix     string
	runDurationPrefix       string
	concurrencyInUsePrefix  string
	sampleRate              float32
}

type StatsdCollectorClient struct {
	client     statsd.Statter
	sampleRate float32
}

// https://github.com/etsy/statsd/blob/master/docs/metric_types.md#multi-metric-packets
const (
	WANStatsdFlushBytes     = 512
	LANStatsdFlushBytes     = 1432
	GigabitStatsdFlushBytes = 8932
)

// StatsdCollectorConfig provides configuration that the Statsd client will need.
type StatsdCollectorConfig struct {
	// StatsdAddr is the tcp address of the Statsd server
	StatsdAddr string
	// Prefix is the prefix that will be prepended to all metrics sent from this collector.
	Prefix string
	// StatsdSampleRate sets statsd sampling. If 0, defaults to 1.0. (no sampling)
	SampleRate float32
	// FlushBytes sets message size for statsd packets. If 0, defaults to LANFlushSize.
	FlushBytes int
}

// InitializeStatsdCollector creates the connection to the Statsd server
// and should be called before any metrics are recorded.
//
// Users should ensure to call Close() on the client.
func InitializeStatsdCollector(config *StatsdCollectorConfig) (*StatsdCollectorClient, error) {
	flushBytes := config.FlushBytes
	if flushBytes == 0 {
		flushBytes = LANStatsdFlushBytes
	}

	sampleRate := config.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}

	c, err := statsd.NewBufferedClient(config.StatsdAddr, config.Prefix, 1*time.Second, flushBytes)
	if err != nil {
		log.Printf("Could not initiale buffered client: %s. Falling back to a Noop Statsd client", err)
		c, _ = statsd.NewNoopClient()
	}
	return &StatsdCollectorClient{
		client:     c,
		sampleRate: sampleRate,
	}, err
}

// NewStatsdCollector creates a collector for a specific circuit. The
// prefix given to this circuit will be {config.Prefix}.{circuit_name}.{metric}.
// Circuits with "/" in their names will have them replaced with ".".
func (s *StatsdCollectorClient) NewStatsdCollector(name string) metricCollector.MetricCollector {
	if s.client == nil {
		log.Fatalf("Statsd client must be initialized before circuits are created.")
	}
	name = strings.Replace(name, "/", "-", -1)
	name = strings.Replace(name, ":", "-", -1)
	name = strings.Replace(name, ".", "-", -1)
	return &StatsdCollector{
		prefix:                  name,
		client:                  s.client,
		circuitOpenPrefix:       name + ".circuitOpen",
		attemptsPrefix:          name + ".attempts",
		errorsPrefix:            name + ".errors",
		successesPrefix:         name + ".successes",
		failuresPrefix:          name + ".failures",
		rejectsPrefix:           name + ".rejects",
		shortCircuitsPrefix:     name + ".shortCircuits",
		timeoutsPrefix:          name + ".timeouts",
		fallbackSuccessesPrefix: name + ".fallbackSuccesses",
		fallbackFailuresPrefix:  name + ".fallbackFailures",
		canceledPrefix:          name + ".contextCanceled",
		deadlinePrefix:          name + ".contextDeadlineExceeded",
		totalDurationPrefix:     name + ".totalDuration",
		runDurationPrefix:       name + ".runDuration",
		concurrencyInUsePrefix:  name + ".concurrencyInUse",
		sampleRate:              s.sampleRate,
	}
}

func (g *StatsdCollector) setGauge(prefix string, value int64) {
	err := g.client.Gauge(prefix, value, g.sampleRate)
	if err != nil {
		log.Printf("Error sending statsd metrics %s", prefix)
	}
}

func (g *StatsdCollector) incrementCounterMetric(prefix string, i float64) {
	if i == 0 {
		return
	}
	err := g.client.Inc(prefix, int64(i), g.sampleRate)
	if err != nil {
		log.Printf("Error sending statsd metrics %s", prefix)
	}
}

func (g *StatsdCollector) updateTimerMetric(prefix string, dur time.Duration) {
	err := g.client.TimingDuration(prefix, dur, g.sampleRate)
	if err != nil {
		log.Printf("Error sending statsd metrics %s", prefix)
	}
}

func (g *StatsdCollector) updateTimingMetric(prefix string, i int64) {
	err := g.client.Timing(prefix, i, g.sampleRate)
	if err != nil {
		log.Printf("Error sending statsd metrics %s", prefix)
	}
}

func (g *StatsdCollector) Update(r metricCollector.MetricResult) {
	g.updateOldMetrics(r)
	g.updateHystrixThreadPoolMetrics(r)
	g.updateHystrixCMDPoolMetrics(r)
}

func (g *StatsdCollector) updateOldMetrics(r metricCollector.MetricResult) {
	if r.Successes > 0 {
		g.setGauge(g.circuitOpenPrefix, 0)
	} else if r.ShortCircuits > 0 {
		g.setGauge(g.circuitOpenPrefix, 1)
	}
	g.incrementCounterMetric(g.attemptsPrefix, r.Attempts)
	g.incrementCounterMetric(g.errorsPrefix, r.Errors)
	g.incrementCounterMetric(g.successesPrefix, r.Successes)
	g.incrementCounterMetric(g.failuresPrefix, r.Failures)
	g.incrementCounterMetric(g.rejectsPrefix, r.Rejects)
	g.incrementCounterMetric(g.shortCircuitsPrefix, r.ShortCircuits)
	g.incrementCounterMetric(g.timeoutsPrefix, r.Timeouts)
	g.incrementCounterMetric(g.fallbackSuccessesPrefix, r.FallbackSuccesses)
	g.incrementCounterMetric(g.fallbackFailuresPrefix, r.FallbackFailures)
	g.incrementCounterMetric(g.canceledPrefix, r.ContextCanceled)
	g.incrementCounterMetric(g.deadlinePrefix, r.ContextDeadlineExceeded)
	g.updateTimerMetric(g.totalDurationPrefix, r.TotalDuration)
	g.updateTimerMetric(g.runDurationPrefix, r.RunDuration)
	g.updateTimingMetric(g.concurrencyInUsePrefix, int64(100*r.ConcurrencyInUse))
}

func (g *StatsdCollector) updateHystrixThreadPoolMetrics(r metricCollector.MetricResult) {
	g.setGauge(g.createHystrixThreadPoolMetricName("threadActiveCount"), int64(r.ThreadPoolMetric.CurrentActiveCount))
	g.setGauge(g.createHystrixThreadPoolMetricName("completedTaskCount"), int64(r.ThreadPoolMetric.CurrentCompletedTaskCount))
	g.setGauge(g.createHystrixThreadPoolMetricName("largestPoolSize"), int64(r.ThreadPoolMetric.CurrentLargestPoolSize))
	g.setGauge(g.createHystrixThreadPoolMetricName("totalTaskCount"), int64(r.ThreadPoolMetric.CurrentTaskCount))
	g.setGauge(g.createHystrixThreadPoolMetricName("queueSize"), int64(r.ThreadPoolMetric.CurrentQueueSize))
	g.setGauge(g.createHystrixThreadPoolMetricName("rollingMaxActiveThreads"), int64(r.ThreadPoolMetric.RollingMaxActiveThreads))
	g.setGauge(g.createHystrixThreadPoolMetricName("rollingCountThreadsExecuted"), int64(r.ThreadPoolMetric.RollingCountThreadsExecuted))
	g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_corePoolSize"), int64(r.ThreadPoolMetric.CurrentCorePoolSize))
	g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_maximumSize"), int64(r.ThreadPoolMetric.CurrentMaximumPoolSize))
	g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_queueSizeRejectionThreshold"), int64(r.ThreadPoolMetric.QueueSizeRejectionThreshold))
	//g.setGauge(g.createHystrixThreadPoolMetricName("rollingCountCommandsRejected"), int64(r.CMDMetric.RollingCountThreadPoolRejected))
	//g.setGauge(g.createHystrixThreadPoolMetricName("prefix"), r.ThreadPoolMetric.Name)
	//g.setGauge(g.createHystrixThreadPoolMetricName("countThreadsExecuted"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_actualMaximumSize"), int64(r.ThreadPoolMetric))
	//g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_keepAliveTimeInMinutes"), int64(r.ThreadPoolMetric))
	//g.setGauge(g.createHystrixThreadPoolMetricName("propertyValue_maxQueueSize"), int64(r.ThreadPoolMetric.que))
}

func (g *StatsdCollector) createHystrixThreadPoolMetricName(name string) string {
	return g.prefix +".HystrixThreadPool."+ name
}

func (g *StatsdCollector) createHystrixCMDMetricName(name string) string {
	return g.prefix +".HystrixCMD."+ name
}

// Reset is a noop operation in this collector.
func (g *StatsdCollector) Reset() {}

func (g *StatsdCollector) updateHystrixCMDPoolMetrics(r metricCollector.MetricResult) {
	//g.setGauge(g.createHystrixCMDMetricName("isCircuitBreakerOpen"), circuitOpen)

	g.setGauge(g.createHystrixCMDMetricName("countBadRequests"), int64(r.CMDMetric.ErrorCount))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackEmit"), int64(r.CMDMetric.RollingCountFallbackFailure))
	//g.setGauge(g.createHystrixCMDMetricName("countEmit"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("countExceptionsThrown"), int64(r.CMDMetric.RollingCountExceptionsThrown))
	//g.setGauge(g.createHystrixCMDMetricName("countFailure"), int64(r.CMDMetric.RollingCountFailure))
	////g.setGauge(g.createHystrixCMDMetricName("countFallbackEmit"), int64(r.CMDMetric.RollingCountFallbackFailure))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackFailure"), int64(r.CMDMetric.RollingCountFallbackFailure))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackDisabled"), int64(r.CMDMetric.fallback))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackMissing"), int64(r.CMDMetric.fallback))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackRejection"), int64(r.CMDMetric.RollingCountFallbackRejection))
	//g.setGauge(g.createHystrixCMDMetricName("countFallbackSuccess"), int64(r.CMDMetric.RollingCountFallbackSuccess))
	//g.setGauge(g.createHystrixCMDMetricName("countResponsesFromCache"), int64(r.CMDMetric.RollingCountResponsesFromCache))
	//g.setGauge(g.createHystrixCMDMetricName("countSemaphoreRejected"), int64(r.CMDMetric.RollingCountSemaphoreRejected))
	//g.setGauge(g.createHystrixCMDMetricName("countShortCircuited"), int64(r.CMDMetric.RollingCountShortCircuited))
	//g.setGauge(g.createHystrixCMDMetricName("countSuccess"), int64(r.CMDMetric.RollingCountSuccess))
	//g.setGauge(g.createHystrixCMDMetricName("countThreadPoolRejected"), int64(r.CMDMetric.RollingCountThreadPoolRejected))
	//g.setGauge(g.createHystrixCMDMetricName("countTimeout"), int64(r.CMDMetric.RollingCountTimeout))

	g.setGauge(g.createHystrixCMDMetricName("executionSemaphorePermitsInUse"), int64(r.CMDMetric.CurrentConcurrentExecutionCount))

	//g.setGauge(g.createHystrixCMDMetricName("rollingCountBadRequests"), int64(r.CMDMetric.count))
	//g.setGauge(g.createHystrixCMDMetricName("rollingCountEmit"), int64(r.CMDMetric.count))
	//g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackEmit"), int64(r.CMDMetric.count))
	//g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackDisabled"), int64(r.CMDMetric.count))
	//g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackMissing"), int64(r.CMDMetric.count))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountExceptionsThrown"), int64(r.CMDMetric.RollingCountExceptionsThrown))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountFailure"), int64(r.CMDMetric.RollingCountFailure))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackFailure"), int64(r.CMDMetric.RollingCountFallbackFailure))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountCollapsedRequests"), int64(r.CMDMetric.RollingCountCollapsedRequests))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackRejection"), int64(r.CMDMetric.RollingCountFallbackRejection))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountFallbackSuccess"), int64(r.CMDMetric.RollingCountFallbackSuccess))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountResponsesFromCache"), int64(r.CMDMetric.RollingCountResponsesFromCache))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountSemaphoreRejected"), int64(r.CMDMetric.RollingCountSemaphoreRejected))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountShortCircuited"), int64(r.CMDMetric.RollingCountShortCircuited))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountSuccess"), int64(r.CMDMetric.RollingCountSuccess))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountThreadPoolRejected"), int64(r.CMDMetric.RollingCountThreadPoolRejected))
	g.setGauge(g.createHystrixCMDMetricName("rollingCountTimeout"), int64(r.CMDMetric.RollingCountTimeout))
	//g.setGauge(g.createHystrixCMDMetricName("rollingMaxConcurrentExecutionCount"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("executionSemaphorePermitsInUse"), int64(r.CMDMetric.))

	g.setGauge(g.createHystrixCMDMetricName("errorPercentage"), int64(r.CMDMetric.ErrorPct))

	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_mean"), int64(r.CMDMetric.LatencyExecuteMean))
	//g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_5"), int64(r.CMDMetric.LatencyExecute.))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_25"), int64(r.CMDMetric.LatencyExecute.Timing25))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_50"), int64(r.CMDMetric.LatencyExecute.Timing50))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_75"), int64(r.CMDMetric.LatencyExecute.Timing75))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_90"), int64(r.CMDMetric.LatencyExecute.Timing90))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_99"), int64(r.CMDMetric.LatencyExecute.Timing99))
	g.setGauge(g.createHystrixCMDMetricName("latencyExecute_percentile_995"), int64(r.CMDMetric.LatencyExecute.Timing995))

	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_mean"), int64(r.CMDMetric.LatencyTotalMean))
	//g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_5"), int64(r.CMDMetric.LatencyTotalMean))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_25"), int64(r.CMDMetric.LatencyTotal.Timing25))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_50"), int64(r.CMDMetric.LatencyTotal.Timing50))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_75"), int64(r.CMDMetric.LatencyTotal.Timing75))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_90"), int64(r.CMDMetric.LatencyTotal.Timing90))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_99"), int64(r.CMDMetric.LatencyTotal.Timing99))
	g.setGauge(g.createHystrixCMDMetricName("latencyTotal_percentile_995"), int64(r.CMDMetric.LatencyTotal.Timing995))

	//g.setGauge(g.createHystrixCMDMetricName("commandGroup"), int64(r.CMDMetric.LatencyTotal.Timing995))

	g.setGauge(g.createHystrixCMDMetricName("propertyValue_rollingStatisticalWindowInMilliseconds"), int64(r.CMDMetric.RollingStatsWindow))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_circuitBreakerRequestVolumeThreshold"), int64(r.CMDMetric.CircuitBreakerRequestVolumeThreshold))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_circuitBreakerSleepWindowInMilliseconds"), int64(r.CMDMetric.CircuitBreakerSleepWindow))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_circuitBreakerErrorThresholdPercentage"), int64(r.CMDMetric.CircuitBreakerErrorThresholdPercent))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_circuitBreakerForceOpen"), int64(r.CMDMetric.CircuitBreakerRequestVolumeThreshold))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_circuitBreakerForceClosed"), int64(r.CMDMetric.CircuitBreakerRequestVolumeThreshold))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_executionIsolationThreadTimeoutInMilliseconds"), int64(r.CMDMetric.ExecutionIsolationThreadTimeout))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_executionTimeoutInMilliseconds"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_executionIsolationStrategy"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_metricsRollingPercentileEnabled"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_requestCacheEnabled"), int64(r.CMDMetric.))
	//g.setGauge(g.createHystrixCMDMetricName("propertyValue_requestLogEnabled"), int64(r.CMDMetric.))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_executionIsolationSemaphoreMaxConcurrentRequests"), int64(r.CMDMetric.ExecutionIsolationSemaphoreMaxConcurrentRequests))
	g.setGauge(g.createHystrixCMDMetricName("propertyValue_fallbackIsolationSemaphoreMaxConcurrentRequests"), int64(r.CMDMetric.FallbackIsolationSemaphoreMaxConcurrentRequests))

}
