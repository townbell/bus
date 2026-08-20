package prometheus

import (
	"errors"
	"fmt"
	"time"

	client "github.com/prometheus/client_golang/prometheus"
	bus "github.com/townbell/bus"
)

// Config controls Prometheus metric names and registration.
type Config struct {
	Namespace  string
	Subsystem  string
	Registerer client.Registerer
	Labels     client.Labels
	Buckets    []float64
}

// Metrics implements bus.DetailedMetrics using Prometheus collectors.
type Metrics struct {
	base *bus.DefaultMetrics

	publishedTotal  client.Counter
	processedTotal  client.Counter
	failedTotal     client.Counter
	subscribers     client.Gauge
	publishedTopic  *client.CounterVec
	processedTopic  *client.CounterVec
	failedTopic     *client.CounterVec
	processedHandle *client.CounterVec
	failedHandle    *client.CounterVec
	duration        *client.HistogramVec
}

var _ bus.DetailedMetrics = (*Metrics)(nil)
var _ bus.HandlerMetricsCleaner = (*Metrics)(nil)

// New creates a Prometheus-backed metrics collector.
func New(config Config) (*Metrics, error) {
	if config.Namespace == "" {
		config.Namespace = "go_bus"
	}
	if config.Registerer == nil {
		config.Registerer = client.DefaultRegisterer
	}
	if len(config.Buckets) == 0 {
		config.Buckets = client.DefBuckets
	}

	m := &Metrics{base: &bus.DefaultMetrics{}}
	m.publishedTotal = client.NewCounter(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_published_total",
		Help:        "Total number of events published.",
		ConstLabels: config.Labels,
	})
	m.processedTotal = client.NewCounter(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_processed_total",
		Help:        "Total number of handler executions that completed successfully.",
		ConstLabels: config.Labels,
	})
	m.failedTotal = client.NewCounter(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_failed_total",
		Help:        "Total number of handler executions that failed.",
		ConstLabels: config.Labels,
	})
	m.subscribers = client.NewGauge(client.GaugeOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "active_subscribers",
		Help:        "Current number of active subscribers.",
		ConstLabels: config.Labels,
	})
	m.publishedTopic = client.NewCounterVec(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_published_by_topic_total",
		Help:        "Total number of events published by topic.",
		ConstLabels: config.Labels,
	}, []string{"topic"})
	m.processedTopic = client.NewCounterVec(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_processed_by_topic_total",
		Help:        "Total number of successful handler executions by topic.",
		ConstLabels: config.Labels,
	}, []string{"topic"})
	m.failedTopic = client.NewCounterVec(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "events_failed_by_topic_total",
		Help:        "Total number of failed handler executions by topic.",
		ConstLabels: config.Labels,
	}, []string{"topic"})
	m.processedHandle = client.NewCounterVec(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "handler_processed_total",
		Help:        "Total number of successful executions by handler.",
		ConstLabels: config.Labels,
	}, []string{"topic", "handler"})
	m.failedHandle = client.NewCounterVec(client.CounterOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "handler_failed_total",
		Help:        "Total number of failed executions by handler.",
		ConstLabels: config.Labels,
	}, []string{"topic", "handler"})
	m.duration = client.NewHistogramVec(client.HistogramOpts{
		Namespace:   config.Namespace,
		Subsystem:   config.Subsystem,
		Name:        "handler_duration_seconds",
		Help:        "Handler execution duration by topic, handler, and status.",
		ConstLabels: config.Labels,
		Buckets:     config.Buckets,
	}, []string{"topic", "handler", "status"})

	var err error
	if m.publishedTotal, err = registerOrReuse[client.Counter](config.Registerer, m.publishedTotal); err != nil {
		return nil, err
	}
	if m.processedTotal, err = registerOrReuse[client.Counter](config.Registerer, m.processedTotal); err != nil {
		return nil, err
	}
	if m.failedTotal, err = registerOrReuse[client.Counter](config.Registerer, m.failedTotal); err != nil {
		return nil, err
	}
	if m.subscribers, err = registerOrReuse[client.Gauge](config.Registerer, m.subscribers); err != nil {
		return nil, err
	}
	if m.publishedTopic, err = registerOrReuse[*client.CounterVec](config.Registerer, m.publishedTopic); err != nil {
		return nil, err
	}
	if m.processedTopic, err = registerOrReuse[*client.CounterVec](config.Registerer, m.processedTopic); err != nil {
		return nil, err
	}
	if m.failedTopic, err = registerOrReuse[*client.CounterVec](config.Registerer, m.failedTopic); err != nil {
		return nil, err
	}
	if m.processedHandle, err = registerOrReuse[*client.CounterVec](config.Registerer, m.processedHandle); err != nil {
		return nil, err
	}
	if m.failedHandle, err = registerOrReuse[*client.CounterVec](config.Registerer, m.failedHandle); err != nil {
		return nil, err
	}
	if m.duration, err = registerOrReuse[*client.HistogramVec](config.Registerer, m.duration); err != nil {
		return nil, err
	}
	return m, nil
}

// MustNew is New for callers that treat invalid metric registration as a
// process-startup failure.
func MustNew(config Config) *Metrics {
	metrics, err := New(config)
	if err != nil {
		panic(err)
	}
	return metrics
}

func registerOrReuse[T client.Collector](registerer client.Registerer, collector T) (T, error) {
	if err := registerer.Register(collector); err != nil {
		var alreadyRegistered client.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			existing, ok := alreadyRegistered.ExistingCollector.(T)
			if !ok {
				var zero T
				return zero, fmt.Errorf("prometheus collector already registered with incompatible type: %w", err)
			}
			return existing, nil
		}
		var zero T
		return zero, fmt.Errorf("register prometheus collector: %w", err)
	}
	return collector, nil
}

func (m *Metrics) IncrementPublished() {
	m.base.IncrementPublished()
	m.publishedTotal.Inc()
}

func (m *Metrics) IncrementProcessed() {
	m.base.IncrementProcessed()
	m.processedTotal.Inc()
}

func (m *Metrics) IncrementFailed() {
	m.base.IncrementFailed()
	m.failedTotal.Inc()
}

func (m *Metrics) IncrementSubscribers() {
	m.base.IncrementSubscribers()
	m.subscribers.Inc()
}

func (m *Metrics) DecrementSubscribers() {
	m.base.DecrementSubscribers()
	m.subscribers.Dec()
}

func (m *Metrics) GetStats() (published, processed, failed int64, activeSubscribers int32) {
	return m.base.GetStats()
}

func (m *Metrics) RecordPublished(topic string) {
	m.base.RecordPublished(topic)
	m.publishedTopic.WithLabelValues(topic).Inc()
}

func (m *Metrics) RecordProcessed(topic, handlerID string, duration time.Duration) {
	m.base.RecordProcessed(topic, handlerID, duration)
	m.processedTopic.WithLabelValues(topic).Inc()
	m.processedHandle.WithLabelValues(topic, handlerID).Inc()
	m.duration.WithLabelValues(topic, handlerID, "success").Observe(duration.Seconds())
}

func (m *Metrics) RecordFailed(topic, handlerID string, duration time.Duration) {
	m.base.RecordFailed(topic, handlerID, duration)
	m.failedTopic.WithLabelValues(topic).Inc()
	m.failedHandle.WithLabelValues(topic, handlerID).Inc()
	m.duration.WithLabelValues(topic, handlerID, "failed").Observe(duration.Seconds())
}

func (m *Metrics) GetTopicStats() map[string]bus.TopicMetricsSnapshot {
	return m.base.GetTopicStats()
}

func (m *Metrics) GetHandlerStats() map[string]bus.HandlerMetricsSnapshot {
	return m.base.GetHandlerStats()
}

// RemoveHandlerMetrics removes metric series for an inactive subscription.
// Aggregate topic and global metrics remain available.
func (m *Metrics) RemoveHandlerMetrics(topic, handlerID string) {
	m.base.RemoveHandlerMetrics(topic, handlerID)
	m.processedHandle.DeleteLabelValues(topic, handlerID)
	m.failedHandle.DeleteLabelValues(topic, handlerID)
	m.duration.DeleteLabelValues(topic, handlerID, "success")
	m.duration.DeleteLabelValues(topic, handlerID, "failed")
}
