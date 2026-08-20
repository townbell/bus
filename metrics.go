package bus

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics defines the monitoring interface for the event bus
type Metrics interface {
	IncrementPublished()
	IncrementProcessed()
	IncrementFailed()
	IncrementSubscribers()
	DecrementSubscribers()
	GetStats() (published, processed, failed int64, activeSubscribers int32)
}

// DetailedMetrics is an optional metrics extension for topic and handler level data.
type DetailedMetrics interface {
	Metrics
	RecordPublished(topic string)
	RecordProcessed(topic, handlerID string, duration time.Duration)
	RecordFailed(topic, handlerID string, duration time.Duration)
}

// HandlerMetricsCleaner is an optional Metrics extension for discarding
// per-handler data when a subscription becomes inactive.
//
// Implementations should retain aggregate topic and global metrics.
type HandlerMetricsCleaner interface {
	RemoveHandlerMetrics(topic, handlerID string)
}

// TopicMetricsSnapshot is a read-only copy of metrics for a topic.
type TopicMetricsSnapshot struct {
	PublishedEvents int64
	ProcessedEvents int64
	FailedEvents    int64
	TotalDuration   time.Duration
}

// HandlerMetricsSnapshot is a read-only copy of metrics for a handler.
type HandlerMetricsSnapshot struct {
	Topic           string
	ProcessedEvents int64
	FailedEvents    int64
	TotalDuration   time.Duration
}

type topicMetrics struct {
	publishedEvents atomic.Int64
	processedEvents atomic.Int64
	failedEvents    atomic.Int64
	totalDuration   atomic.Int64
}

type handlerMetrics struct {
	topic           string
	processedEvents atomic.Int64
	failedEvents    atomic.Int64
	totalDuration   atomic.Int64
}

// DefaultMetrics is the default implementation of the Metrics interface.
//
// Aggregate counters and detailed metrics are independently atomically updated.
// The detailed metric maps use sync.Map because they grow only when a topic or
// subscription is first observed, but are read and written on every publish.
// Read them through GetStats, GetTopicStats, and GetHandlerStats rather than
// accessing the counter fields directly.
type DefaultMetrics struct {
	publishedEvents   int64
	processedEvents   int64
	failedEvents      int64
	activeSubscribers int32
	topicMetrics      sync.Map // map[string]*topicMetrics
	handlerMetrics    sync.Map // map[string]*handlerMetrics
}

var _ DetailedMetrics = (*DefaultMetrics)(nil)
var _ HandlerMetricsCleaner = (*DefaultMetrics)(nil)

func (m *DefaultMetrics) IncrementPublished() {
	atomic.AddInt64(&m.publishedEvents, 1)
}

func (m *DefaultMetrics) IncrementProcessed() {
	atomic.AddInt64(&m.processedEvents, 1)
}

func (m *DefaultMetrics) IncrementFailed() {
	atomic.AddInt64(&m.failedEvents, 1)
}

func (m *DefaultMetrics) IncrementSubscribers() {
	atomic.AddInt32(&m.activeSubscribers, 1)
}

func (m *DefaultMetrics) DecrementSubscribers() {
	atomic.AddInt32(&m.activeSubscribers, -1)
}

func (m *DefaultMetrics) GetStats() (published, processed, failed int64, activeSubscribers int32) {
	return atomic.LoadInt64(&m.publishedEvents),
		atomic.LoadInt64(&m.processedEvents),
		atomic.LoadInt64(&m.failedEvents),
		atomic.LoadInt32(&m.activeSubscribers)
}

// RecordPublished records a published event for a topic.
func (m *DefaultMetrics) RecordPublished(topic string) {
	m.getTopicMetrics(topic).publishedEvents.Add(1)
}

// RecordProcessed records a successful handler execution.
func (m *DefaultMetrics) RecordProcessed(topic, handlerID string, duration time.Duration) {
	topicStats := m.getTopicMetrics(topic)
	topicStats.processedEvents.Add(1)
	topicStats.totalDuration.Add(int64(duration))

	handlerStats := m.getHandlerMetrics(topic, handlerID)
	handlerStats.processedEvents.Add(1)
	handlerStats.totalDuration.Add(int64(duration))
}

// RecordFailed records a failed handler execution.
func (m *DefaultMetrics) RecordFailed(topic, handlerID string, duration time.Duration) {
	topicStats := m.getTopicMetrics(topic)
	topicStats.failedEvents.Add(1)
	topicStats.totalDuration.Add(int64(duration))

	handlerStats := m.getHandlerMetrics(topic, handlerID)
	handlerStats.failedEvents.Add(1)
	handlerStats.totalDuration.Add(int64(duration))
}

// GetTopicStats returns a snapshot of per-topic metrics.
func (m *DefaultMetrics) GetTopicStats() map[string]TopicMetricsSnapshot {
	result := make(map[string]TopicMetricsSnapshot)
	m.topicMetrics.Range(func(key, value any) bool {
		topic := key.(string)
		stats := value.(*topicMetrics)
		result[topic] = TopicMetricsSnapshot{
			PublishedEvents: stats.publishedEvents.Load(),
			ProcessedEvents: stats.processedEvents.Load(),
			FailedEvents:    stats.failedEvents.Load(),
			TotalDuration:   time.Duration(stats.totalDuration.Load()),
		}
		return true
	})
	return result
}

// GetHandlerStats returns a snapshot of per-handler metrics.
func (m *DefaultMetrics) GetHandlerStats() map[string]HandlerMetricsSnapshot {
	result := make(map[string]HandlerMetricsSnapshot)
	m.handlerMetrics.Range(func(key, value any) bool {
		handlerID := key.(string)
		stats := value.(*handlerMetrics)
		result[handlerID] = HandlerMetricsSnapshot{
			Topic:           stats.topic,
			ProcessedEvents: stats.processedEvents.Load(),
			FailedEvents:    stats.failedEvents.Load(),
			TotalDuration:   time.Duration(stats.totalDuration.Load()),
		}
		return true
	})
	return result
}

// RemoveHandlerMetrics discards per-handler metrics for an inactive subscription.
func (m *DefaultMetrics) RemoveHandlerMetrics(_ string, handlerID string) {
	m.handlerMetrics.Delete(handlerID)
}

func (m *DefaultMetrics) getTopicMetrics(topic string) *topicMetrics {
	if metrics, ok := m.topicMetrics.Load(topic); ok {
		return metrics.(*topicMetrics)
	}
	candidate := &topicMetrics{}
	metrics, loaded := m.topicMetrics.LoadOrStore(topic, candidate)
	if loaded {
		return metrics.(*topicMetrics)
	}
	return candidate
}

func (m *DefaultMetrics) getHandlerMetrics(topic, handlerID string) *handlerMetrics {
	if metrics, ok := m.handlerMetrics.Load(handlerID); ok {
		return metrics.(*handlerMetrics)
	}
	candidate := &handlerMetrics{topic: topic}
	metrics, loaded := m.handlerMetrics.LoadOrStore(handlerID, candidate)
	if loaded {
		return metrics.(*handlerMetrics)
	}
	return candidate
}
