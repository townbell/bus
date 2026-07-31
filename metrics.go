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
	publishedEvents int64
	processedEvents int64
	failedEvents    int64
	totalDuration   time.Duration
}

type handlerMetrics struct {
	topic           string
	processedEvents int64
	failedEvents    int64
	totalDuration   time.Duration
}

// DefaultMetrics is the default implementation of the Metrics interface.
//
// The counter fields are updated atomically and sit on the publish hot path;
// read them through GetStats rather than directly. The mutex guards only the
// per-topic and per-handler maps.
type DefaultMetrics struct {
	PublishedEvents   int64
	ProcessedEvents   int64
	FailedEvents      int64
	ActiveSubscribers int32
	topicMetrics      map[string]*topicMetrics
	handlerMetrics    map[string]*handlerMetrics
	mu                sync.RWMutex
}

var _ DetailedMetrics = (*DefaultMetrics)(nil)
var _ HandlerMetricsCleaner = (*DefaultMetrics)(nil)

func (m *DefaultMetrics) IncrementPublished() {
	atomic.AddInt64(&m.PublishedEvents, 1)
}

func (m *DefaultMetrics) IncrementProcessed() {
	atomic.AddInt64(&m.ProcessedEvents, 1)
}

func (m *DefaultMetrics) IncrementFailed() {
	atomic.AddInt64(&m.FailedEvents, 1)
}

func (m *DefaultMetrics) IncrementSubscribers() {
	atomic.AddInt32(&m.ActiveSubscribers, 1)
}

func (m *DefaultMetrics) DecrementSubscribers() {
	atomic.AddInt32(&m.ActiveSubscribers, -1)
}

func (m *DefaultMetrics) GetStats() (published, processed, failed int64, activeSubscribers int32) {
	return atomic.LoadInt64(&m.PublishedEvents),
		atomic.LoadInt64(&m.ProcessedEvents),
		atomic.LoadInt64(&m.FailedEvents),
		atomic.LoadInt32(&m.ActiveSubscribers)
}

// RecordPublished records a published event for a topic.
func (m *DefaultMetrics) RecordPublished(topic string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMaps()
	m.getTopicMetricsLocked(topic).publishedEvents++
}

// RecordProcessed records a successful handler execution.
func (m *DefaultMetrics) RecordProcessed(topic, handlerID string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMaps()
	topicStats := m.getTopicMetricsLocked(topic)
	topicStats.processedEvents++
	topicStats.totalDuration += duration

	handlerStats := m.getHandlerMetricsLocked(topic, handlerID)
	handlerStats.processedEvents++
	handlerStats.totalDuration += duration
}

// RecordFailed records a failed handler execution.
func (m *DefaultMetrics) RecordFailed(topic, handlerID string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensureMaps()
	topicStats := m.getTopicMetricsLocked(topic)
	topicStats.failedEvents++
	topicStats.totalDuration += duration

	handlerStats := m.getHandlerMetricsLocked(topic, handlerID)
	handlerStats.failedEvents++
	handlerStats.totalDuration += duration
}

// GetTopicStats returns a snapshot of per-topic metrics.
func (m *DefaultMetrics) GetTopicStats() map[string]TopicMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]TopicMetricsSnapshot, len(m.topicMetrics))
	for topic, stats := range m.topicMetrics {
		result[topic] = TopicMetricsSnapshot{
			PublishedEvents: stats.publishedEvents,
			ProcessedEvents: stats.processedEvents,
			FailedEvents:    stats.failedEvents,
			TotalDuration:   stats.totalDuration,
		}
	}
	return result
}

// GetHandlerStats returns a snapshot of per-handler metrics.
func (m *DefaultMetrics) GetHandlerStats() map[string]HandlerMetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]HandlerMetricsSnapshot, len(m.handlerMetrics))
	for handlerID, stats := range m.handlerMetrics {
		result[handlerID] = HandlerMetricsSnapshot{
			Topic:           stats.topic,
			ProcessedEvents: stats.processedEvents,
			FailedEvents:    stats.failedEvents,
			TotalDuration:   stats.totalDuration,
		}
	}
	return result
}

// RemoveHandlerMetrics discards per-handler metrics for an inactive subscription.
func (m *DefaultMetrics) RemoveHandlerMetrics(_ string, handlerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.handlerMetrics, handlerID)
}

func (m *DefaultMetrics) ensureMaps() {
	if m.topicMetrics == nil {
		m.topicMetrics = make(map[string]*topicMetrics)
	}
	if m.handlerMetrics == nil {
		m.handlerMetrics = make(map[string]*handlerMetrics)
	}
}

func (m *DefaultMetrics) getTopicMetricsLocked(topic string) *topicMetrics {
	stats := m.topicMetrics[topic]
	if stats == nil {
		stats = &topicMetrics{}
		m.topicMetrics[topic] = stats
	}
	return stats
}

func (m *DefaultMetrics) getHandlerMetricsLocked(topic, handlerID string) *handlerMetrics {
	stats := m.handlerMetrics[handlerID]
	if stats == nil {
		stats = &handlerMetrics{topic: topic}
		m.handlerMetrics[handlerID] = stats
	}
	return stats
}
