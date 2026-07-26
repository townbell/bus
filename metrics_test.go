package bus

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// CustomMetrics is a custom implementation of the Metrics interface
type CustomMetrics struct {
	published   int64
	processed   int64
	failed      int64
	subscribers int32
}

func (m *CustomMetrics) IncrementPublished() {
	atomic.AddInt64(&m.published, 1)
}

func (m *CustomMetrics) IncrementProcessed() {
	atomic.AddInt64(&m.processed, 1)
}

func (m *CustomMetrics) IncrementFailed() {
	atomic.AddInt64(&m.failed, 1)
}

func (m *CustomMetrics) IncrementSubscribers() {
	atomic.AddInt32(&m.subscribers, 1)
}

func (m *CustomMetrics) DecrementSubscribers() {
	atomic.AddInt32(&m.subscribers, -1)
}

func (m *CustomMetrics) GetStats() (published, processed, failed int64, activeSubscribers int32) {
	return atomic.LoadInt64(&m.published),
		atomic.LoadInt64(&m.processed),
		atomic.LoadInt64(&m.failed),
		atomic.LoadInt32(&m.subscribers)
}

func TestCustomMetricsImplementation(t *testing.T) {
	// Create custom metrics
	customMetrics := &CustomMetrics{}

	// Create a bus with custom metrics using WithMetrics option
	bus := New(WithMetrics[any](customMetrics))
	defer bus.Close()

	// Subscribe to an event
	handle := mustSubscribe(t, bus, "test.topic", discard[any])
	defer handle.Unsubscribe()

	// Check subscriber count
	published, processed, failed, subscribers := customMetrics.GetStats()
	if subscribers != 1 {
		t.Errorf("Expected 1 subscriber, got %d", subscribers)
	}

	// Publish an event
	bus.Publish("test.topic", "test event")
	bus.WaitAsync()

	// Check metrics
	published, processed, failed, subscribers = customMetrics.GetStats()
	if published != 1 {
		t.Errorf("Expected 1 published event, got %d", published)
	}
	if processed != 1 {
		t.Errorf("Expected 1 processed event, got %d", processed)
	}
	if failed != 0 {
		t.Errorf("Expected 0 failed events, got %d", failed)
	}

	// Unsubscribe
	handle.Unsubscribe()

	// Check subscriber count after unsubscribe
	published, processed, failed, subscribers = customMetrics.GetStats()
	if subscribers != 0 {
		t.Errorf("Expected 0 subscribers after unsubscribe, got %d", subscribers)
	}
}

func TestWithOptions(t *testing.T) {
	// Test various Option combinations

	// Custom metrics
	customMetrics := &CustomMetrics{}

	// Custom logger
	customLogger := NewDefaultLogger()

	// Error handler
	var errorCount int
	errorHandler := func(err *EventError) {
		errorCount++
	}

	// Middleware
	middleware := func(topic string, event any, next func()) error {
		next()
		return nil
	}

	// Create bus with all options
	bus := New(
		WithMetrics[any](customMetrics),
		WithLogger[any](customLogger),
		WithErrorHandler[any](errorHandler),
		WithMiddleware[any](middleware),
	)
	defer bus.Close()

	// Test publish and subscribe
	mustSubscribe(t, bus, "test.options", discard[any])

	bus.Publish("test.options", "test event")
	bus.WaitAsync()

	// Verify metrics are being used correctly
	published, processed, _, _ := customMetrics.GetStats()
	if published != 1 {
		t.Errorf("Expected 1 published event, got %d", published)
	}
	if processed != 1 {
		t.Errorf("Expected 1 processed event, got %d", processed)
	}

	// Get metrics from bus and verify it's our custom implementation
	metricsFromBus := bus.GetMetrics()
	if metricsFromBus != customMetrics {
		t.Errorf("Expected metrics from bus to be our custom metrics instance")
	}
}

func TestHandlerErrorRecordedInDetailedMetrics(t *testing.T) {
	bus := NewTyped[string]()
	defer bus.Close()

	mustSubscribe(t, bus, "orders.created", func(ctx context.Context, event string) error {
		return context.DeadlineExceeded
	})
	_ = bus.Publish("orders.created", "o-1")

	detailed, ok := bus.GetMetrics().(*DefaultMetrics)
	if !ok {
		t.Fatal("Expected the default metrics implementation")
	}
	stats := detailed.GetTopicStats()["orders.created"]
	if stats.FailedEvents != 1 {
		t.Fatalf("Expected the returned handler error to count as a failed delivery, got %d", stats.FailedEvents)
	}
}

func TestDefaultMetricsDetailedSnapshots(t *testing.T) {
	metrics := &DefaultMetrics{}

	metrics.IncrementPublished()
	metrics.RecordPublished("orders.created")
	metrics.IncrementProcessed()
	metrics.RecordProcessed("orders.created", "orders.created#1", 10*time.Millisecond)
	metrics.IncrementFailed()
	metrics.RecordFailed("orders.created", "orders.created#1", 5*time.Millisecond)

	topics := metrics.GetTopicStats()
	topicStats := topics["orders.created"]
	if topicStats.PublishedEvents != 1 {
		t.Fatalf("Expected 1 topic publish, got %d", topicStats.PublishedEvents)
	}
	if topicStats.ProcessedEvents != 1 {
		t.Fatalf("Expected 1 topic processed event, got %d", topicStats.ProcessedEvents)
	}
	if topicStats.FailedEvents != 1 {
		t.Fatalf("Expected 1 topic failed event, got %d", topicStats.FailedEvents)
	}
	if topicStats.TotalDuration != 15*time.Millisecond {
		t.Fatalf("Expected 15ms topic duration, got %v", topicStats.TotalDuration)
	}

	handlers := metrics.GetHandlerStats()
	handlerStats := handlers["orders.created#1"]
	if handlerStats.Topic != "orders.created" {
		t.Fatalf("Expected handler topic orders.created, got %s", handlerStats.Topic)
	}
	if handlerStats.ProcessedEvents != 1 || handlerStats.FailedEvents != 1 {
		t.Fatalf("Expected handler processed=1 failed=1, got processed=%d failed=%d", handlerStats.ProcessedEvents, handlerStats.FailedEvents)
	}
}
