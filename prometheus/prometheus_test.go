package prometheus

import (
	"context"
	"testing"

	client "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	bus "github.com/townbell/bus"
)

func TestMetricsImplementsBusMetrics(t *testing.T) {
	registry := client.NewRegistry()
	metrics := New(Config{
		Namespace:  "test",
		Subsystem:  "bus",
		Registerer: registry,
	})

	eventBus := bus.NewTyped[string](bus.WithMetrics[string](metrics))
	defer eventBus.Close()

	if _, err := eventBus.Subscribe("topic", func(ctx context.Context, event string) error {
		return nil
	}, bus.HandlerSerial()); err != nil {
		t.Fatalf("Unexpected subscribe error: %v", err)
	}
	if err := eventBus.Publish("topic", "event"); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	published, processed, failed, subscribers := metrics.GetStats()
	if published != 1 || processed != 1 || failed != 0 || subscribers != 1 {
		t.Fatalf("Unexpected stats published=%d processed=%d failed=%d subscribers=%d", published, processed, failed, subscribers)
	}

	if count := testutil.CollectAndCount(metrics.publishedTopic); count != 1 {
		t.Fatalf("Expected one topic counter metric, got %d", count)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 1 {
		t.Fatalf("Expected one handler counter metric, got %d", count)
	}
}

func TestMetricsReusesAlreadyRegisteredCollectors(t *testing.T) {
	registry := client.NewRegistry()
	_ = New(Config{
		Namespace:  "test",
		Subsystem:  "reuse",
		Registerer: registry,
	})
	second := New(Config{
		Namespace:  "test",
		Subsystem:  "reuse",
		Registerer: registry,
	})

	second.RecordPublished("topic")

	if value := testutil.ToFloat64(second.publishedTopic.WithLabelValues("topic")); value != 1 {
		t.Fatalf("Expected reused topic collector value 1, got %v", value)
	}
	if count := testutil.CollectAndCount(second.publishedTopic); count != 1 {
		t.Fatalf("Expected one registered topic metric, got %d", count)
	}
}

func TestMetricsRemovesHandlerSeriesOnUnsubscribe(t *testing.T) {
	registry := client.NewRegistry()
	metrics := New(Config{Namespace: "test", Subsystem: "cleanup", Registerer: registry})
	eventBus := bus.NewTyped[string](bus.WithMetrics[string](metrics))
	defer eventBus.Close()

	handle, err := eventBus.Subscribe("topic", func(ctx context.Context, event string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := eventBus.Publish("topic", "event"); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 1 {
		t.Fatalf("Expected one handler series before unsubscribe, got %d", count)
	}
	if err := handle.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 0 {
		t.Fatalf("Expected no handler series after unsubscribe, got %d", count)
	}
}

func TestMetricsKeepsOtherBusHandlerSeriesOnUnsubscribe(t *testing.T) {
	registry := client.NewRegistry()
	first := New(Config{Namespace: "test", Subsystem: "shared", Registerer: registry})
	second := New(Config{Namespace: "test", Subsystem: "shared", Registerer: registry})
	firstBus := bus.NewTyped[string](bus.WithMetrics[string](first))
	secondBus := bus.NewTyped[string](bus.WithMetrics[string](second))
	defer firstBus.Close()
	defer secondBus.Close()

	firstHandle, err := firstBus.Subscribe("topic", func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatalf("First Subscribe: %v", err)
	}
	if _, err := secondBus.Subscribe("topic", func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("Second Subscribe: %v", err)
	}
	if err := firstBus.Publish("topic", "first"); err != nil {
		t.Fatalf("First Publish: %v", err)
	}
	if err := secondBus.Publish("topic", "second"); err != nil {
		t.Fatalf("Second Publish: %v", err)
	}
	if count := testutil.CollectAndCount(first.processedHandle); count != 2 {
		t.Fatalf("Expected two handler series before unsubscribe, got %d", count)
	}
	if err := firstHandle.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if count := testutil.CollectAndCount(first.processedHandle); count != 1 {
		t.Fatalf("Expected second bus handler series to remain, got %d", count)
	}
}

func TestMetricsRemovesHandlerSeriesAfterOnceAndClose(t *testing.T) {
	registry := client.NewRegistry()
	metrics := New(Config{Namespace: "test", Subsystem: "once_close", Registerer: registry})
	eventBus := bus.NewTyped[string](bus.WithMetrics[string](metrics))

	if _, err := eventBus.Subscribe("once", func(context.Context, string) error { return nil }, bus.HandlerOnce()); err != nil {
		t.Fatalf("Subscribe once: %v", err)
	}
	if err := eventBus.Publish("once", "event"); err != nil {
		t.Fatalf("Publish once: %v", err)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 0 {
		t.Fatalf("Expected no handler series after once delivery, got %d", count)
	}

	if _, err := eventBus.Subscribe("close", func(context.Context, string) error { return nil }); err != nil {
		t.Fatalf("Subscribe close: %v", err)
	}
	if err := eventBus.Publish("close", "event"); err != nil {
		t.Fatalf("Publish close: %v", err)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 1 {
		t.Fatalf("Expected one handler series before Close, got %d", count)
	}
	if err := eventBus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if count := testutil.CollectAndCount(metrics.processedHandle); count != 0 {
		t.Fatalf("Expected no handler series after Close, got %d", count)
	}
}
