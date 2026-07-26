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
