package bus

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestHierarchicalPatternMatching(t *testing.T) {
	cases := []struct {
		pattern, topic string
		want           bool
	}{
		{"*", "user.created", true},
		{"*", "anything", true},
		{"user.*", "user.created", true},
		{"user.*", "user.created.eu", true},
		{"user.*", "user", false},
		{"user.*", "users.created", false},
		{"user.*", "user.", false},
		{"user.created.*", "user.created.eu", true},
		{"user.created.*", "user.created", false},
		{"user", "user.created", false}, // not a pattern
	}
	for _, c := range cases {
		if got := topicMatchesPattern(c.pattern, c.topic); got != c.want {
			t.Errorf("topicMatchesPattern(%q, %q) = %v, want %v", c.pattern, c.topic, got, c.want)
		}
	}
}

func TestHierarchicalPatternSubscription(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var calls []string
	var mu sync.Mutex
	record := func(name string) Handler[TestEvent] {
		return func(ctx context.Context, event TestEvent) error {
			mu.Lock()
			calls = append(calls, name+":"+event.ID)
			mu.Unlock()
			return nil
		}
	}

	mustSubscribe(t, bus, "user.*", record("pattern"), HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "user.created", record("exact"))

	bus.Publish("user.created", TestEvent{ID: "a"})    // exact + pattern
	bus.Publish("user.deleted.eu", TestEvent{ID: "b"}) // pattern only (deep)
	bus.Publish("order.created", TestEvent{ID: "c"})   // neither
	bus.Publish("user", TestEvent{ID: "d"})            // prefix itself: neither

	want := []string{"pattern:a", "exact:a", "pattern:b"}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != len(want) {
		t.Fatalf("Expected %d calls, got %d: %v", len(want), len(calls), calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("Expected calls[%d]=%q, got %q", i, want[i], calls[i])
		}
	}
}

func TestPatternUnsubscribeStopsMatching(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var count int32
	handle := mustSubscribe(t, bus, "user.*", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	bus.Publish("user.created", TestEvent{ID: "a"})
	if err := handle.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	bus.Publish("user.created", TestEvent{ID: "b"})

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("Expected pattern handler to stop after unsubscribe, got %d calls", count)
	}
}

func TestPatternSubscribeOnce(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var count int32
	mustSubscribe(t, bus, "order.*", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	}, HandlerOnce())

	bus.Publish("order.created", TestEvent{ID: "a"})
	bus.Publish("order.cancelled", TestEvent{ID: "b"})

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("Expected once pattern handler to run once, got %d", count)
	}
	if bus.HasCallback("order.*") {
		t.Fatal("Expected once pattern handler to unsubscribe")
	}
}

func TestDeadEventHandler(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var dead []string
	var mu sync.Mutex
	bus.SetDeadEventHandler(func(topic string, event TestEvent) {
		mu.Lock()
		dead = append(dead, topic+":"+event.ID)
		mu.Unlock()
	})

	// No subscribers at all: dead event fires.
	bus.Publish("nobody.home", TestEvent{ID: "a"})

	// A pattern subscriber counts as a subscriber: no dead event.
	mustSubscribe(t, bus, "user.*", discard[TestEvent])
	bus.Publish("user.created", TestEvent{ID: "b"})

	// A subscribed-but-filtered-out handler still counts as a subscriber.
	mustSubscribe(t, bus, "orders", discard[TestEvent],
		HandlerFilter(func(topic string, event TestEvent) bool { return false }))
	bus.Publish("orders", TestEvent{ID: "c"})

	mu.Lock()
	defer mu.Unlock()
	if len(dead) != 1 || dead[0] != "nobody.home:a" {
		t.Fatalf("Expected exactly one dead event nobody.home:a, got %v", dead)
	}
}

func TestDeadEventHandlerViaOptionAndRemoval(t *testing.T) {
	var count int32
	bus := NewTyped[TestEvent](WithDeadEventHandler[TestEvent](func(topic string, event TestEvent) {
		atomic.AddInt32(&count, 1)
	}))
	defer bus.Close()

	bus.Publish("void", TestEvent{ID: "a"})
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("Expected constructor-option dead handler to fire, got %d", count)
	}

	bus.SetDeadEventHandler(nil)
	bus.Publish("void", TestEvent{ID: "b"})
	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("Expected no dead events after removal, got %d", count)
	}
}
