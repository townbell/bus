package bus

import (
	"context"
	"testing"
)

// The Subscribe helpers that return only a handle yield nil when the
// subscription is rejected. A deferred Unsubscribe on that nil handle must
// report an error instead of panicking.
func TestNilHandleIsSafe(t *testing.T) {
	closed := NewTyped[string]()
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cases := map[string]*Handle[string]{
		"closed bus, SubscribeWithHandle":   closed.SubscribeWithHandle("topic", func(string) {}),
		"closed bus, SubscribeWithPriority": closed.SubscribeWithPriority("topic", func(string) {}, PriorityHigh),
		"closed bus, SubscribeWithFilter": closed.SubscribeWithFilter("topic", func(string) {}, func(string, string) bool {
			return true
		}),
		"closed bus, SubscribeWithContext": closed.SubscribeWithContext(context.Background(), "topic", func(string) {}),
		"nil callback":                     NewTyped[string]().SubscribeWithHandle("topic", nil),
	}

	for name, handle := range cases {
		t.Run(name, func(t *testing.T) {
			if handle != nil {
				t.Fatalf("expected a nil handle, got %#v", handle)
			}

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("nil handle panicked: %v", r)
				}
			}()

			if handle.IsActive() {
				t.Error("nil handle reported active")
			}
			if err := handle.Unsubscribe(); err == nil {
				t.Error("Unsubscribe on a nil handle returned no error")
			}
		})
	}
}

func TestHandleUnsubscribeIsIdempotent(t *testing.T) {
	b := NewTyped[string]()
	defer b.Close()

	handle := b.SubscribeWithHandle("topic", func(string) {})
	if handle == nil {
		t.Fatal("expected a handle")
	}
	if !handle.IsActive() {
		t.Fatal("fresh handle should be active")
	}

	if err := handle.Unsubscribe(); err != nil {
		t.Fatalf("first Unsubscribe: %v", err)
	}
	if handle.IsActive() {
		t.Error("handle still active after Unsubscribe")
	}
	if err := handle.Unsubscribe(); err == nil {
		t.Error("second Unsubscribe returned no error")
	}
}
