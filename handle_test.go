package bus

import (
	"context"
	"testing"
)

// Ignoring the error from Subscribe leaves a nil handle. A deferred
// Unsubscribe on that nil handle must report an error instead of panicking.
func TestNilHandleIsSafe(t *testing.T) {
	closed := NewTyped[string]()
	if err := closed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	subscribe := func(b *EventBus[string], fn Handler[string], opts ...HandlerOption) *Handle[string] {
		handle, _ := b.Subscribe("topic", fn, opts...)
		return handle
	}

	cases := map[string]*Handle[string]{
		"closed bus":               subscribe(closed, discard[string]),
		"closed bus with priority": subscribe(closed, discard[string], HandlerPriority(PriorityHigh)),
		"closed bus with context":  subscribe(closed, discard[string], HandlerContext(context.Background())),
		"nil callback":             subscribe(NewTyped[string](), nil),
		"filter type mismatch": subscribe(NewTyped[string](), discard[string],
			HandlerFilter(func(topic string, event int) bool { return true })),
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

	handle := mustSubscribe(t, b, "topic", discard[string])
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
