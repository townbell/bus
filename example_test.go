package bus_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/townbell/bus"
)

type UserEvent struct {
	UserID string
	Action string
}

// Synchronous handlers run in the goroutine that calls Publish, so the event is
// fully handled by the time Publish returns.
func Example() {
	b := bus.NewTyped[UserEvent]()
	defer b.Close()

	handle := b.SubscribeWithHandle("user.login", func(event UserEvent) {
		fmt.Printf("%s performed %s\n", event.UserID, event.Action)
	})
	defer handle.Unsubscribe()

	b.Publish("user.login", UserEvent{UserID: "u-1", Action: "login"})

	// Output:
	// u-1 performed login
}

// Handlers run from the highest priority to the lowest. Handlers registered at
// the same priority keep their registration order.
func ExampleEventBus_SubscribeWithPriority() {
	b := bus.NewTyped[string]()
	defer b.Close()

	b.SubscribeWithPriority("order.placed", func(string) {
		fmt.Println("3. analytics")
	}, bus.PriorityLow)
	b.SubscribeWithPriority("order.placed", func(string) {
		fmt.Println("1. fraud check")
	}, bus.PriorityCritical)
	b.SubscribeWithPriority("order.placed", func(string) {
		fmt.Println("2. fulfilment")
	}, bus.PriorityNormal)

	b.Publish("order.placed", "o-1")

	// Output:
	// 1. fraud check
	// 2. fulfilment
	// 3. analytics
}

// A filter decides per event whether its handler runs at all.
func ExampleEventBus_SubscribeWithFilter() {
	b := bus.NewTyped[UserEvent]()
	defer b.Close()

	b.SubscribeWithFilter("user.action", func(event UserEvent) {
		fmt.Println("admin action:", event.Action)
	}, func(topic string, event UserEvent) bool {
		return strings.HasPrefix(event.UserID, "admin-")
	})

	b.Publish("user.action", UserEvent{UserID: "u-1", Action: "read"})
	b.Publish("user.action", UserEvent{UserID: "admin-1", Action: "delete"})

	// Output:
	// admin action: delete
}

// SubscribeWithOptions is the extensible subscription path. It is the only
// helper that reports why a subscription was rejected.
func ExampleEventBus_SubscribeWithOptions() {
	b := bus.NewTyped[string]()
	defer b.Close()

	handle, err := b.SubscribeWithOptions("payment.validate",
		func(id string) { fmt.Println("validating", id) },
		bus.HandlerPriority(bus.PriorityHigh),
		bus.HandlerTimeout(2*time.Second),
		bus.HandlerRecoverPolicy(bus.RecoverAndStop),
		bus.HandlerSerial(),
	)
	if err != nil {
		fmt.Println("subscribe failed:", err)
		return
	}
	defer handle.Unsubscribe()

	b.Publish("payment.validate", "p-1")

	// Output:
	// validating p-1
}

// Middleware wraps the whole dispatch. Calling next runs the remaining
// middleware and then the handlers; not calling it intercepts the event.
func ExampleEventBus_AddMiddleware() {
	b := bus.NewTyped[string]()
	defer b.Close()

	b.AddMiddleware(func(topic string, event any, next func()) error {
		fmt.Println("before", topic)
		next()
		fmt.Println("after", topic)
		return nil
	})

	_ = b.Subscribe("job.run", func(id string) {
		fmt.Println("handling", id)
	})
	b.Publish("job.run", "j-1")

	// Output:
	// before job.run
	// handling j-1
	// after job.run
}

// The "*" topic receives every event. Wildcard handlers are merged with the
// topic's own handlers and ordered by priority.
func ExampleEventBus_Subscribe_wildcard() {
	b := bus.NewTyped[string]()
	defer b.Close()

	_ = b.Subscribe("user.created", func(payload string) {
		fmt.Println("welcome:", payload)
	})
	_ = b.Subscribe("*", func(payload string) {
		fmt.Println("audit:", payload)
	})

	b.Publish("user.created", "u-1")

	// Output:
	// welcome: u-1
	// audit: u-1
}

// Publish discards errors. Use PublishWithContext when cancellation, timeout or
// closed-bus errors matter.
func ExampleEventBus_PublishWithContext() {
	b := bus.NewTyped[string]()
	b.Close()

	err := b.PublishWithContext(context.Background(), "user.created", "u-1")
	fmt.Println("err:", err)

	// Output:
	// err: event bus is closed
}

func ExampleEventBus_GetMetrics() {
	b := bus.NewTyped[string]()
	defer b.Close()

	_ = b.Subscribe("job.run", func(string) {})
	b.Publish("job.run", "j-1")
	b.Publish("job.run", "j-2")

	published, processed, failed, subscribers := b.GetMetrics().GetStats()
	fmt.Println(published, processed, failed, subscribers)

	// Output:
	// 2 2 0 1
}

// Subscribe helpers that return only a handle yield nil when the subscription
// is rejected. The returned handle stays safe to use, so a deferred
// Unsubscribe never panics.
func ExampleHandle_Unsubscribe_nilHandle() {
	b := bus.NewTyped[string]()
	b.Close()

	handle := b.SubscribeWithHandle("user.created", func(string) {})
	fmt.Println("active:", handle.IsActive())
	fmt.Println("err:", handle.Unsubscribe())

	// Output:
	// active: false
	// err: handle is nil: the subscription was never created
}
