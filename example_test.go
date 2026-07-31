package bus_test

import (
	"context"
	"errors"
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

	handle, err := b.Subscribe("user.login", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("%s performed %s\n", event.UserID, event.Action)
		return nil
	})
	if err != nil {
		fmt.Println("subscribe failed:", err)
		return
	}
	defer handle.Unsubscribe()

	b.Publish("user.login", UserEvent{UserID: "u-1", Action: "login"})

	// Output:
	// u-1 performed login
}

// Handlers run from the highest priority to the lowest. Handlers registered at
// the same priority keep their registration order.
func ExampleHandlerPriority() {
	b := bus.NewTyped[string]()
	defer b.Close()

	say := func(line string) bus.Handler[string] {
		return func(ctx context.Context, event string) error {
			fmt.Println(line)
			return nil
		}
	}

	b.Subscribe("order.placed", say("3. analytics"), bus.HandlerPriority(bus.PriorityLow))
	b.Subscribe("order.placed", say("1. fraud check"), bus.HandlerPriority(bus.PriorityCritical))
	b.Subscribe("order.placed", say("2. fulfilment"), bus.HandlerPriority(bus.PriorityNormal))

	b.Publish("order.placed", "o-1")

	// Output:
	// 1. fraud check
	// 2. fulfilment
	// 3. analytics
}

// A filter decides per event whether its handler runs at all.
func ExampleHandlerFilter() {
	b := bus.NewTyped[UserEvent]()
	defer b.Close()

	b.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Println("admin action:", event.Action)
		return nil
	}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
		return strings.HasPrefix(event.UserID, "admin-")
	}))

	b.Publish("user.action", UserEvent{UserID: "u-1", Action: "read"})
	b.Publish("user.action", UserEvent{UserID: "admin-1", Action: "delete"})

	// Output:
	// admin action: delete
}

// Options compose: a single Subscribe call configures priority, timeout,
// panic policy and concurrency.
func ExampleEventBus_Subscribe() {
	b := bus.NewTyped[string]()
	defer b.Close()

	handle, err := b.Subscribe("payment.validate",
		func(ctx context.Context, id string) error {
			fmt.Println("validating", id)
			return nil
		},
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

// A handler error does not stop dispatch: later handlers still run, and the
// publish call returns the joined failures of the synchronous handlers.
func ExampleEventBus_Publish() {
	b := bus.NewTyped[string]()
	defer b.Close()
	// The default logger reports handler failures on stdout; silence it here
	// so the example output stays deterministic.
	b.SetLogger(bus.NewNoOpLogger())

	b.Subscribe("job.run", func(ctx context.Context, id string) error {
		return errors.New("disk full")
	}, bus.HandlerPriority(bus.PriorityHigh))
	b.Subscribe("job.run", func(ctx context.Context, id string) error {
		fmt.Println("second handler still runs")
		return nil
	})

	err := b.Publish("job.run", "j-1")
	fmt.Println("err:", err)

	// Output:
	// second handler still runs
	// err: disk full
}

// PublishCollect exposes each synchronous dispatch failure separately when a
// caller needs to log, retry, or classify individual handler outcomes.
func ExampleEventBus_PublishCollect() {
	b := bus.NewTyped[string]()
	defer b.Close()
	b.SetLogger(bus.NewNoOpLogger())

	b.Subscribe("job.run", func(ctx context.Context, id string) error {
		return errors.New("disk full")
	}, bus.HandlerPriority(bus.PriorityHigh))
	b.Subscribe("job.run", func(ctx context.Context, id string) error {
		return errors.New("index unavailable")
	})

	for _, err := range b.PublishCollect("job.run", "j-1") {
		fmt.Println("failure:", err)
	}

	// Output:
	// failure: disk full
	// failure: index unavailable
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

	b.Subscribe("job.run", func(ctx context.Context, id string) error {
		fmt.Println("handling", id)
		return nil
	})
	b.Publish("job.run", "j-1")

	// Output:
	// before job.run
	// handling j-1
	// after job.run
}

// A trailing ".*" subscribes to every topic under a prefix: "orders.*"
// matches "orders.created" and "orders.created.eu", but not "orders" itself.
func ExampleEventBus_Subscribe_hierarchical() {
	b := bus.NewTyped[string]()
	defer b.Close()

	b.Subscribe("orders.*", func(ctx context.Context, payload string) error {
		fmt.Println("orders event:", payload)
		return nil
	})

	b.Publish("orders.created", "o-1")
	b.Publish("orders.created.eu", "o-2")
	b.Publish("orders", "ignored") // the prefix itself does not match
	b.Publish("invoices.created", "ignored")

	// Output:
	// orders event: o-1
	// orders event: o-2
}

// A dead-event handler observes publishes that reached no subscriber at all —
// usually a misspelled topic.
func ExampleEventBus_SetDeadEventHandler() {
	b := bus.NewTyped[string]()
	defer b.Close()

	b.SetDeadEventHandler(func(topic string, event string) {
		fmt.Printf("dead event on %q: %s\n", topic, event)
	})
	b.Subscribe("user.created", func(ctx context.Context, event string) error {
		return nil
	})

	b.Publish("user.created", "delivered") // has a subscriber: no dead event
	b.Publish("user.craeted", "u-1")       // typo: nobody subscribed

	// Output:
	// dead event on "user.craeted": u-1
}

// The "*" topic receives every event. Wildcard handlers are merged with the
// topic's own handlers and ordered by priority.
func ExampleEventBus_Subscribe_wildcard() {
	b := bus.NewTyped[string]()
	defer b.Close()

	b.Subscribe("user.created", func(ctx context.Context, payload string) error {
		fmt.Println("welcome:", payload)
		return nil
	})
	b.Subscribe("*", func(ctx context.Context, payload string) error {
		fmt.Println("audit:", payload)
		return nil
	})

	b.Publish("user.created", "u-1")

	// Output:
	// welcome: u-1
	// audit: u-1
}

// Canceling the publish context aborts dispatch; a closed bus rejects the
// publish outright.
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

	b.Subscribe("job.run", func(ctx context.Context, id string) error { return nil })
	b.Publish("job.run", "j-1")
	b.Publish("job.run", "j-2")

	published, processed, failed, subscribers := b.GetMetrics().GetStats()
	fmt.Println(published, processed, failed, subscribers)

	// Output:
	// 2 2 0 1
}

// Ignoring the error from Subscribe leaves a nil handle. The nil handle stays
// safe to use, so a deferred Unsubscribe never panics.
func ExampleHandle_Unsubscribe_nilHandle() {
	b := bus.NewTyped[string]()
	b.Close()

	handle, _ := b.Subscribe("user.created", func(ctx context.Context, event string) error {
		return nil
	})
	fmt.Println("active:", handle.IsActive())
	fmt.Println("err:", handle.Unsubscribe())

	// Output:
	// active: false
	// err: handle is nil: the subscription was never created
}
