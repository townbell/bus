package bus

import (
	"context"
	"fmt"
)

// Priority defines the execution priority of handlers
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// Handler processes events delivered to a subscription.
//
// The context reports cancellation to the handler: for synchronous handlers it
// is derived from the publish call and is canceled when the publish context is
// canceled or the handler's timeout elapses; asynchronous handlers receive the
// subscription context instead, because the publish call may return before they
// run.
//
// A non-nil error marks the delivery as failed: it is counted in metrics,
// reported to the bus ErrorHandler, and - for synchronous handlers - joined
// into the error returned by the publish call. Returning an error does not
// stop dispatch to the remaining handlers.
type Handler[T any] func(ctx context.Context, event T) error

// EventError represents an error that occurred during event handling
type EventError struct {
	Topic   string
	Event   interface{}
	Handler interface{}
	Err     error
}

func (e *EventError) Error() string {
	return fmt.Sprintf("event error in topic '%s': %v", e.Topic, e.Err)
}

// DeadEventHandler observes events published to a topic with no subscribed
// handlers, in the spirit of Guava's DeadEvent. It runs synchronously in the
// publishing goroutine, so it should return quickly.
type DeadEventHandler[T any] func(topic string, event T)

// EventFilter allows filtering events before they reach handlers
type EventFilter[T any] func(topic string, event T) bool

// EventMiddleware allows intercepting events before and after processing
type EventMiddleware[T any] func(topic string, event T, next func()) error

// ErrorHandler defines how to handle errors during event processing
type ErrorHandler func(err *EventError)
