package bus

import "fmt"

// Priority defines the execution priority of handlers
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

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

// EventFilter allows filtering events before they reach handlers
type EventFilter[T any] func(topic string, event T) bool

// EventMiddleware allows intercepting events before and after processing
type EventMiddleware[T any] func(topic string, event T, next func()) error

// ErrorHandler defines how to handle errors during event processing
type ErrorHandler func(err *EventError)
