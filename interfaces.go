package bus

import (
	"context"
	"time"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber[T any] interface {
	Subscribe(topic string, fn Handler[T], options ...HandlerOption) (*Handle[T], error)
	SetDeadEventHandler(handler DeadEventHandler[T])
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher[T any] interface {
	Publish(topic string, event T) error
	PublishWithContext(ctx context.Context, topic string, event T) error
	PublishWithTimeout(topic string, event T, timeout time.Duration) error
}

// BusResultCollector defines the optional detailed publishing behavior. It is
// kept separate from BusPublisher so existing publisher implementations stay
// source-compatible.
type BusResultCollector[T any] interface {
	PublishCollect(topic string, event T) []error
	PublishCollectWithContext(ctx context.Context, topic string, event T) []error
	PublishCollectWithTimeout(topic string, event T, timeout time.Duration) []error
}

// BusController defines bus control behavior
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
	GetMetrics() Metrics
	SetErrorHandler(handler ErrorHandler)
	AddMiddleware(middleware EventMiddleware[any])
	SetLogger(logger Logger)
	GetLogger() Logger
	GetTopics() []string
	GetSubscriberCount(topic string) int
	Close() error
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus[T any] interface {
	BusController
	BusSubscriber[T]
	BusPublisher[T]
}
