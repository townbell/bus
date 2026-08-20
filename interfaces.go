package bus

import (
	"context"
	"time"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber[T any] interface {
	Subscribe(topic string, fn Handler[T], options ...HandlerOption) (*Handle[T], error)
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
