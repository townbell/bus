package bus

import "errors"

var (
	// ErrBusClosed reports an operation attempted after an EventBus was closed.
	ErrBusClosed = errors.New("event bus is closed")
	// ErrNilHandler reports an attempt to subscribe a nil handler.
	ErrNilHandler = errors.New("event handler is nil")
	// ErrNilHandle reports an operation attempted on a nil subscription handle.
	ErrNilHandle = errors.New("handle is nil: the subscription was never created")
	// ErrSubscriptionInactive reports an operation attempted on an inactive subscription.
	ErrSubscriptionInactive = errors.New("subscription is inactive")
)
