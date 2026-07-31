package bus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Handle represents a subscription handle that can be used to unsubscribe
type Handle[T any] struct {
	bus     *EventBus[T]
	topic   string
	handler *eventHandler[T]
	mu      sync.Mutex
}

// Unsubscribe removes this specific subscription.
//
// A nil handle is reported as an error rather than a panic, so ignoring the
// error from Subscribe and deferring Unsubscribe stays safe.
func (h *Handle[T]) Unsubscribe() error {
	if h == nil {
		return ErrNilHandle
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	if h.handler == nil {
		return ErrSubscriptionInactive
	}
	if !h.handler.active.Load() {
		h.handler = nil
		return ErrSubscriptionInactive
	}

	if logger := h.bus.GetLogger(); logger != nil {
		logger.Debug("Unsubscribing handler from topic '%s'", h.topic)
	}
	if !h.bus.removeHandler(h.topic, h.handler) {
		h.handler = nil
		return fmt.Errorf("%w: handler not found for topic %s", ErrSubscriptionInactive, h.topic)
	}
	h.handler = nil

	return nil
}

// IsActive returns whether this handle is still active. A nil handle is never
// active.
func (h *Handle[T]) IsActive() bool {
	if h == nil {
		return false
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	return h.handler != nil && h.handler.active.Load()
}

// eventHandler represents an internal event handler
type eventHandler[T any] struct {
	id                    string
	topic                 string
	callBack              Handler[T]
	flagOnce              bool
	async                 bool
	transactional         bool
	priority              Priority
	filter                EventFilter[T]
	ctx                   context.Context
	timeout               time.Duration
	recoverPolicy         RecoverPolicy
	maxConcurrency        int
	concurrency           chan struct{}
	active                atomic.Bool
	metricsMu             sync.Mutex
	metricsInFlight       int
	metricsCleanupPending bool
	sync.Mutex            // lock for an event handler - useful for running async callbacks serially
}
