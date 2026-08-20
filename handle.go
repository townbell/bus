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
	id             string
	topic          string
	callBack       Handler[T]
	flagOnce       bool
	async          bool
	transactional  bool
	priority       Priority
	filter         EventFilter[T]
	ctx            context.Context
	timeout        time.Duration
	recoverPolicy  RecoverPolicy
	maxConcurrency int
	concurrency    chan struct{}
	asyncQueue     *asyncQueue
	active         atomic.Bool
	// metricsState stores the number of running handler invocations and a
	// cleanup bit. It avoids serializing every publish merely to coordinate
	// metric-series removal during unsubscribe.
	metricsState atomic.Int64
	sync.Mutex   // lock for an event handler - useful for running async callbacks serially
}

// asyncQueue bounds asynchronous work without keeping idle worker goroutines.
// A worker drains queued tasks before exiting, so running never exceeds limit.
type asyncQueue struct {
	limit    int
	capacity int
	mu       sync.Mutex
	running  int
	tasks    []func()
}

func (q *asyncQueue) submit(task func()) bool {
	q.mu.Lock()
	if q.running+len(q.tasks) >= q.capacity {
		q.mu.Unlock()
		return false
	}
	if q.running < q.limit {
		q.running++
		q.mu.Unlock()
		go q.run(task)
		return true
	}
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()
	return true
}

func (q *asyncQueue) run(task func()) {
	for {
		task()
		q.mu.Lock()
		if len(q.tasks) == 0 {
			q.running--
			q.mu.Unlock()
			return
		}
		task = q.tasks[0]
		q.tasks[0] = nil // Do not retain an event after it is dequeued.
		q.tasks = q.tasks[1:]
		q.mu.Unlock()
	}
}
