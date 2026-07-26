package bus

import (
	"context"
	"time"
)

// RecoverPolicy controls how handler panics are reported to the publisher.
type RecoverPolicy int

const (
	// RecoverAndContinue records panics and continues dispatching later handlers.
	RecoverAndContinue RecoverPolicy = iota
	// RecoverAndStop records panics and stops the current publish call.
	RecoverAndStop
)

type handlerOptions struct {
	priority       Priority
	ctx            context.Context
	async          bool
	transactional  bool
	once           bool
	timeout        time.Duration
	recoverPolicy  RecoverPolicy
	maxConcurrency int
	// filter holds an EventFilter[T]. The option constructor is generic while
	// HandlerOption is not, so the value is carried as any and re-typed by
	// Subscribe, which reports a mismatch as an error.
	filter any
}

// HandlerOption configures a subscription created with Subscribe.
type HandlerOption func(*handlerOptions)

// HandlerPriority sets the handler priority.
func HandlerPriority(priority Priority) HandlerOption {
	return func(opts *handlerOptions) {
		opts.priority = priority
	}
}

// HandlerContext sets a context that can disable the handler when canceled.
// Asynchronous handlers also receive this context while running.
func HandlerContext(ctx context.Context) HandlerOption {
	return func(opts *handlerOptions) {
		opts.ctx = ctx
	}
}

// HandlerAsync runs the handler in a goroutine. Set transactional to true to serialize this handler.
func HandlerAsync(transactional bool) HandlerOption {
	return func(opts *handlerOptions) {
		opts.async = true
		opts.transactional = transactional
	}
}

// HandlerOnce removes the handler before its first dispatch attempt.
func HandlerOnce() HandlerOption {
	return func(opts *handlerOptions) {
		opts.once = true
	}
}

// HandlerFilter runs the handler only for events accepted by the filter. The
// filter's event type must match the bus event type; Subscribe reports a
// mismatch as an error.
func HandlerFilter[T any](filter EventFilter[T]) HandlerOption {
	return func(opts *handlerOptions) {
		if filter == nil {
			return
		}
		opts.filter = filter
	}
}

// HandlerTimeout bounds how long a publish call waits for this handler. The
// handler's context is canceled when the timeout elapses.
func HandlerTimeout(timeout time.Duration) HandlerOption {
	return func(opts *handlerOptions) {
		opts.timeout = timeout
	}
}

// HandlerRecoverPolicy sets how recovered panics affect the current publish call.
func HandlerRecoverPolicy(policy RecoverPolicy) HandlerOption {
	return func(opts *handlerOptions) {
		opts.recoverPolicy = policy
	}
}

// HandlerMaxConcurrency limits concurrent executions of this handler. Values below 1 mean unlimited.
func HandlerMaxConcurrency(limit int) HandlerOption {
	return func(opts *handlerOptions) {
		opts.maxConcurrency = limit
	}
}

// HandlerSerial limits this handler to one execution at a time.
func HandlerSerial() HandlerOption {
	return HandlerMaxConcurrency(1)
}

func defaultHandlerOptions() handlerOptions {
	return handlerOptions{
		priority:      PriorityNormal,
		ctx:           context.Background(),
		recoverPolicy: RecoverAndContinue,
	}
}
