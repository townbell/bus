package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EventBus - box for handlers and callbacks.
type EventBus[T any] struct {
	handlers map[string][]*eventHandler[T]
	// patternTopics tracks the handler-map keys that are patterns ("*" or a
	// trailing ".*"), so a publish only scans patterns instead of every topic.
	patternTopics    map[string]struct{}
	middlewares      []EventMiddleware[any]
	errorHandler     ErrorHandler
	deadEventHandler DeadEventHandler[T]
	metrics          Metrics
	logger           Logger
	lock             sync.RWMutex
	wg               sync.WaitGroup
	closed           bool
	closeCh          chan struct{}
	nextHandlerID    uint64
}

// Option defines a functional option for EventBus
type Option[T any] func(*EventBus[T])

// WithMetrics allows custom Metrics implementation
func WithMetrics[T any](metrics Metrics) Option[T] {
	return func(b *EventBus[T]) {
		if metrics == nil {
			return
		}
		b.metrics = metrics
	}
}

// WithLogger sets a custom logger for the EventBus
func WithLogger[T any](logger Logger) Option[T] {
	return func(b *EventBus[T]) {
		b.logger = logger
	}
}

// WithErrorHandler sets a custom error handler for the EventBus
func WithErrorHandler[T any](handler ErrorHandler) Option[T] {
	return func(b *EventBus[T]) {
		b.errorHandler = handler
	}
}

// WithMiddleware adds a middleware to the EventBus
func WithMiddleware[T any](middleware EventMiddleware[any]) Option[T] {
	return func(b *EventBus[T]) {
		if middleware == nil {
			return
		}
		b.middlewares = append(b.middlewares, middleware)
	}
}

// WithDeadEventHandler sets a handler for events published to a topic with no
// subscribed handlers.
func WithDeadEventHandler[T any](handler DeadEventHandler[T]) Option[T] {
	return func(b *EventBus[T]) {
		b.deadEventHandler = handler
	}
}

// NewTyped returns new EventBus with empty handlers for the specified type.
func NewTyped[T any](opts ...Option[T]) *EventBus[T] {
	b := &EventBus[T]{
		handlers:      make(map[string][]*eventHandler[T]),
		patternTopics: make(map[string]struct{}),
		middlewares:   make([]EventMiddleware[any], 0),
		metrics:       &DefaultMetrics{},
		logger:        NewDefaultLogger(),
		lock:          sync.RWMutex{},
		wg:            sync.WaitGroup{},
		closed:        false,
		closeCh:       make(chan struct{}),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(b)
	}
	if b.metrics == nil {
		b.metrics = &DefaultMetrics{}
	}
	return b
}

// New returns new EventBus with empty handlers (for compatibility, uses any type).
func New(opts ...Option[any]) *EventBus[any] {
	return NewTyped[any](opts...)
}

// Subscribe registers fn for topic and returns a handle that cancels the
// subscription. Behavior is configured through HandlerOption values; with no
// options the handler runs synchronously at PriorityNormal.
//
// Topic may be a pattern: "*" receives every event, and a trailing ".*"
// receives every topic under a prefix — "user.*" matches "user.created" and
// "user.created.eu" but not "user" itself.
//
// Subscribe reports why a subscription was rejected: a nil handler, a filter
// whose event type does not match the bus, or a closed bus. On error the
// returned handle is nil; a nil handle is still safe to use.
func (bus *EventBus[T]) Subscribe(topic string, fn Handler[T], options ...HandlerOption) (*Handle[T], error) {
	if fn == nil {
		return nil, fmt.Errorf("event handler is nil")
	}

	opts := defaultHandlerOptions()
	for _, option := range options {
		if option != nil {
			option(&opts)
		}
	}
	if opts.ctx == nil {
		opts.ctx = context.Background()
	}

	var filter EventFilter[T]
	if opts.filter != nil {
		typed, ok := opts.filter.(EventFilter[T])
		if !ok {
			return nil, fmt.Errorf("handler filter is %T, want a filter for event type %T", opts.filter, *new(T))
		}
		filter = typed
	}

	handler := &eventHandler[T]{
		callBack:       fn,
		topic:          topic,
		flagOnce:       opts.once,
		async:          opts.async,
		transactional:  opts.transactional,
		priority:       opts.priority,
		filter:         filter,
		ctx:            opts.ctx,
		timeout:        opts.timeout,
		recoverPolicy:  opts.recoverPolicy,
		maxConcurrency: opts.maxConcurrency,
		Mutex:          sync.Mutex{},
	}

	bus.lock.Lock()
	defer bus.lock.Unlock()

	if bus.closed {
		return nil, fmt.Errorf("event bus is closed")
	}
	bus.prepareHandlerLocked(topic, handler)

	// Insert before the first handler with strictly lower priority, so equal
	// priorities keep subscription order.
	handlers := bus.handlers[topic]
	inserted := false
	for i, h := range handlers {
		if handler.priority > h.priority {
			handlers = append(handlers[:i], append([]*eventHandler[T]{handler}, handlers[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		handlers = append(handlers, handler)
	}
	bus.handlers[topic] = handlers
	if isPatternTopic(topic) {
		bus.patternTopics[topic] = struct{}{}
	}
	bus.metrics.IncrementSubscribers()

	if bus.logger != nil {
		bus.logger.Debug("Handler subscribed to topic '%s' with priority %v", topic, handler.priority)
	}

	return &Handle[T]{bus: bus, topic: topic, handler: handler}, nil
}

// isPatternTopic reports whether topic subscribes to a pattern rather than a
// single topic.
func isPatternTopic(topic string) bool {
	return topic == "*" || strings.HasSuffix(topic, ".*")
}

// topicMatchesPattern reports whether pattern captures topic. "*" matches
// every topic; "prefix.*" matches every topic strictly under "prefix.".
func topicMatchesPattern(pattern, topic string) bool {
	if pattern == "*" {
		return true
	}
	prefix, ok := strings.CutSuffix(pattern, ".*")
	if !ok {
		return false
	}
	return len(topic) > len(prefix)+1 && strings.HasPrefix(topic, prefix+".")
}

func (bus *EventBus[T]) prepareHandlerLocked(topic string, handler *eventHandler[T]) {
	bus.nextHandlerID++
	handler.id = topic + "#" + strconv.FormatUint(bus.nextHandlerID, 10)
	if handler.maxConcurrency > 0 && handler.concurrency == nil {
		handler.concurrency = make(chan struct{}, handler.maxConcurrency)
	}
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus[T]) HasCallback(topic string) bool {
	bus.lock.RLock()
	defer bus.lock.RUnlock()
	_, ok := bus.handlers[topic]
	if ok {
		return len(bus.handlers[topic]) > 0
	}
	return false
}

// Publish delivers event to the topic's handlers. The returned error joins
// the failures of the synchronous handlers, and is safe to ignore when
// delivery failures do not matter to the caller. Asynchronous handler
// failures are reported through the ErrorHandler instead.
func (bus *EventBus[T]) Publish(topic string, event T) error {
	return bus.PublishWithContext(context.Background(), topic, event)
}

// PublishWithContext publishes an event with context. Canceling the context
// aborts dispatch to the remaining handlers and cancels the context passed to
// the currently running synchronous handler.
func (bus *EventBus[T]) PublishWithContext(ctx context.Context, topic string, event T) error {
	if ctx == nil {
		ctx = context.Background()
	}

	bus.lock.RLock()
	if bus.closed {
		bus.lock.RUnlock()
		return fmt.Errorf("event bus is closed")
	}
	logger := bus.logger
	errorHandler := bus.errorHandler
	deadEventHandler := bus.deadEventHandler
	metrics := bus.metrics
	closeCh := bus.closeCh
	handlers := append([]*eventHandler[T](nil), bus.handlers[topic]...)
	// Merge handlers subscribed to matching patterns. Pattern names are
	// sorted so that same-priority handlers from different patterns keep a
	// deterministic order across publishes.
	var patterns []string
	for pattern := range bus.patternTopics {
		if pattern != topic && topicMatchesPattern(pattern, topic) {
			patterns = append(patterns, pattern)
		}
	}
	if len(patterns) > 0 {
		sort.Strings(patterns)
		for _, pattern := range patterns {
			handlers = append(handlers, bus.handlers[pattern]...)
		}
		sort.SliceStable(handlers, func(i, j int) bool {
			return handlers[i].priority > handlers[j].priority
		})
	}
	middlewares := append([]EventMiddleware[any](nil), bus.middlewares...)
	bus.lock.RUnlock()

	// Log event publishing
	if logger != nil {
		logger.Debug("Publishing event to topic '%s'", topic)
	}

	metrics.IncrementPublished()
	if detailed, ok := metrics.(DetailedMetrics); ok {
		detailed.RecordPublished(topic)
	}

	if len(handlers) == 0 && deadEventHandler != nil {
		deadEventHandler(topic, event)
	}

	dispatch := func() error {
		return bus.dispatch(ctx, topic, event, handlers, closeCh, logger, errorHandler, metrics)
	}
	dispatchErr, middlewareErr := runMiddlewares(middlewares, topic, event, dispatch)
	if middlewareErr != nil {
		if errorHandler != nil {
			errorHandler(&EventError{
				Topic: topic,
				Event: event,
				Err:   middlewareErr,
			})
		}
		return middlewareErr
	}
	return dispatchErr
}

func runMiddlewares(middlewares []EventMiddleware[any], topic string, event any, dispatch func() error) (dispatchErr, middlewareErr error) {
	var run func(int)
	run = func(i int) {
		if middlewareErr != nil {
			return
		}
		if i == len(middlewares) {
			dispatchErr = dispatch()
			return
		}
		var nextOnce sync.Once
		next := func() {
			nextOnce.Do(func() {
				run(i + 1)
			})
		}
		if err := middlewares[i](topic, event, next); err != nil {
			middlewareErr = err
		}
	}
	run(0)
	return dispatchErr, middlewareErr
}

func (bus *EventBus[T]) dispatch(ctx context.Context, topic string, event T, handlers []*eventHandler[T], closeCh <-chan struct{}, logger Logger, errorHandler ErrorHandler, metrics Metrics) error {
	var errs []error
	abort := func(err error) error {
		return errors.Join(append(errs, err)...)
	}

	for _, handler := range handlers {
		select {
		case <-ctx.Done():
			return abort(ctx.Err())
		case <-closeCh:
			return abort(fmt.Errorf("event bus is closed"))
		default:
		}

		if handler.ctx != nil {
			select {
			case <-handler.ctx.Done():
				continue
			default:
			}
		}

		if handler.filter != nil && !handler.filter(topic, event) {
			continue
		}

		if handler.flagOnce && !bus.removeHandler(handler.topic, handler) {
			continue
		}

		if !handler.async {
			err, stop := bus.doPublish(ctx, closeCh, handler, topic, event, logger, errorHandler, metrics)
			if err != nil {
				errs = append(errs, err)
			}
			if stop {
				return errors.Join(errs...)
			}
			continue
		}

		if !bus.addAsync() {
			return abort(fmt.Errorf("event bus is closed"))
		}
		if handler.transactional {
			handler.Lock()
		}
		go bus.doPublishAsync(handler, topic, event, closeCh, logger, errorHandler, metrics)
	}
	return errors.Join(errs...)
}

func (bus *EventBus[T]) addAsync() bool {
	bus.lock.RLock()
	defer bus.lock.RUnlock()
	if bus.closed {
		return false
	}
	bus.wg.Add(1)
	return true
}

// PublishWithTimeout publishes an event with timeout
func (bus *EventBus[T]) PublishWithTimeout(topic string, event T, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return bus.PublishWithContext(ctx, topic, event)
}

type handlerPanicError struct {
	value interface{}
}

func (e *handlerPanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.value)
}

// doPublish runs one handler and records the outcome. The second return value
// reports whether dispatch must stop (a recovered panic under RecoverAndStop).
func (bus *EventBus[T]) doPublish(ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T], topic string, event T, logger Logger, errorHandler ErrorHandler, metrics Metrics) (error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	start := time.Now()
	err := bus.runHandler(ctx, closeCh, handler, event)
	duration := time.Since(start)

	if err == nil {
		if logger != nil {
			logger.Debug("Handler executed successfully for topic '%s'", topic)
		}
		metrics.IncrementProcessed()
		if detailed, ok := metrics.(DetailedMetrics); ok {
			detailed.RecordProcessed(topic, handler.id, duration)
		}
		return nil, false
	}

	var panicErr *handlerPanicError
	isPanic := errors.As(err, &panicErr)
	if logger != nil {
		if isPanic {
			logger.Error("Handler panic for topic '%s': %v", topic, err)
		} else {
			logger.Error("Handler failed for topic '%s': %v", topic, err)
		}
	}
	if errorHandler != nil {
		errorHandler(&EventError{
			Topic:   topic,
			Event:   event,
			Handler: handler.callBack,
			Err:     err,
		})
	}
	metrics.IncrementFailed()
	if detailed, ok := metrics.(DetailedMetrics); ok {
		detailed.RecordFailed(topic, handler.id, duration)
	}
	return err, isPanic && handler.recoverPolicy == RecoverAndStop
}

func (bus *EventBus[T]) runHandler(ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T], event T) error {
	_, hasDeadline := ctx.Deadline()
	if handler.timeout <= 0 && !hasDeadline {
		return bus.runHandlerWithoutTimeout(ctx, closeCh, handler, event)
	}

	if !bus.addAsync() {
		return fmt.Errorf("event bus is closed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		defer bus.wg.Done()
		done <- bus.runHandlerWithoutTimeout(runCtx, closeCh, handler, event)
	}()

	var timeout <-chan time.Time
	if handler.timeout > 0 {
		timer := time.NewTimer(handler.timeout)
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case err := <-done:
		return err
	case <-timeout:
		return fmt.Errorf("handler timeout after %s", handler.timeout)
	case <-ctx.Done():
		return ctx.Err()
	case <-closeCh:
		return fmt.Errorf("event bus is closed")
	}
}

func (bus *EventBus[T]) runHandlerWithoutTimeout(ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T], event T) (err error) {
	if err := acquireConcurrency(ctx, closeCh, handler); err != nil {
		return err
	}
	release := handler.concurrency != nil
	defer func() {
		if release {
			<-handler.concurrency
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			err = &handlerPanicError{value: r}
		}
	}()
	return handler.callBack(ctx, event)
}

func acquireConcurrency[T any](ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T]) error {
	if handler.concurrency == nil {
		return nil
	}
	select {
	case handler.concurrency <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-closeCh:
		return fmt.Errorf("event bus is closed")
	}
}

func (bus *EventBus[T]) doPublishAsync(handler *eventHandler[T], topic string, event T, closeCh <-chan struct{}, logger Logger, errorHandler ErrorHandler, metrics Metrics) {
	defer bus.wg.Done()
	defer func() {
		if handler.transactional {
			handler.Unlock()
		}
	}()

	// The publish call may already have returned, so asynchronous handlers run
	// under the subscription context rather than the publish context.
	ctx := handler.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	_, _ = bus.doPublish(ctx, closeCh, handler, topic, event, logger, errorHandler, metrics)
}

func (bus *EventBus[T]) removeHandler(topic string, target *eventHandler[T]) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	if _, ok := bus.handlers[topic]; !ok {
		return false
	}

	for idx, handler := range bus.handlers[topic] {
		if handler != target {
			continue
		}
		l := len(bus.handlers[topic])
		copy(bus.handlers[topic][idx:], bus.handlers[topic][idx+1:])
		bus.handlers[topic][l-1] = nil
		bus.handlers[topic] = bus.handlers[topic][:l-1]
		if len(bus.handlers[topic]) == 0 {
			delete(bus.handlers, topic)
			delete(bus.patternTopics, topic)
		}
		bus.metrics.DecrementSubscribers()

		// Log handler removal
		if bus.logger != nil {
			bus.logger.Debug("Handler removed from topic '%s'", topic)
		}
		return true
	}
	return false
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus[T]) WaitAsync() {
	bus.wg.Wait()
}

// GetMetrics returns the current metrics
func (bus *EventBus[T]) GetMetrics() Metrics {
	return bus.metrics
}

// SetErrorHandler sets the error handler for the bus
func (bus *EventBus[T]) SetErrorHandler(handler ErrorHandler) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	bus.errorHandler = handler
}

// SetDeadEventHandler sets a handler for events published to a topic with no
// subscribed handlers (including pattern subscribers). Pass nil to remove it.
func (bus *EventBus[T]) SetDeadEventHandler(handler DeadEventHandler[T]) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	bus.deadEventHandler = handler
}

// AddMiddleware adds middleware to the bus
func (bus *EventBus[T]) AddMiddleware(middleware EventMiddleware[any]) {
	if middleware == nil {
		return
	}

	bus.lock.Lock()
	defer bus.lock.Unlock()
	bus.middlewares = append(bus.middlewares, middleware)
}

// SetLogger sets the logger for the bus
func (bus *EventBus[T]) SetLogger(logger Logger) {
	bus.lock.Lock()
	defer bus.lock.Unlock()
	bus.logger = logger
}

// GetLogger returns the current logger
func (bus *EventBus[T]) GetLogger() Logger {
	bus.lock.RLock()
	defer bus.lock.RUnlock()
	return bus.logger
}

// GetTopics returns all topics that have subscribers
func (bus *EventBus[T]) GetTopics() []string {
	bus.lock.RLock()
	defer bus.lock.RUnlock()

	topics := make([]string, 0, len(bus.handlers))
	for topic := range bus.handlers {
		topics = append(topics, topic)
	}
	return topics
}

// GetSubscriberCount returns the number of subscribers for a topic
func (bus *EventBus[T]) GetSubscriberCount(topic string) int {
	bus.lock.RLock()
	defer bus.lock.RUnlock()

	if handlers, ok := bus.handlers[topic]; ok {
		return len(handlers)
	}
	return 0
}

// Close gracefully shuts down the event bus
func (bus *EventBus[T]) Close() error {
	bus.lock.Lock()

	if bus.closed {
		bus.lock.Unlock()
		return fmt.Errorf("event bus already closed")
	}

	bus.closed = true
	close(bus.closeCh)
	subscriberCount := 0
	for _, handlers := range bus.handlers {
		subscriberCount += len(handlers)
	}
	bus.handlers = make(map[string][]*eventHandler[T])
	bus.patternTopics = make(map[string]struct{})
	bus.lock.Unlock()

	// Wait for all async operations to complete
	bus.wg.Wait()

	for i := 0; i < subscriberCount; i++ {
		bus.metrics.DecrementSubscribers()
	}

	return nil
}
