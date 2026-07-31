package bus

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EventBus dispatches events to topic subscribers.
//
// EventBus is safe for concurrent use by multiple goroutines.
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
}

// handlerIDSequence gives handler metrics a process-wide identity. This keeps
// per-handler Prometheus series distinct when several buses share a registry.
var handlerIDSequence atomic.Uint64

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
		return nil, ErrNilHandler
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
	handler.active.Store(true)

	bus.lock.Lock()
	defer bus.lock.Unlock()

	if bus.closed {
		return nil, ErrBusClosed
	}
	bus.prepareHandlerLocked(topic, handler)

	// Handler slices are copy-on-write: once stored in the map they are never
	// mutated, so a publish can use them without taking its own copy. Insert
	// before the first handler with strictly lower priority, so equal
	// priorities keep subscription order.
	old := bus.handlers[topic]
	at := len(old)
	for i, h := range old {
		if handler.priority > h.priority {
			at = i
			break
		}
	}
	handlers := make([]*eventHandler[T], 0, len(old)+1)
	handlers = append(handlers, old[:at]...)
	handlers = append(handlers, handler)
	handlers = append(handlers, old[at:]...)
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
	handler.id = topic + "#" + strconv.FormatUint(handlerIDSequence.Add(1), 10)
	if handler.maxConcurrency > 0 && handler.concurrency == nil {
		handler.concurrency = make(chan struct{}, handler.maxConcurrency)
	}
}

// HasCallback reports whether topic has an exact or matching-pattern subscription.
func (bus *EventBus[T]) HasCallback(topic string) bool {
	bus.lock.RLock()
	defer bus.lock.RUnlock()
	if len(bus.handlers[topic]) > 0 {
		return true
	}
	for pattern := range bus.patternTopics {
		if topicMatchesPattern(pattern, topic) && len(bus.handlers[pattern]) > 0 {
			return true
		}
	}
	return false
}

// Publish delivers event to the topic's handlers. The returned error joins
// the failures of the synchronous handlers, and is safe to ignore when
// delivery failures do not matter to the caller. Asynchronous handler
// failures are reported through the ErrorHandler instead.
func (bus *EventBus[T]) Publish(topic string, event T) error {
	return errors.Join(bus.PublishCollect(topic, event)...)
}

// PublishWithContext publishes an event with context. Canceling the context
// aborts dispatch to the remaining handlers and cancels the context passed to
// the currently running synchronous handler.
func (bus *EventBus[T]) PublishWithContext(ctx context.Context, topic string, event T) error {
	return errors.Join(bus.PublishCollectWithContext(ctx, topic, event)...)
}

// PublishCollect publishes an event and returns each synchronous dispatch
// failure in handler execution order. It includes errors caused by context
// cancellation, closing the bus, and middleware. Asynchronous handler
// failures remain available through ErrorHandler only, because they may occur
// after this method returns.
//
// A nil result means no synchronous dispatch failure occurred. The result is
// independent of future publishes and may be inspected or retained by the
// caller.
func (bus *EventBus[T]) PublishCollect(topic string, event T) []error {
	return bus.PublishCollectWithContext(context.Background(), topic, event)
}

// PublishCollectWithContext is PublishCollect with a caller-provided context.
// A nil context is treated as context.Background.
func (bus *EventBus[T]) PublishCollectWithContext(ctx context.Context, topic string, event T) []error {
	if ctx == nil {
		ctx = context.Background()
	}

	bus.lock.RLock()
	if bus.closed {
		bus.lock.RUnlock()
		return []error{ErrBusClosed}
	}
	logger := bus.logger
	errorHandler := bus.errorHandler
	deadEventHandler := bus.deadEventHandler
	metrics := bus.metrics
	closeCh := bus.closeCh
	// Handler slices are copy-on-write (see Subscribe), so the map value can
	// be used directly without copying on the hot path.
	handlers := bus.handlers[topic]
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
		merged := make([]*eventHandler[T], 0, len(handlers)+len(patterns))
		merged = append(merged, handlers...)
		for _, pattern := range patterns {
			merged = append(merged, bus.handlers[pattern]...)
		}
		sort.SliceStable(merged, func(i, j int) bool {
			return merged[i].priority > merged[j].priority
		})
		handlers = merged
	}
	middlewares := append([]EventMiddleware[any](nil), bus.middlewares...)
	bus.lock.RUnlock()

	// A varargs call boxes its arguments before the logger can filter by
	// level, so the level check happens here, once per publish.
	debugLog := logger != nil && logger.GetLevel() <= LogLevelDebug
	if debugLog {
		logger.Debug("Publishing event to topic '%s'", topic)
	}

	metrics.IncrementPublished()
	if detailed, ok := metrics.(DetailedMetrics); ok {
		detailed.RecordPublished(topic)
	}

	if len(handlers) == 0 && deadEventHandler != nil {
		deadEventHandler(topic, event)
	}

	// Fast path: with no middleware there is no reason to box the event into
	// any or allocate the chain closures.
	if len(middlewares) == 0 {
		return bus.dispatchCollect(ctx, topic, event, handlers, closeCh, logger, debugLog, errorHandler, metrics)
	}

	dispatch := func() []error {
		return bus.dispatchCollect(ctx, topic, event, handlers, closeCh, logger, debugLog, errorHandler, metrics)
	}
	dispatchErrs, middlewareErr := runMiddlewares(middlewares, topic, event, dispatch)
	if middlewareErr != nil {
		if errorHandler != nil {
			errorHandler(&EventError{
				Topic: topic,
				Event: event,
				Err:   middlewareErr,
			})
		}
		return []error{middlewareErr}
	}
	return dispatchErrs
}

func runMiddlewares(middlewares []EventMiddleware[any], topic string, event any, dispatch func() []error) (dispatchErrs []error, middlewareErr error) {
	var run func(int)
	run = func(i int) {
		if middlewareErr != nil {
			return
		}
		if i == len(middlewares) {
			dispatchErrs = dispatch()
			return
		}
		var nextMu sync.Mutex
		nextCalled := false
		middlewareReturned := false
		next := func() {
			nextMu.Lock()
			defer nextMu.Unlock()
			if middlewareReturned || nextCalled {
				return
			}
			nextCalled = true
			run(i + 1)
		}
		err := middlewares[i](topic, event, next)
		nextMu.Lock()
		middlewareReturned = true
		nextMu.Unlock()
		if err != nil {
			middlewareErr = err
		}
	}
	run(0)
	return dispatchErrs, middlewareErr
}

func (bus *EventBus[T]) dispatchCollect(ctx context.Context, topic string, event T, handlers []*eventHandler[T], closeCh <-chan struct{}, logger Logger, debugLog bool, errorHandler ErrorHandler, metrics Metrics) []error {
	var errs []error

	for _, handler := range handlers {
		select {
		case <-ctx.Done():
			return append(errs, ctx.Err())
		case <-closeCh:
			return append(errs, ErrBusClosed)
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
			err, stop := bus.doPublish(ctx, closeCh, handler, topic, event, logger, debugLog, errorHandler, metrics)
			if err != nil {
				errs = append(errs, err)
			}
			if stop {
				return errs
			}
			continue
		}

		if !bus.addAsync() {
			return append(errs, ErrBusClosed)
		}
		if handler.transactional {
			handler.Lock()
		}
		go bus.doPublishAsync(handler, topic, event, closeCh, logger, debugLog, errorHandler, metrics)
	}
	return errs
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
	return errors.Join(bus.PublishCollectWithTimeout(topic, event, timeout)...)
}

// PublishCollectWithTimeout is PublishCollect with a timeout. A timeout is
// reported as context.DeadlineExceeded in the returned errors.
func (bus *EventBus[T]) PublishCollectWithTimeout(topic string, event T, timeout time.Duration) []error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return bus.PublishCollectWithContext(ctx, topic, event)
}

// PanicError wraps a value recovered from a panicking handler. It reaches the
// ErrorHandler and, for synchronous handlers, the joined publish error, so
// callers can tell panics apart from ordinary handler errors:
//
//	var pe *bus.PanicError
//	if errors.As(err, &pe) { ... }
type PanicError struct {
	// Value is the value the handler panicked with.
	Value any
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("panic: %v", e.Value)
}

// doPublish runs one handler and records the outcome. The second return value
// reports whether dispatch must stop (a recovered panic under RecoverAndStop).
func (bus *EventBus[T]) doPublish(ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T], topic string, event T, logger Logger, debugLog bool, errorHandler ErrorHandler, metrics Metrics) (error, bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	bus.beginHandlerMetrics(handler)
	start := time.Now()
	err := bus.runHandler(ctx, closeCh, handler, event)
	duration := time.Since(start)

	if err == nil {
		if debugLog {
			logger.Debug("Handler executed successfully for topic '%s'", topic)
		}
		metrics.IncrementProcessed()
		bus.recordHandlerMetrics(metrics, handler, topic, duration, false)
		return nil, false
	}

	var panicErr *PanicError
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
	bus.recordHandlerMetrics(metrics, handler, topic, duration, true)
	return err, isPanic && handler.recoverPolicy == RecoverAndStop
}

func (bus *EventBus[T]) runHandler(ctx context.Context, closeCh <-chan struct{}, handler *eventHandler[T], event T) error {
	_, hasDeadline := ctx.Deadline()
	if handler.timeout <= 0 && !hasDeadline {
		return bus.runHandlerWithoutTimeout(ctx, closeCh, handler, event)
	}

	if !bus.addAsync() {
		return ErrBusClosed
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
		return ErrBusClosed
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
			err = &PanicError{Value: r}
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
		return ErrBusClosed
	}
}

func (bus *EventBus[T]) doPublishAsync(handler *eventHandler[T], topic string, event T, closeCh <-chan struct{}, logger Logger, debugLog bool, errorHandler ErrorHandler, metrics Metrics) {
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
	_, _ = bus.doPublish(ctx, closeCh, handler, topic, event, logger, debugLog, errorHandler, metrics)
}

func (bus *EventBus[T]) removeHandler(topic string, target *eventHandler[T]) bool {
	bus.lock.Lock()

	if _, ok := bus.handlers[topic]; !ok {
		bus.lock.Unlock()
		return false
	}

	for idx, handler := range bus.handlers[topic] {
		if handler != target {
			continue
		}
		target.active.Store(false)
		// Copy-on-write: build a fresh slice so publishes holding the old one
		// keep a consistent view without copying on their hot path.
		old := bus.handlers[topic]
		if len(old) == 1 {
			delete(bus.handlers, topic)
			delete(bus.patternTopics, topic)
		} else {
			handlers := make([]*eventHandler[T], 0, len(old)-1)
			handlers = append(handlers, old[:idx]...)
			handlers = append(handlers, old[idx+1:]...)
			bus.handlers[topic] = handlers
		}
		bus.metrics.DecrementSubscribers()

		// Log handler removal
		if bus.logger != nil {
			bus.logger.Debug("Handler removed from topic '%s'", topic)
		}
		bus.lock.Unlock()
		bus.removeHandlerMetrics(target)
		return true
	}
	bus.lock.Unlock()
	return false
}

func (bus *EventBus[T]) beginHandlerMetrics(handler *eventHandler[T]) {
	handler.metricsMu.Lock()
	defer handler.metricsMu.Unlock()
	handler.metricsInFlight++
}

func (bus *EventBus[T]) recordHandlerMetrics(metrics Metrics, handler *eventHandler[T], topic string, duration time.Duration, failed bool) {
	handler.metricsMu.Lock()
	defer handler.metricsMu.Unlock()

	if detailed, ok := metrics.(DetailedMetrics); ok {
		if failed {
			detailed.RecordFailed(topic, handler.id, duration)
		} else {
			detailed.RecordProcessed(topic, handler.id, duration)
		}
	}
	handler.metricsInFlight--
	if handler.metricsCleanupPending && handler.metricsInFlight == 0 {
		if cleaner, ok := metrics.(HandlerMetricsCleaner); ok {
			cleaner.RemoveHandlerMetrics(handler.topic, handler.id)
		}
	}
}

func (bus *EventBus[T]) removeHandlerMetrics(handler *eventHandler[T]) {
	handler.metricsMu.Lock()
	defer handler.metricsMu.Unlock()
	handler.metricsCleanupPending = true
	if handler.metricsInFlight == 0 {
		if cleaner, ok := bus.metrics.(HandlerMetricsCleaner); ok {
			cleaner.RemoveHandlerMetrics(handler.topic, handler.id)
		}
	}
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

// GetTopics returns all topics that have subscribers in lexical order.
func (bus *EventBus[T]) GetTopics() []string {
	bus.lock.RLock()
	defer bus.lock.RUnlock()

	topics := make([]string, 0, len(bus.handlers))
	for topic := range bus.handlers {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
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
		return fmt.Errorf("%w: already closed", ErrBusClosed)
	}

	bus.closed = true
	close(bus.closeCh)
	subscriberCount := 0
	removedHandlers := make([]*eventHandler[T], 0)
	for _, handlers := range bus.handlers {
		subscriberCount += len(handlers)
		for _, handler := range handlers {
			handler.active.Store(false)
			removedHandlers = append(removedHandlers, handler)
		}
	}
	bus.handlers = make(map[string][]*eventHandler[T])
	bus.patternTopics = make(map[string]struct{})
	bus.lock.Unlock()

	for _, handler := range removedHandlers {
		bus.removeHandlerMetrics(handler)
	}

	// Wait for all async operations to complete
	bus.wg.Wait()

	for i := 0; i < subscriberCount; i++ {
		bus.metrics.DecrementSubscribers()
	}

	return nil
}
