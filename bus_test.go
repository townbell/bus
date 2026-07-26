package bus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestEvent struct {
	ID    string
	Value int
}

// discard is a handler that succeeds without doing anything.
func discard[T any](context.Context, T) error { return nil }

// mustSubscribe fails the test when the subscription is rejected.
func mustSubscribe[T any](tb testing.TB, b *EventBus[T], topic string, fn Handler[T], opts ...HandlerOption) *Handle[T] {
	tb.Helper()
	handle, err := b.Subscribe(topic, fn, opts...)
	if err != nil {
		tb.Fatalf("Subscribe(%q): %v", topic, err)
	}
	return handle
}

func TestBasicPublishSubscribe(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var received TestEvent
	var wg sync.WaitGroup
	wg.Add(1)

	handle := mustSubscribe(t, bus, "test.topic", func(ctx context.Context, event TestEvent) error {
		received = event
		wg.Done()
		return nil
	})
	defer handle.Unsubscribe()

	if err := bus.Publish("test.topic", TestEvent{ID: "test1", Value: 42}); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	wg.Wait()

	if received.ID != "test1" || received.Value != 42 {
		t.Errorf("Expected ID='test1', Value=42, got ID='%s', Value=%d", received.ID, received.Value)
	}
}

func TestPriorityHandling(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var executionOrder []string
	var mu sync.Mutex
	record := func(name string) Handler[TestEvent] {
		return func(ctx context.Context, event TestEvent) error {
			mu.Lock()
			executionOrder = append(executionOrder, name)
			mu.Unlock()
			return nil
		}
	}

	mustSubscribe(t, bus, "priority.test", record("low"), HandlerPriority(PriorityLow))
	mustSubscribe(t, bus, "priority.test", record("high"), HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "priority.test", record("normal"), HandlerPriority(PriorityNormal))
	mustSubscribe(t, bus, "priority.test", record("critical"), HandlerPriority(PriorityCritical))

	bus.Publish("priority.test", TestEvent{ID: "priority", Value: 1})
	bus.WaitAsync()

	expected := []string{"critical", "high", "normal", "low"}
	mu.Lock()
	defer mu.Unlock()

	if len(executionOrder) != len(expected) {
		t.Fatalf("Expected %d handlers, got %d", len(expected), len(executionOrder))
	}

	for i, exp := range expected {
		if executionOrder[i] != exp {
			t.Errorf("Expected order[%d]='%s', got '%s'", i, exp, executionOrder[i])
		}
	}
}

func TestEventFiltering(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var filteredEvents []TestEvent
	var allEvents []TestEvent
	var mu sync.Mutex

	// Filter: only process events with Value > 10
	mustSubscribe(t, bus, "filter.test", func(ctx context.Context, event TestEvent) error {
		mu.Lock()
		filteredEvents = append(filteredEvents, event)
		mu.Unlock()
		return nil
	}, HandlerFilter(func(topic string, event TestEvent) bool {
		return event.Value > 10
	}))

	// Process all events
	mustSubscribe(t, bus, "filter.test", func(ctx context.Context, event TestEvent) error {
		mu.Lock()
		allEvents = append(allEvents, event)
		mu.Unlock()
		return nil
	})

	// Publish test events
	testEvents := []TestEvent{
		{ID: "e1", Value: 5},  // Should not be processed by filter
		{ID: "e2", Value: 15}, // Should be processed by filter
		{ID: "e3", Value: 3},  // Should not be processed by filter
		{ID: "e4", Value: 25}, // Should be processed by filter
	}

	for _, event := range testEvents {
		bus.Publish("filter.test", event)
	}
	bus.WaitAsync()

	mu.Lock()
	defer mu.Unlock()

	if len(allEvents) != 4 {
		t.Errorf("Expected 4 events in allEvents, got %d", len(allEvents))
	}

	if len(filteredEvents) != 2 {
		t.Errorf("Expected 2 events in filteredEvents, got %d", len(filteredEvents))
	}

	// Verify filtered events
	for _, event := range filteredEvents {
		if event.Value <= 10 {
			t.Errorf("Filtered event should have Value > 10, got %d", event.Value)
		}
	}
}

func TestFilterTypeMismatchIsRejected(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	handle, err := bus.Subscribe("filter.mismatch", discard[TestEvent],
		HandlerFilter(func(topic string, event string) bool { return true }))
	if err == nil {
		t.Fatal("Expected an error for a filter with the wrong event type")
	}
	if handle != nil {
		t.Fatal("Expected a nil handle for a rejected subscription")
	}
}

func TestContextCancellation(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var processedCount int32

	handle := mustSubscribe(t, bus, "context.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processedCount, 1)
		return nil
	}, HandlerContext(ctx))
	defer handle.Unsubscribe()

	// Publish first event (should be processed)
	bus.Publish("context.test", TestEvent{ID: "before_cancel", Value: 1})

	// Cancel context
	cancel()

	// Wait briefly to ensure cancellation takes effect
	time.Sleep(10 * time.Millisecond)

	// Publish second event (should be skipped)
	bus.Publish("context.test", TestEvent{ID: "after_cancel", Value: 2})

	bus.WaitAsync()

	// Should only process one event
	if count := atomic.LoadInt32(&processedCount); count != 1 {
		t.Errorf("Expected 1 processed event, got %d", count)
	}
}

func TestPanicIsRecoveredAndReported(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var errorCount int32
	var normalCount int32

	// Set error handler
	bus.SetErrorHandler(func(err *EventError) {
		atomic.AddInt32(&errorCount, 1)
	})

	// Subscribe a handler that will panic
	mustSubscribe(t, bus, "error.test", func(ctx context.Context, event TestEvent) error {
		if event.Value < 0 {
			panic("negative value")
		}
		atomic.AddInt32(&normalCount, 1)
		return nil
	})

	// Publish normal event
	if err := bus.Publish("error.test", TestEvent{ID: "normal", Value: 5}); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}

	// Publish event that will cause panic: the panic is recovered, reported,
	// and joined into the publish error.
	if err := bus.Publish("error.test", TestEvent{ID: "panic", Value: -1}); err == nil {
		t.Fatal("Expected the recovered panic in the publish error")
	}

	bus.WaitAsync()

	if count := atomic.LoadInt32(&normalCount); count != 1 {
		t.Errorf("Expected 1 normal event, got %d", count)
	}

	if count := atomic.LoadInt32(&errorCount); count != 1 {
		t.Errorf("Expected 1 error, got %d", count)
	}
}

func TestPanicErrorIsDetectable(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()
	bus.SetLogger(NewNoOpLogger())

	reported := make(chan error, 1)
	bus.SetErrorHandler(func(err *EventError) {
		reported <- err.Err
	})

	mustSubscribe(t, bus, "panic.detect", func(ctx context.Context, event TestEvent) error {
		panic("kaboom")
	})
	mustSubscribe(t, bus, "error.detect", func(ctx context.Context, event TestEvent) error {
		return errors.New("ordinary failure")
	})

	err := bus.Publish("panic.detect", TestEvent{ID: "p"})
	var pe *PanicError
	if !errors.As(err, &pe) || pe.Value != "kaboom" {
		t.Fatalf("Expected a PanicError carrying the panic value in the publish error, got %v", err)
	}
	if !errors.As(<-reported, &pe) {
		t.Fatal("Expected the ErrorHandler to receive a PanicError")
	}

	err = bus.Publish("error.detect", TestEvent{ID: "e"})
	if errors.As(err, &pe) {
		t.Fatal("An ordinary handler error must not be a PanicError")
	}
}

func TestHandlerErrorsAreJoinedAndDispatchContinues(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	errFirst := errors.New("first failure")
	errSecond := errors.New("second failure")
	var reported int32
	var thirdRan int32

	bus.SetErrorHandler(func(err *EventError) {
		atomic.AddInt32(&reported, 1)
	})

	mustSubscribe(t, bus, "errors.join", func(ctx context.Context, event TestEvent) error {
		return errFirst
	}, HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "errors.join", func(ctx context.Context, event TestEvent) error {
		return errSecond
	}, HandlerPriority(PriorityNormal))
	mustSubscribe(t, bus, "errors.join", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&thirdRan, 1)
		return nil
	}, HandlerPriority(PriorityLow))

	err := bus.Publish("errors.join", TestEvent{ID: "join", Value: 1})
	if !errors.Is(err, errFirst) || !errors.Is(err, errSecond) {
		t.Fatalf("Expected both handler errors in the joined publish error, got %v", err)
	}
	if atomic.LoadInt32(&thirdRan) != 1 {
		t.Fatal("Expected dispatch to continue past failing handlers")
	}
	if atomic.LoadInt32(&reported) != 2 {
		t.Fatalf("Expected 2 errors reported to the ErrorHandler, got %d", reported)
	}

	_, _, failed, _ := bus.GetMetrics().GetStats()
	if failed != 2 {
		t.Fatalf("Expected 2 failed deliveries in metrics, got %d", failed)
	}
}

func TestAsyncHandlerErrorGoesToErrorHandler(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	errAsync := errors.New("async failure")
	reported := make(chan error, 1)
	bus.SetErrorHandler(func(err *EventError) {
		reported <- err.Err
	})

	mustSubscribe(t, bus, "errors.async", func(ctx context.Context, event TestEvent) error {
		return errAsync
	}, HandlerAsync(false))

	// The async handler's failure is not part of the publish error.
	if err := bus.Publish("errors.async", TestEvent{ID: "async", Value: 1}); err != nil {
		t.Fatalf("Expected no publish error for an async handler failure, got %v", err)
	}
	bus.WaitAsync()

	select {
	case err := <-reported:
		if !errors.Is(err, errAsync) {
			t.Fatalf("Expected the async handler error, got %v", err)
		}
	default:
		t.Fatal("Expected the async handler error to reach the ErrorHandler")
	}
}

func TestMetrics(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	// Subscribe handlers
	handle1 := mustSubscribe(t, bus, "metrics.test", discard[TestEvent])
	handle2 := mustSubscribe(t, bus, "metrics.test", discard[TestEvent])

	defer func() {
		handle1.Unsubscribe()
		handle2.Unsubscribe()
	}()

	// Check initial subscriber count
	if count := bus.GetSubscriberCount("metrics.test"); count != 2 {
		t.Errorf("Expected 2 subscribers, got %d", count)
	}

	// Publish events
	for i := 0; i < 3; i++ {
		bus.Publish("metrics.test", TestEvent{ID: fmt.Sprintf("event%d", i), Value: i})
	}

	bus.WaitAsync()

	// Check metrics
	metrics := bus.GetMetrics()
	published, processed, failed, subscribers := metrics.GetStats()

	if published < 3 {
		t.Errorf("Expected at least 3 published events, got %d", published)
	}

	if processed < 6 { // 3 events * 2 handlers
		t.Errorf("Expected at least 6 processed events, got %d", processed)
	}

	if failed != 0 {
		t.Errorf("Expected 0 failed events, got %d", failed)
	}

	if subscribers != 2 {
		t.Errorf("Expected 2 active subscribers, got %d", subscribers)
	}
}

func TestMiddleware(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var middlewareCalled int32
	var handlerCalled int32

	// Add middleware
	bus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		atomic.AddInt32(&middlewareCalled, 1)
		next()
		return nil
	})

	// Subscribe handler
	mustSubscribe(t, bus, "middleware.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&handlerCalled, 1)
		return nil
	})

	// Publish event
	bus.Publish("middleware.test", TestEvent{ID: "test", Value: 1})
	bus.WaitAsync()

	if count := atomic.LoadInt32(&middlewareCalled); count != 1 {
		t.Errorf("Expected middleware to be called 1 time, got %d", count)
	}

	if count := atomic.LoadInt32(&handlerCalled); count != 1 {
		t.Errorf("Expected handler to be called 1 time, got %d", count)
	}
}

func TestMiddlewareCanSkipHandlers(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var handlerCalled int32

	bus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		return nil
	})
	mustSubscribe(t, bus, "middleware.skip.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&handlerCalled, 1)
		return nil
	})

	bus.Publish("middleware.skip.test", TestEvent{ID: "test", Value: 1})

	if count := atomic.LoadInt32(&handlerCalled); count != 0 {
		t.Errorf("Expected middleware to skip handler, got %d calls", count)
	}
}

func TestMiddlewareNextIsIdempotent(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var handlerCalled int32
	bus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		next()
		next()
		return nil
	})
	mustSubscribe(t, bus, "middleware.next.once", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&handlerCalled, 1)
		return nil
	})

	bus.Publish("middleware.next.once", TestEvent{ID: "test", Value: 1})

	if count := atomic.LoadInt32(&handlerCalled); count != 1 {
		t.Fatalf("Expected handler to run once, got %d", count)
	}
}

func TestMiddlewareNextIsConcurrentSafe(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var handlerCalled int32
	bus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				next()
			}()
		}
		wg.Wait()
		return nil
	})
	mustSubscribe(t, bus, "middleware.next.concurrent", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&handlerCalled, 1)
		return nil
	})

	bus.Publish("middleware.next.concurrent", TestEvent{ID: "test", Value: 1})

	if count := atomic.LoadInt32(&handlerCalled); count != 1 {
		t.Fatalf("Expected handler to run once, got %d", count)
	}
}

func TestPublishDoesNotHoldBusLockDuringHandler(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	done := make(chan struct{})
	mustSubscribe(t, bus, "lock.test", func(ctx context.Context, event TestEvent) error {
		handle, err := bus.Subscribe("lock.test.extra", discard[TestEvent])
		if err == nil {
			handle.Unsubscribe()
		}
		close(done)
		return nil
	})

	go bus.Publish("lock.test", TestEvent{ID: "lock", Value: 1})

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("handler could not call bus APIs while publish was running")
	}
}

func TestPublishWithTimeout(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	// Subscribe a synchronous handler that will block for a long time
	mustSubscribe(t, bus, "timeout.test", func(ctx context.Context, event TestEvent) error {
		// Simulate blocking operation
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	// Test timeout publish (timeout shorter than processing time)
	start := time.Now()
	err := bus.PublishWithTimeout("timeout.test", TestEvent{ID: "timeout", Value: 1}, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Expected context.DeadlineExceeded in the publish error, got %v", err)
	}

	// Verify it returns around timeout time
	if elapsed > 80*time.Millisecond {
		t.Errorf("Expected timeout around 50ms, but took %v", elapsed)
	}
	bus.WaitAsync()
}

func TestHandlerTimeoutOption(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	mustSubscribe(t, bus, "handler.timeout", func(ctx context.Context, event TestEvent) error {
		time.Sleep(100 * time.Millisecond)
		return nil
	}, HandlerTimeout(20*time.Millisecond))

	start := time.Now()
	err := bus.PublishWithContext(context.Background(), "handler.timeout", TestEvent{ID: "timeout", Value: 1})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("Expected handler timeout error")
	}
	if elapsed > 80*time.Millisecond {
		t.Fatalf("Expected handler timeout around 20ms, took %v", elapsed)
	}

	_, _, failed, _ := bus.GetMetrics().GetStats()
	if failed != 1 {
		t.Fatalf("Expected 1 failed handler, got %d", failed)
	}
	bus.WaitAsync()
}

func TestHandlerTimeoutCancelsHandlerContext(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	canceled := make(chan struct{})
	mustSubscribe(t, bus, "handler.timeout.ctx", func(ctx context.Context, event TestEvent) error {
		select {
		case <-ctx.Done():
			close(canceled)
			return ctx.Err()
		case <-time.After(time.Second):
			return fmt.Errorf("handler context was never canceled")
		}
	}, HandlerTimeout(10*time.Millisecond))

	if err := bus.PublishWithContext(context.Background(), "handler.timeout.ctx", TestEvent{ID: "ctx", Value: 1}); err == nil {
		t.Fatal("Expected handler timeout error")
	}

	bus.WaitAsync()
	select {
	case <-canceled:
	default:
		t.Fatal("Expected the handler context to be canceled at the timeout")
	}
}

func TestHandlerTimeoutGoroutineIsWaited(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	done := make(chan struct{})
	mustSubscribe(t, bus, "handler.timeout.wait", func(ctx context.Context, event TestEvent) error {
		time.Sleep(60 * time.Millisecond)
		close(done)
		return nil
	}, HandlerTimeout(10*time.Millisecond))

	if err := bus.PublishWithContext(context.Background(), "handler.timeout.wait", TestEvent{ID: "timeout", Value: 1}); err == nil {
		t.Fatal("Expected handler timeout error")
	}

	bus.WaitAsync()
	select {
	case <-done:
	default:
		t.Fatal("Expected WaitAsync to wait for timed-out handler goroutine")
	}
}

func TestHandlerTimeoutCancelsConcurrencyWait(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	block := make(chan struct{})
	mustSubscribe(t, bus, "handler.timeout.concurrency", func(ctx context.Context, event TestEvent) error {
		if event.ID == "first" {
			<-block
		}
		return nil
	}, HandlerTimeout(10*time.Millisecond), HandlerMaxConcurrency(1))

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- bus.PublishWithContext(context.Background(), "handler.timeout.concurrency", TestEvent{ID: "first", Value: 1})
	}()
	if err := <-firstDone; err == nil {
		t.Fatal("Expected first publish to time out")
	}

	if err := bus.PublishWithContext(context.Background(), "handler.timeout.concurrency", TestEvent{ID: "second", Value: 2}); err == nil {
		t.Fatal("Expected second publish to time out waiting for concurrency slot")
	}

	waited := make(chan struct{})
	go func() {
		bus.WaitAsync()
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("WaitAsync should still wait for the blocked first handler")
	case <-time.After(20 * time.Millisecond):
	}

	close(block)
	select {
	case <-waited:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected WaitAsync to finish after blocked handler is released")
	}
}

func TestPublishWithNilContextUsesBackground(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var called int32
	mustSubscribe(t, bus, "nil.publish.context", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&called, 1)
		return nil
	})

	if err := bus.PublishWithContext(nil, "nil.publish.context", TestEvent{ID: "nil", Value: 1}); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Fatal("Expected handler to run with nil publish context")
	}
}

func TestRecoverAndStopOption(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var secondCalled int32
	mustSubscribe(t, bus, "recover.stop", func(ctx context.Context, event TestEvent) error {
		panic("stop")
	}, HandlerRecoverPolicy(RecoverAndStop), HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "recover.stop", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&secondCalled, 1)
		return nil
	}, HandlerPriority(PriorityLow))

	err := bus.PublishWithContext(context.Background(), "recover.stop", TestEvent{ID: "panic", Value: 1})
	if err == nil {
		t.Fatal("Expected recovered panic to stop publish")
	}
	if atomic.LoadInt32(&secondCalled) != 0 {
		t.Fatal("Expected lower priority handler to be skipped")
	}
}

func TestReturnedErrorDoesNotStopDispatchUnderRecoverAndStop(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var secondCalled int32
	mustSubscribe(t, bus, "recover.stop.error", func(ctx context.Context, event TestEvent) error {
		return errors.New("business failure")
	}, HandlerRecoverPolicy(RecoverAndStop), HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "recover.stop.error", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&secondCalled, 1)
		return nil
	}, HandlerPriority(PriorityLow))

	if err := bus.Publish("recover.stop.error", TestEvent{ID: "err", Value: 1}); err == nil {
		t.Fatal("Expected the handler error in the publish error")
	}
	if atomic.LoadInt32(&secondCalled) != 1 {
		t.Fatal("RecoverAndStop governs panics; a returned error must not stop dispatch")
	}
}

func TestHandlerMaxConcurrencyOption(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var current int32
	var maxSeen int32
	mustSubscribe(t, bus, "max.concurrency", func(ctx context.Context, event TestEvent) error {
		now := atomic.AddInt32(&current, 1)
		for {
			seen := atomic.LoadInt32(&maxSeen)
			if now <= seen || atomic.CompareAndSwapInt32(&maxSeen, seen, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		return nil
	}, HandlerAsync(false), HandlerMaxConcurrency(1))

	for i := 0; i < 3; i++ {
		bus.Publish("max.concurrency", TestEvent{ID: fmt.Sprintf("%d", i), Value: i})
	}
	bus.WaitAsync()

	if atomic.LoadInt32(&maxSeen) != 1 {
		t.Fatalf("Expected one concurrent execution for the handler, got %d", maxSeen)
	}
}

func TestHandleUnsubscribe(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var count int32

	handle := mustSubscribe(t, bus, "unsubscribe.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	// Publish first event
	bus.Publish("unsubscribe.test", TestEvent{ID: "first", Value: 1})

	// Unsubscribe
	if err := handle.Unsubscribe(); err != nil {
		t.Errorf("Unexpected error during unsubscribe: %v", err)
	}

	// Publish second event (should not be processed)
	bus.Publish("unsubscribe.test", TestEvent{ID: "second", Value: 2})

	bus.WaitAsync()

	if finalCount := atomic.LoadInt32(&count); finalCount != 1 {
		t.Errorf("Expected 1 processed event after unsubscribe, got %d", finalCount)
	}

	// Check handle status
	if handle.IsActive() {
		t.Error("Handle should not be active after unsubscribe")
	}

	// Unsubscribing again should return error
	if err := handle.Unsubscribe(); err == nil {
		t.Error("Expected error when unsubscribing already unsubscribed handle")
	}
}

func TestBusClose(t *testing.T) {
	bus := NewTyped[TestEvent]()

	var processed int32

	// Subscribe handler
	mustSubscribe(t, bus, "close.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		return nil
	})

	// Publish event
	bus.Publish("close.test", TestEvent{ID: "before_close", Value: 1})

	// Close bus
	if err := bus.Close(); err != nil {
		t.Errorf("Unexpected error during close: %v", err)
	}

	// Try to publish again (should fail)
	if err := bus.PublishWithContext(context.Background(), "close.test", TestEvent{ID: "after_close", Value: 2}); err == nil {
		t.Error("Expected error when publishing to closed bus")
	}

	// Try to subscribe (should fail)
	if _, err := bus.Subscribe("close.test", discard[TestEvent]); err == nil {
		t.Error("Expected error when subscribing to closed bus")
	}

	// Closing again should return error
	if err := bus.Close(); err == nil {
		t.Error("Expected error when closing already closed bus")
	}
}

func TestBusCloseDoesNotDeadlockWhenAsyncHandlerUsesBus(t *testing.T) {
	bus := NewTyped[TestEvent]()
	started := make(chan struct{})
	release := make(chan struct{})

	mustSubscribe(t, bus, "close.deadlock", func(ctx context.Context, event TestEvent) error {
		close(started)
		<-release
		_ = bus.GetSubscriberCount("close.deadlock")
		return nil
	}, HandlerAsync(false))

	bus.Publish("close.deadlock", TestEvent{ID: "close", Value: 1})
	<-started

	done := make(chan error, 1)
	go func() {
		done <- bus.Close()
	}()

	close(release)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Unexpected close error: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close deadlocked while waiting for async handler")
	}
}

func TestBusCloseClearsSubscriberMetrics(t *testing.T) {
	bus := NewTyped[TestEvent]()

	mustSubscribe(t, bus, "close.metrics", discard[TestEvent])
	mustSubscribe(t, bus, "close.metrics", discard[TestEvent])

	if err := bus.Close(); err != nil {
		t.Fatalf("Unexpected close error: %v", err)
	}

	_, _, _, subscribers := bus.GetMetrics().GetStats()
	if subscribers != 0 {
		t.Fatalf("Expected 0 active subscribers after close, got %d", subscribers)
	}
}

func TestPublishDoesNotStartAsyncHandlerAfterClose(t *testing.T) {
	bus := NewTyped[TestEvent]()
	ready := make(chan struct{})
	release := make(chan struct{})
	var processed int32

	bus.AddMiddleware(func(topic string, event any, next func()) error {
		close(ready)
		<-release
		next()
		return nil
	})
	mustSubscribe(t, bus, "close.publish", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}, HandlerAsync(false))

	published := make(chan error, 1)
	go func() {
		published <- bus.PublishWithContext(context.Background(), "close.publish", TestEvent{ID: "close", Value: 1})
	}()

	<-ready
	if err := bus.Close(); err != nil {
		t.Fatalf("Unexpected close error: %v", err)
	}
	close(release)

	if err := <-published; err == nil {
		t.Fatal("Expected publish error after bus closed")
	}
	bus.WaitAsync()
	if count := atomic.LoadInt32(&processed); count != 0 {
		t.Fatalf("Expected async handler not to run after close, got %d calls", count)
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var processed int64
	var wg sync.WaitGroup

	// Create multiple subscribers
	numSubscribers := 10
	for i := 0; i < numSubscribers; i++ {
		mustSubscribe(t, bus, "concurrent.test", func(ctx context.Context, event TestEvent) error {
			atomic.AddInt64(&processed, 1)
			return nil
		})
	}

	// Concurrent event publishing
	numPublishers := 5
	eventsPerPublisher := 100

	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			for j := 0; j < eventsPerPublisher; j++ {
				bus.Publish("concurrent.test", TestEvent{
					ID:    fmt.Sprintf("p%d-e%d", publisherID, j),
					Value: j,
				})
			}
		}(i)
	}

	wg.Wait()
	bus.WaitAsync()

	expectedProcessed := int64(numPublishers * eventsPerPublisher * numSubscribers)
	actualProcessed := atomic.LoadInt64(&processed)

	if actualProcessed != expectedProcessed {
		t.Errorf("Expected %d processed events, got %d", expectedProcessed, actualProcessed)
	}
}

// TestNew tests the New() function for compatibility
func TestNew(t *testing.T) {
	bus := New()
	defer bus.Close()

	var received interface{}
	var wg sync.WaitGroup
	wg.Add(1)

	handle := mustSubscribe(t, bus, "new.test", func(ctx context.Context, event any) error {
		received = event
		wg.Done()
		return nil
	})
	defer handle.Unsubscribe()

	testData := map[string]interface{}{"key": "value", "number": 42}
	bus.Publish("new.test", testData)
	wg.Wait()

	if received == nil {
		t.Error("Expected to receive event, got nil")
	}

	receivedMap, ok := received.(map[string]interface{})
	if !ok {
		t.Error("Expected received event to be map[string]interface{}")
	} else {
		if receivedMap["key"] != "value" || receivedMap["number"] != 42 {
			t.Errorf("Expected received data to match sent data, got %v", receivedMap)
		}
	}
}

// TestSubscribeAsync tests asynchronous subscription
func TestSubscribeAsync(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var processed int32
	var wg sync.WaitGroup

	// Test non-transactional async
	mustSubscribe(t, bus, "async.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		wg.Done()
		return nil
	}, HandlerAsync(false))

	wg.Add(1)
	bus.Publish("async.test", TestEvent{ID: "async1", Value: 1})
	wg.Wait()

	if count := atomic.LoadInt32(&processed); count != 1 {
		t.Errorf("Expected 1 processed event, got %d", count)
	}

	// Test transactional async
	mustSubscribe(t, bus, "async.transactional", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		wg.Done()
		return nil
	}, HandlerAsync(true))

	wg.Add(1)
	bus.Publish("async.transactional", TestEvent{ID: "async2", Value: 2})
	wg.Wait()

	if count := atomic.LoadInt32(&processed); count != 2 {
		t.Errorf("Expected 2 processed events, got %d", count)
	}
}

// TestSubscribeOnce tests one-time subscription
func TestSubscribeOnce(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var processed int32

	mustSubscribe(t, bus, "once.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		return nil
	}, HandlerOnce())

	// Publish first event (should be processed)
	bus.Publish("once.test", TestEvent{ID: "once1", Value: 1})
	bus.WaitAsync()

	// Publish second event (should NOT be processed)
	bus.Publish("once.test", TestEvent{ID: "once2", Value: 2})
	bus.WaitAsync()

	if count := atomic.LoadInt32(&processed); count != 1 {
		t.Errorf("Expected 1 processed event (once only), got %d", count)
	}

	// Verify no callback exists for the topic anymore
	if bus.HasCallback("once.test") {
		t.Error("Expected no callback after HandlerOnce execution")
	}
}

func TestMultipleSubscribeOnceSameTopic(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var first int32
	var second int32

	mustSubscribe(t, bus, "once.multi.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&first, 1)
		return nil
	}, HandlerOnce())
	mustSubscribe(t, bus, "once.multi.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&second, 1)
		return nil
	}, HandlerOnce())

	bus.Publish("once.multi.test", TestEvent{ID: "once", Value: 1})
	bus.Publish("once.multi.test", TestEvent{ID: "again", Value: 2})

	if atomic.LoadInt32(&first) != 1 || atomic.LoadInt32(&second) != 1 {
		t.Fatalf("Expected both once handlers to run once, got first=%d second=%d", first, second)
	}
	if bus.HasCallback("once.multi.test") {
		t.Error("Expected no callback after once handlers run")
	}
}

func TestWildcardSubscription(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var calls []string
	var mu sync.Mutex

	mustSubscribe(t, bus, "*", func(ctx context.Context, event TestEvent) error {
		mu.Lock()
		calls = append(calls, "wildcard:"+event.ID)
		mu.Unlock()
		return nil
	}, HandlerPriority(PriorityHigh))
	mustSubscribe(t, bus, "orders.created", func(ctx context.Context, event TestEvent) error {
		mu.Lock()
		calls = append(calls, "topic:"+event.ID)
		mu.Unlock()
		return nil
	})

	bus.Publish("orders.created", TestEvent{ID: "a", Value: 1})
	bus.Publish("users.created", TestEvent{ID: "b", Value: 2})

	want := []string{"wildcard:a", "topic:a", "wildcard:b"}
	if len(calls) != len(want) {
		t.Fatalf("Expected %d calls, got %d: %v", len(want), len(calls), calls)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("Expected calls[%d]=%q, got %q", i, want[i], calls[i])
		}
	}
}

func TestWildcardSubscribeOnce(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var count int32
	mustSubscribe(t, bus, "*", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&count, 1)
		return nil
	}, HandlerOnce())

	bus.Publish("first.topic", TestEvent{ID: "first", Value: 1})
	bus.Publish("second.topic", TestEvent{ID: "second", Value: 2})

	if atomic.LoadInt32(&count) != 1 {
		t.Fatalf("Expected wildcard once handler to run once, got %d", count)
	}
	if bus.HasCallback("*") {
		t.Fatal("Expected wildcard once handler to unsubscribe")
	}
}

// TestSubscribeOnceAsync tests one-time asynchronous subscription
func TestSubscribeOnceAsync(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var processed int32
	var wg sync.WaitGroup

	mustSubscribe(t, bus, "once.async.test", func(ctx context.Context, event TestEvent) error {
		atomic.AddInt32(&processed, 1)
		wg.Done()
		return nil
	}, HandlerOnce(), HandlerAsync(false))

	// Publish first event (should be processed)
	wg.Add(1)
	bus.Publish("once.async.test", TestEvent{ID: "once_async1", Value: 1})
	wg.Wait()

	// Publish second event (should NOT be processed)
	bus.Publish("once.async.test", TestEvent{ID: "once_async2", Value: 2})
	bus.WaitAsync()

	if count := atomic.LoadInt32(&processed); count != 1 {
		t.Errorf("Expected 1 processed event (once only), got %d", count)
	}

	// Verify no callback exists for the topic anymore
	if bus.HasCallback("once.async.test") {
		t.Error("Expected no callback after once async execution")
	}
}

// TestHasCallback tests the HasCallback function
func TestHasCallback(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	// Initially no callback
	if bus.HasCallback("callback.test") {
		t.Error("Expected no callback initially")
	}

	// Subscribe a handler
	handle := mustSubscribe(t, bus, "callback.test", discard[TestEvent])
	defer handle.Unsubscribe()

	// Now should have callback
	if !bus.HasCallback("callback.test") {
		t.Error("Expected callback after subscription")
	}

	// Unsubscribe
	handle.Unsubscribe()

	// Should not have callback anymore
	if bus.HasCallback("callback.test") {
		t.Error("Expected no callback after unsubscription")
	}

	// Test with non-existent topic
	if bus.HasCallback("non.existent.topic") {
		t.Error("Expected no callback for non-existent topic")
	}
}

// TestGetTopics tests the GetTopics function
func TestGetTopics(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	// Initially no topics
	topics := bus.GetTopics()
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(topics))
	}

	// Subscribe to multiple topics
	handle1 := mustSubscribe(t, bus, "topic1", discard[TestEvent])
	handle2 := mustSubscribe(t, bus, "topic2", discard[TestEvent])
	handle3 := mustSubscribe(t, bus, "topic3", discard[TestEvent])

	defer handle1.Unsubscribe()
	defer handle2.Unsubscribe()
	defer handle3.Unsubscribe()

	// Get topics
	topics = bus.GetTopics()
	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}

	// Check if all topics are present
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}

	expectedTopics := []string{"topic1", "topic2", "topic3"}
	for _, expected := range expectedTopics {
		if !topicMap[expected] {
			t.Errorf("Expected topic '%s' to be present", expected)
		}
	}

	handle1.Unsubscribe()
	handle2.Unsubscribe()
	handle3.Unsubscribe()

	if topics := bus.GetTopics(); len(topics) != 0 {
		t.Errorf("Expected no topics after all handlers unsubscribe, got %v", topics)
	}
}

// TestGetSubscriberCount tests the GetSubscriberCount function
func TestGetSubscriberCount(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	topic := "subscriber.count.test"

	// Initially no subscribers
	if count := bus.GetSubscriberCount(topic); count != 0 {
		t.Errorf("Expected 0 subscribers initially, got %d", count)
	}

	// Add subscribers
	handle1 := mustSubscribe(t, bus, topic, discard[TestEvent])
	handle2 := mustSubscribe(t, bus, topic, discard[TestEvent])
	handle3 := mustSubscribe(t, bus, topic, discard[TestEvent])

	defer handle1.Unsubscribe()
	defer handle2.Unsubscribe()
	defer handle3.Unsubscribe()

	// Should have 3 subscribers
	if count := bus.GetSubscriberCount(topic); count != 3 {
		t.Errorf("Expected 3 subscribers, got %d", count)
	}

	// Unsubscribe one
	handle1.Unsubscribe()

	// Should have 2 subscribers
	if count := bus.GetSubscriberCount(topic); count != 2 {
		t.Errorf("Expected 2 subscribers after unsubscribe, got %d", count)
	}

	// Test non-existent topic
	if count := bus.GetSubscriberCount("non.existent.topic"); count != 0 {
		t.Errorf("Expected 0 subscribers for non-existent topic, got %d", count)
	}
}

// TestEventError tests the EventError type
func TestEventError(t *testing.T) {
	err := &EventError{
		Topic: "test.topic",
		Event: TestEvent{ID: "test", Value: 42},
		Err:   fmt.Errorf("test error"),
	}

	errorString := err.Error()
	if !strings.Contains(errorString, "event error in topic 'test.topic'") {
		t.Errorf("Expected error string to contain the topic, got '%s'", errorString)
	}
	if !strings.Contains(errorString, "test error") {
		t.Errorf("Expected error string to contain 'test error', got '%s'", errorString)
	}
}

// TestAsyncErrorHandling tests error handling in async operations
func TestAsyncErrorHandling(t *testing.T) {
	bus := NewTyped[TestEvent]()
	defer bus.Close()

	var errorCount int32
	var wg sync.WaitGroup

	// Set error handler
	bus.SetErrorHandler(func(err *EventError) {
		atomic.AddInt32(&errorCount, 1)
		wg.Done()
	})

	// Subscribe async handler that will panic
	mustSubscribe(t, bus, "async.error.test", func(ctx context.Context, event TestEvent) error {
		if event.Value < 0 {
			panic("negative value in async handler")
		}
		return nil
	}, HandlerAsync(false))

	// Publish event that will cause panic
	wg.Add(1)
	bus.Publish("async.error.test", TestEvent{ID: "error", Value: -1})
	wg.Wait()

	if count := atomic.LoadInt32(&errorCount); count != 1 {
		t.Errorf("Expected 1 error to be handled, got %d", count)
	}
}

// TestClosedBusOperations tests operations on a closed bus
func TestClosedBusOperations(t *testing.T) {
	bus := NewTyped[TestEvent]()

	// Close the bus
	if err := bus.Close(); err != nil {
		t.Errorf("Unexpected error closing bus: %v", err)
	}

	// Every subscription shape is rejected with an error and a nil handle.
	cases := [][]HandlerOption{
		nil,
		{HandlerAsync(false)},
		{HandlerOnce()},
		{HandlerOnce(), HandlerAsync(false)},
		{HandlerPriority(PriorityHigh), HandlerSerial()},
	}
	for i, opts := range cases {
		handle, err := bus.Subscribe("closed.test", discard[TestEvent], opts...)
		if err == nil {
			t.Errorf("case %d: expected error when subscribing to closed bus", i)
		}
		if handle != nil {
			t.Errorf("case %d: expected nil handle when subscribing to closed bus", i)
		}
	}
}

func TestNilInputsAreIgnoredOrRejected(t *testing.T) {
	bus := NewTyped[TestEvent](
		nil,
		WithMetrics[TestEvent](nil),
		WithMiddleware[TestEvent](nil),
	)
	defer bus.Close()

	if bus.GetMetrics() == nil {
		t.Fatal("Expected default metrics when nil metrics option is used")
	}

	bus.AddMiddleware(nil)
	if _, err := bus.Subscribe("nil.fn", nil); err == nil {
		t.Fatal("Expected error for nil handler")
	}
	if handle, err := bus.Subscribe("nil.ctx", discard[TestEvent], HandlerContext(nil)); err != nil || handle == nil {
		t.Fatalf("Expected nil context to default to background context, got handle=%v err=%v", handle, err)
	}

	bus.Publish("nil.ctx", TestEvent{ID: "ok", Value: 1})
}
