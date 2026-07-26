//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/townbell/bus"
)

// UserEvent represents a user action event
type UserEvent struct {
	UserID    string                 `json:"user_id"`
	Action    string                 `json:"action"`
	Timestamp time.Time              `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
}

func main() {
	// Create type-safe event bus
	eventBus := bus.NewTyped[UserEvent]()
	defer eventBus.Close()

	// Set up error handler
	eventBus.SetErrorHandler(func(err *bus.EventError) {
		log.Printf("Event processing error: %v", err)
	})

	// Add middleware - logging
	eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		log.Printf("Publishing event to topic '%s': %+v", topic, event)
		start := time.Now()
		next()
		log.Printf("Event processed in %v", time.Since(start))
		return nil
	})

	// Example 1: Basic subscription and publishing
	fmt.Println("=== Example 1: Basic Subscription and Publishing ===")
	basicExample(eventBus)

	// Example 2: Priority-based processing
	fmt.Println("\n=== Example 2: Priority-based Processing ===")
	priorityExample(eventBus)

	// Example 3: Event filtering
	fmt.Println("\n=== Example 3: Event Filtering ===")
	filterExample(eventBus)

	// Example 4: Context cancellation
	fmt.Println("\n=== Example 4: Context Cancellation ===")
	contextExample(eventBus)

	// Example 5: Monitoring and metrics
	fmt.Println("\n=== Example 5: Monitoring and Metrics ===")
	metricsExample(eventBus)

	// Example 6: Handler errors
	fmt.Println("\n=== Example 6: Handler Errors ===")
	errorHandlingExample(eventBus)

	// Example 7: Timeout publishing
	fmt.Println("\n=== Example 7: Timeout Publishing ===")
	timeoutExample(eventBus)

	// Wait for all async processing to complete
	eventBus.WaitAsync()
}

func basicExample(eventBus bus.Bus[UserEvent]) {
	// Subscribe to user events
	handle, err := eventBus.Subscribe("user.login", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("User login: %s at %s\n", event.UserID, event.Timestamp.Format("15:04:05"))
		return nil
	})
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	defer handle.Unsubscribe()

	// Publish event
	eventBus.Publish("user.login", UserEvent{
		UserID:    "user123",
		Action:    "login",
		Timestamp: time.Now(),
		Data:      map[string]interface{}{"ip": "192.168.1.1"},
	})
}

func priorityExample(eventBus bus.Bus[UserEvent]) {
	// High priority handler - security check
	securityHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("🔒 Security check: User %s performing %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerPriority(bus.PriorityCritical))

	// Normal priority handler - logging
	logHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("📝 Logging: User %s performed %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerPriority(bus.PriorityNormal))

	// Low priority handler - analytics
	analyticsHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("📊 Analytics: User %s performed %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerPriority(bus.PriorityLow))

	defer func() {
		securityHandle.Unsubscribe()
		logHandle.Unsubscribe()
		analyticsHandle.Unsubscribe()
	}()

	// Publish event and observe execution order
	eventBus.Publish("user.action", UserEvent{
		UserID:    "user456",
		Action:    "delete_file",
		Timestamp: time.Now(),
	})
}

func filterExample(eventBus bus.Bus[UserEvent]) {
	// Only process admin user events
	adminHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("👑 Admin action: %s performed %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
		// Assume admin user IDs start with "admin_"
		return strings.HasPrefix(event.UserID, "admin_")
	}))

	// Only process sensitive operations
	sensitiveHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("⚠️  Sensitive operation alert: %s performed %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
		sensitiveActions := []string{"delete", "modify_permissions", "export_data"}
		for _, action := range sensitiveActions {
			if event.Action == action {
				return true
			}
		}
		return false
	}))

	defer func() {
		adminHandle.Unsubscribe()
		sensitiveHandle.Unsubscribe()
	}()

	// Test different types of events
	events := []UserEvent{
		{UserID: "admin_root", Action: "delete", Timestamp: time.Now()},
		{UserID: "user123", Action: "delete", Timestamp: time.Now()},
		{UserID: "admin_john", Action: "view", Timestamp: time.Now()},
		{UserID: "user456", Action: "view", Timestamp: time.Now()},
	}

	for _, event := range events {
		eventBus.Publish("user.action", event)
	}
}

func contextExample(eventBus bus.Bus[UserEvent]) {
	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	// Subscribe with context
	handle, _ := eventBus.Subscribe("user.session", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("Session event: %s - %s\n", event.UserID, event.Action)
		return nil
	}, bus.HandlerContext(ctx))
	defer handle.Unsubscribe()

	// Publish an event - should be processed
	eventBus.Publish("user.session", UserEvent{
		UserID:    "user789",
		Action:    "session_start",
		Timestamp: time.Now(),
	})

	// Cancel context
	cancel()

	// Brief wait to ensure cancellation takes effect
	time.Sleep(10 * time.Millisecond)

	// Publish another event - should be skipped
	fmt.Println("Publishing event after context cancellation...")
	eventBus.Publish("user.session", UserEvent{
		UserID:    "user789",
		Action:    "session_end",
		Timestamp: time.Now(),
	})
}

func metricsExample(eventBus bus.Bus[UserEvent]) {
	// Subscribe multiple handlers
	handle1, _ := eventBus.Subscribe("metrics.test", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("Handler 1 processing: %s\n", event.UserID)
		return nil
	})

	handle2, _ := eventBus.Subscribe("metrics.test", func(ctx context.Context, event UserEvent) error {
		fmt.Printf("Handler 2 processing: %s\n", event.UserID)
		return nil
	})

	defer func() {
		handle1.Unsubscribe()
		handle2.Unsubscribe()
	}()

	// Publish several events
	for i := 0; i < 3; i++ {
		eventBus.Publish("metrics.test", UserEvent{
			UserID:    fmt.Sprintf("user%d", i),
			Action:    "test",
			Timestamp: time.Now(),
		})
	}

	// Wait for async processing to complete
	eventBus.WaitAsync()

	// Get metrics
	metrics := eventBus.GetMetrics()
	published, processed, failed, subscribers := metrics.GetStats()

	fmt.Printf("📈 Metrics:\n")
	fmt.Printf("  - Published events: %d\n", published)
	fmt.Printf("  - Processed events: %d\n", processed)
	fmt.Printf("  - Failed events: %d\n", failed)
	fmt.Printf("  - Active subscribers: %d\n", subscribers)
	fmt.Printf("  - Topics list: %v\n", eventBus.GetTopics())
	fmt.Printf("  - metrics.test subscriber count: %d\n", eventBus.GetSubscriberCount("metrics.test"))
}

func errorHandlingExample(eventBus bus.Bus[UserEvent]) {
	// A handler can now report a business failure by returning an error; the
	// publish call returns the joined failures of the synchronous handlers.
	errorHandle, _ := eventBus.Subscribe("user.error", func(ctx context.Context, event UserEvent) error {
		if event.Action == "fail" {
			return fmt.Errorf("simulated business failure for %s", event.UserID)
		}
		fmt.Printf("Normal processing: %s\n", event.UserID)
		return nil
	})
	defer errorHandle.Unsubscribe()

	// Publish normal event
	if err := eventBus.Publish("user.error", UserEvent{
		UserID:    "user_normal",
		Action:    "normal",
		Timestamp: time.Now(),
	}); err != nil {
		fmt.Printf("Unexpected publish error: %v\n", err)
	}

	// Publish event whose handler fails; the error comes back from Publish
	// and is also reported to the ErrorHandler.
	fmt.Println("Publishing event whose handler fails...")
	if err := eventBus.Publish("user.error", UserEvent{
		UserID:    "user_fail",
		Action:    "fail",
		Timestamp: time.Now(),
	}); err != nil {
		fmt.Printf("Publish returned the handler failure: %v\n", err)
	}

	// Check error metrics
	metrics := eventBus.GetMetrics()
	_, _, failed, _ := metrics.GetStats()
	fmt.Printf("Failed events count: %d\n", failed)
}

func timeoutExample(eventBus bus.Bus[UserEvent]) {
	// Subscribe a slow handler with a per-handler timeout. The handler's
	// context is canceled when the timeout elapses, so it can stop early.
	handle, _ := eventBus.Subscribe("user.slow", func(ctx context.Context, event UserEvent) error {
		select {
		case <-time.After(2 * time.Second): // Simulate slow processing
			fmt.Printf("Slow processing completed: %s\n", event.UserID)
			return nil
		case <-ctx.Done():
			fmt.Printf("Slow processing canceled for %s: %v\n", event.UserID, ctx.Err())
			return ctx.Err()
		}
	}, bus.HandlerTimeout(500*time.Millisecond))
	defer handle.Unsubscribe()

	// Use timeout publishing
	err := eventBus.PublishWithTimeout("user.slow", UserEvent{
		UserID:    "user_timeout",
		Action:    "slow_operation",
		Timestamp: time.Now(),
	}, 1*time.Second)

	if err != nil {
		fmt.Printf("Publish timeout: %v\n", err)
	}
	eventBus.WaitAsync()
}
