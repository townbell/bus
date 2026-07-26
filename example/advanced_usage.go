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

// OrderEvent represents an order-related event
type OrderEvent struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
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

	// Example 6: Error handling
	fmt.Println("\n=== Example 6: Error Handling ===")
	errorHandlingExample(eventBus)

	// Example 7: Timeout publishing
	fmt.Println("\n=== Example 7: Timeout Publishing ===")
	timeoutExample(eventBus)

	// Wait for all async processing to complete
	eventBus.WaitAsync()
}

func basicExample(eventBus bus.Bus[UserEvent]) {
	// Subscribe to user events
	handle := eventBus.SubscribeWithHandle("user.login", func(event UserEvent) {
		fmt.Printf("User login: %s at %s\n", event.UserID, event.Timestamp.Format("15:04:05"))
	})
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
	securityHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
		fmt.Printf("🔒 Security check: User %s performing %s\n", event.UserID, event.Action)
	}, bus.PriorityCritical)

	// Normal priority handler - logging
	logHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
		fmt.Printf("📝 Logging: User %s performed %s\n", event.UserID, event.Action)
	}, bus.PriorityNormal)

	// Low priority handler - analytics
	analyticsHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
		fmt.Printf("📊 Analytics: User %s performed %s\n", event.UserID, event.Action)
	}, bus.PriorityLow)

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
	adminHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
		fmt.Printf("👑 Admin action: %s performed %s\n", event.UserID, event.Action)
	}, func(topic string, event UserEvent) bool {
		// Assume admin user IDs start with "admin_"
		return strings.HasPrefix(event.UserID, "admin_")
	})

	// Only process sensitive operations
	sensitiveHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
		fmt.Printf("⚠️  Sensitive operation alert: %s performed %s\n", event.UserID, event.Action)
	}, func(topic string, event UserEvent) bool {
		sensitiveActions := []string{"delete", "modify_permissions", "export_data"}
		for _, action := range sensitiveActions {
			if event.Action == action {
				return true
			}
		}
		return false
	})

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
	handle := eventBus.SubscribeWithContext(ctx, "user.session", func(event UserEvent) {
		fmt.Printf("Session event: %s - %s\n", event.UserID, event.Action)
	})
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
	handle1 := eventBus.SubscribeWithHandle("metrics.test", func(event UserEvent) {
		fmt.Printf("Handler 1 processing: %s\n", event.UserID)
	})

	handle2 := eventBus.SubscribeWithHandle("metrics.test", func(event UserEvent) {
		fmt.Printf("Handler 2 processing: %s\n", event.UserID)
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
	// Subscribe a handler that will panic
	panicHandle := eventBus.SubscribeWithHandle("user.error", func(event UserEvent) {
		if event.Action == "panic" {
			panic("simulating handler crash")
		}
		fmt.Printf("Normal processing: %s\n", event.UserID)
	})
	defer panicHandle.Unsubscribe()

	// Publish normal event
	eventBus.Publish("user.error", UserEvent{
		UserID:    "user_normal",
		Action:    "normal",
		Timestamp: time.Now(),
	})

	// Publish event that will cause panic
	fmt.Println("Publishing event that will cause panic...")
	eventBus.Publish("user.error", UserEvent{
		UserID:    "user_panic",
		Action:    "panic",
		Timestamp: time.Now(),
	})

	// Wait for processing to complete
	eventBus.WaitAsync()

	// Check error metrics
	metrics := eventBus.GetMetrics()
	_, _, failed, _ := metrics.GetStats()
	fmt.Printf("Failed events count: %d\n", failed)
}

func timeoutExample(eventBus bus.Bus[UserEvent]) {
	// Subscribe an async slow handler
	eventBus.SubscribeAsync("user.slow", func(event UserEvent) {
		time.Sleep(2 * time.Second) // Simulate slow processing
		fmt.Printf("Slow processing completed: %s\n", event.UserID)
	}, false)

	// Use timeout publishing
	err := eventBus.PublishWithTimeout("user.slow", UserEvent{
		UserID:    "user_timeout",
		Action:    "slow_operation",
		Timestamp: time.Now(),
	}, 1*time.Second)

	if err != nil {
		fmt.Printf("Publish timeout: %v\n", err)
	}
}
