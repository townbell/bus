//go:build ignore

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/townbell/bus"
)

// Event represents a basic event structure
type Event struct {
	ID      string    `json:"id"`
	Message string    `json:"message"`
	Time    time.Time `json:"time"`
}

// UserAction represents a user action event
type UserAction struct {
	UserID string `json:"user_id"`
	Action string `json:"action"`
}

func main() {
	fmt.Println("=== Townbell Basic Usage Examples ===")

	// Example 1: Basic publish and subscribe
	basicExample()

	// Example 2: Type-safe event bus
	typedExample()

	// Example 3: Async handlers
	asyncExample()

	// Example 4: Context with cancellation
	contextExample()

	// Example 5: Multiple subscribers
	multipleSubscribersExample()

	// Example 6: Once-only subscription
	onceExample()

	fmt.Println("\nAll examples completed!")
}

func basicExample() {
	fmt.Println("--- Example 1: Basic Publish/Subscribe ---")

	// Create event bus
	eventBus := bus.New()
	defer eventBus.Close()

	// Subscribe to an event
	eventBus.Subscribe("user.login", func(ctx context.Context, data any) error {
		fmt.Printf("User logged in: %v\n", data)
		return nil
	})

	// Publish an event
	eventBus.Publish("user.login", "john@example.com")

	fmt.Println()
}

func typedExample() {
	fmt.Println("--- Example 2: Type-Safe Event Bus ---")

	// Create typed event bus
	eventBus := bus.NewTyped[Event]()
	defer eventBus.Close()

	// Subscribe with typed handler
	handle, err := eventBus.Subscribe("app.event", func(ctx context.Context, event Event) error {
		fmt.Printf("Received event: ID=%s, Message=%s, Time=%s\n",
			event.ID, event.Message, event.Time.Format("15:04:05"))
		return nil
	})
	if err != nil {
		fmt.Println("subscribe failed:", err)
		return
	}
	defer handle.Unsubscribe()

	// Publish typed event
	eventBus.Publish("app.event", Event{
		ID:      "evt-001",
		Message: "Application started",
		Time:    time.Now(),
	})

	fmt.Println()
}

func asyncExample() {
	fmt.Println("--- Example 3: Async Handlers ---")

	eventBus := bus.NewTyped[UserAction]()
	defer eventBus.Close()

	// Sync handler
	eventBus.Subscribe("user.action", func(ctx context.Context, action UserAction) error {
		fmt.Printf("Sync handler: User %s performed %s\n", action.UserID, action.Action)
		return nil
	})

	// Async handler
	eventBus.Subscribe("user.action", func(ctx context.Context, action UserAction) error {
		time.Sleep(100 * time.Millisecond) // Simulate slow processing
		fmt.Printf("Async handler: User %s performed %s (processed after delay)\n",
			action.UserID, action.Action)
		return nil
	}, bus.HandlerAsync(false))

	// Publish event
	eventBus.Publish("user.action", UserAction{
		UserID: "user123",
		Action: "click_button",
	})

	// Wait for async processing
	eventBus.WaitAsync()
	fmt.Println()
}

func contextExample() {
	fmt.Println("--- Example 4: Context Cancellation ---")

	eventBus := bus.NewTyped[Event]()
	defer eventBus.Close()

	// Create cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Subscribe with context
	handle, err := eventBus.Subscribe("system.message", func(ctx context.Context, event Event) error {
		fmt.Printf("Context handler: %s\n", event.Message)
		return nil
	}, bus.HandlerContext(ctx))
	if err != nil {
		fmt.Println("subscribe failed:", err)
		return
	}
	defer handle.Unsubscribe()

	// Publish event - should be processed
	eventBus.Publish("system.message", Event{
		ID:      "msg-001",
		Message: "First message",
		Time:    time.Now(),
	})

	// Cancel context
	cancel()
	time.Sleep(10 * time.Millisecond) // Let cancellation take effect

	// Publish event - should be ignored due to cancellation
	eventBus.Publish("system.message", Event{
		ID:      "msg-002",
		Message: "Second message (should be ignored)",
		Time:    time.Now(),
	})

	fmt.Println()
}

func multipleSubscribersExample() {
	fmt.Println("--- Example 5: Multiple Subscribers ---")

	eventBus := bus.New()
	defer eventBus.Close()

	// Multiple handlers for the same event
	eventBus.Subscribe("notification", func(ctx context.Context, data any) error {
		fmt.Printf("Email handler: Sending email for %v\n", data)
		return nil
	})

	eventBus.Subscribe("notification", func(ctx context.Context, data any) error {
		fmt.Printf("SMS handler: Sending SMS for %v\n", data)
		return nil
	})

	eventBus.Subscribe("notification", func(ctx context.Context, data any) error {
		fmt.Printf("Push handler: Sending push notification for %v\n", data)
		return nil
	})

	// Publish event to all subscribers
	eventBus.Publish("notification", "New order received")

	fmt.Println()
}

func onceExample() {
	fmt.Println("--- Example 6: Once-Only Subscription ---")

	eventBus := bus.New()
	defer eventBus.Close()

	// Subscribe once - handler will be removed after first execution
	eventBus.Subscribe("init.complete", func(ctx context.Context, data any) error {
		fmt.Printf("Initialization completed: %v\n", data)
		return nil
	}, bus.HandlerOnce())

	// Check if callback exists
	fmt.Printf("Has init.complete handler: %v\n", eventBus.HasCallback("init.complete"))

	// Publish first time - handler executes
	eventBus.Publish("init.complete", "Application ready")

	// Check again - handler should be removed
	fmt.Printf("Has init.complete handler after first publish: %v\n", eventBus.HasCallback("init.complete"))

	// Publish second time - no handler should execute
	eventBus.Publish("init.complete", "Second init (should be ignored)")

	fmt.Println()
}
