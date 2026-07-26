//go:build ignore

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/townbell/bus"
)

// Event represents a generic event
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	eventBus := bus.NewTyped[Event]()
	defer eventBus.Close()

	// Middleware 1: Request ID injection
	eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		log.Printf("[REQUEST] Processing event for topic: %s", topic)
		next()
		log.Printf("[REQUEST] Completed processing for topic: %s", topic)
		return nil
	})

	// Middleware 2: Timing
	eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		start := time.Now()
		log.Printf("[TIMER] Start processing topic: %s", topic)

		next()

		duration := time.Since(start)
		log.Printf("[TIMER] Topic %s processed in %v", topic, duration)
		return nil
	})

	// Middleware 3: Rate limiting simulation
	eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		// Simulate rate limit check
		if topic == "rate.limited" {
			log.Printf("[RATE_LIMIT] Rate limit applied to topic: %s", topic)
			time.Sleep(100 * time.Millisecond)
		}

		next()
		return nil
	})

	// Subscribe to events
	handle1 := eventBus.SubscribeWithHandle("user.created", func(event Event) {
		fmt.Printf("User created handler: %s - %s\n", event.ID, event.Payload)
	})

	handle2 := eventBus.SubscribeWithHandle("rate.limited", func(event Event) {
		fmt.Printf("Rate limited handler: %s - %s\n", event.ID, event.Payload)
	})

	defer func() {
		handle1.Unsubscribe()
		handle2.Unsubscribe()
	}()

	fmt.Println("=== Middleware Example ===")

	// Publish events
	eventBus.Publish("user.created", Event{
		ID:        "event-001",
		Type:      "UserCreated",
		Payload:   "User john@example.com created",
		Timestamp: time.Now(),
	})

	eventBus.Publish("rate.limited", Event{
		ID:        "event-002",
		Type:      "RateLimited",
		Payload:   "This event has rate limiting",
		Timestamp: time.Now(),
	})

	eventBus.WaitAsync()
	fmt.Println("All events processed")
}
