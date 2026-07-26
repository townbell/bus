//go:build ignore

// This example shows the event bus inside a net/http service: HTTP handlers
// publish domain events, and decoupled subscribers (audit, notification,
// analytics) react to them. It runs against an httptest server and exits, so
// it can be executed directly: go run http_example.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	"github.com/townbell/bus"
)

// OrderPlaced is the domain event the HTTP layer publishes.
type OrderPlaced struct {
	OrderID string  `json:"order_id"`
	UserID  string  `json:"user_id"`
	Amount  float64 `json:"amount"`
}

func main() {
	eventBus := bus.NewTyped[OrderPlaced]()
	defer eventBus.Close()
	eventBus.SetLogger(bus.NewNoOpLogger())

	// Route mistakes surface instead of vanishing: any event published to a
	// topic nobody subscribed to lands here.
	eventBus.SetDeadEventHandler(func(topic string, event OrderPlaced) {
		log.Printf("[DEAD] nobody handles %q: %+v", topic, event)
	})

	// Audit trail must not lose events - synchronous, highest priority.
	eventBus.Subscribe("orders.placed", func(ctx context.Context, e OrderPlaced) error {
		fmt.Printf("[AUDIT]    order %s by %s (%.2f)\n", e.OrderID, e.UserID, e.Amount)
		return nil
	}, bus.HandlerPriority(bus.PriorityCritical))

	// Notification is slow - run it off the request goroutine.
	eventBus.Subscribe("orders.placed", func(ctx context.Context, e OrderPlaced) error {
		time.Sleep(50 * time.Millisecond) // simulate an email API call
		fmt.Printf("[NOTIFY]   confirmation sent for %s\n", e.OrderID)
		return nil
	}, bus.HandlerAsync(false))

	// Analytics wants everything under orders.* - a hierarchical pattern.
	eventBus.Subscribe("orders.*", func(ctx context.Context, e OrderPlaced) error {
		fmt.Printf("[ANALYTIC] recorded event for %s\n", e.OrderID)
		return nil
	}, bus.HandlerPriority(bus.PriorityLow))

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var order OrderPlaced
		if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// The request context bounds synchronous dispatch: if the client
		// disconnects, sync handlers see a canceled ctx. Async handlers keep
		// running on the subscription context.
		if err := eventBus.PublishWithContext(r.Context(), "orders.placed", order); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/typo", func(w http.ResponseWriter, r *http.Request) {
		// A misrouted publish: no subscriber for this topic, so the
		// dead-event handler reports it.
		eventBus.Publish("order.placed", OrderPlaced{OrderID: "oops"})
		w.WriteHeader(http.StatusAccepted)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	fmt.Println("=== HTTP Integration Example ===")
	post(server.URL+"/orders", `{"order_id":"o-1001","user_id":"u-1","amount":49.90}`)
	post(server.URL+"/typo", `{}`)

	// Let async subscribers finish before the process exits.
	eventBus.WaitAsync()
	fmt.Println("done")
}

func post(url, body string) {
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		log.Fatalf("POST %s: %v", url, err)
	}
	resp.Body.Close()
	fmt.Printf("POST %s -> %s\n", url, resp.Status)
}
