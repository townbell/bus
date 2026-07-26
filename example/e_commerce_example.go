//go:build ignore

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/townbell/bus"
)

// Order represents an e-commerce order
type Order struct {
	ID       string   `json:"id"`
	UserID   string   `json:"user_id"`
	Amount   float64  `json:"amount"`
	Status   string   `json:"status"`
	Products []string `json:"products"`
}

// Payment represents a payment transaction
type Payment struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
	Method  string  `json:"method"`
	Status  string  `json:"status"`
}

// Inventory represents inventory changes
type Inventory struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Operation string `json:"operation"` // "reserve" or "release"
}

func main() {
	// Create separate event buses for different domains
	orderBus := bus.NewTyped[Order]()
	paymentBus := bus.NewTyped[Payment]()
	inventoryBus := bus.NewTyped[Inventory]()

	defer func() {
		orderBus.Close()
		paymentBus.Close()
		inventoryBus.Close()
	}()

	// Set up error handlers
	orderBus.SetErrorHandler(func(err *bus.EventError) {
		log.Printf("[ORDER-ERROR] %v", err)
	})

	paymentBus.SetErrorHandler(func(err *bus.EventError) {
		log.Printf("[PAYMENT-ERROR] %v", err)
	})

	inventoryBus.SetErrorHandler(func(err *bus.EventError) {
		log.Printf("[INVENTORY-ERROR] %v", err)
	})

	// Add middleware for audit logging
	orderBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
		log.Printf("[AUDIT] Order event: %s", topic)
		next()
		return nil
	})

	fmt.Println("=== E-commerce Event System Demo ===")

	// Start the order processing workflow
	processOrderWorkflow(orderBus, paymentBus, inventoryBus)

	// Wait for all async operations
	orderBus.WaitAsync()
	paymentBus.WaitAsync()
	inventoryBus.WaitAsync()

	fmt.Println("\nWorkflow completed!")
}

func processOrderWorkflow(orderBus bus.Bus[Order], paymentBus bus.Bus[Payment], inventoryBus bus.Bus[Inventory]) {
	// Order Service - handles order lifecycle
	orderCreatedHandle := orderBus.SubscribeWithPriority("order.created", func(order Order) {
		fmt.Printf("📦 [ORDER-SERVICE] Order created: %s for user %s (%.2f)\n",
			order.ID, order.UserID, order.Amount)

		// Reserve inventory for products
		for _, productID := range order.Products {
			inventoryBus.Publish("inventory.reserve", Inventory{
				ProductID: productID,
				Quantity:  1,
				Operation: "reserve",
			})
		}

		// Trigger payment processing
		paymentBus.Publish("payment.process", Payment{
			OrderID: order.ID,
			Amount:  order.Amount,
			Method:  "credit_card",
			Status:  "pending",
		})
	}, bus.PriorityCritical)

	// Inventory Service - manages stock
	inventoryReserveHandle := inventoryBus.SubscribeWithHandle("inventory.reserve", func(inv Inventory) {
		fmt.Printf("📦 [INVENTORY-SERVICE] Reserving %d units of product %s\n",
			inv.Quantity, inv.ProductID)

		// Simulate inventory check and reservation
		time.Sleep(50 * time.Millisecond)
		fmt.Printf("✅ [INVENTORY-SERVICE] Reserved %d units of product %s\n",
			inv.Quantity, inv.ProductID)
	})

	inventoryReleaseHandle := inventoryBus.SubscribeWithHandle("inventory.release", func(inv Inventory) {
		fmt.Printf("🔄 [INVENTORY-SERVICE] Releasing %d units of product %s\n",
			inv.Quantity, inv.ProductID)
	})

	// Payment Service - processes payments
	paymentProcessHandle := paymentBus.SubscribeWithHandle("payment.process", func(payment Payment) {
		fmt.Printf("💳 [PAYMENT-SERVICE] Processing payment for order %s (%.2f)\n",
			payment.OrderID, payment.Amount)

		// Simulate payment processing
		time.Sleep(100 * time.Millisecond)

		// Simulate payment success/failure
		if payment.Amount > 1000 {
			// Payment failed
			paymentBus.Publish("payment.failed", Payment{
				OrderID: payment.OrderID,
				Amount:  payment.Amount,
				Method:  payment.Method,
				Status:  "failed",
			})
		} else {
			// Payment succeeded
			paymentBus.Publish("payment.succeeded", Payment{
				OrderID: payment.OrderID,
				Amount:  payment.Amount,
				Method:  payment.Method,
				Status:  "completed",
			})
		}
	})

	// Payment Success Handler
	paymentSuccessHandle := paymentBus.SubscribeWithHandle("payment.succeeded", func(payment Payment) {
		fmt.Printf("✅ [PAYMENT-SERVICE] Payment succeeded for order %s\n", payment.OrderID)

		// Update order status to confirmed
		orderBus.Publish("order.confirmed", Order{
			ID:     payment.OrderID,
			Status: "confirmed",
		})
	})

	// Payment Failure Handler
	paymentFailureHandle := paymentBus.SubscribeWithHandle("payment.failed", func(payment Payment) {
		fmt.Printf("❌ [PAYMENT-SERVICE] Payment failed for order %s\n", payment.OrderID)

		// Cancel order and release inventory
		orderBus.Publish("order.cancelled", Order{
			ID:     payment.OrderID,
			Status: "cancelled",
		})
	})

	// Order Confirmed Handler
	orderConfirmedHandle := orderBus.SubscribeWithHandle("order.confirmed", func(order Order) {
		fmt.Printf("🎉 [ORDER-SERVICE] Order confirmed: %s\n", order.ID)

		// Trigger shipping process
		fmt.Printf("📮 [SHIPPING-SERVICE] Shipping initiated for order %s\n", order.ID)
	})

	// Order Cancelled Handler
	orderCancelledHandle := orderBus.SubscribeWithHandle("order.cancelled", func(order Order) {
		fmt.Printf("❌ [ORDER-SERVICE] Order cancelled: %s\n", order.ID)

		// Release reserved inventory (simulation)
		inventoryBus.Publish("inventory.release", Inventory{
			ProductID: "product-1", // Simplified for demo
			Quantity:  1,
			Operation: "release",
		})
	})

	// Notification Service - sends notifications (low priority)
	notificationHandle := orderBus.SubscribeWithPriority("order.confirmed", func(order Order) {
		fmt.Printf("📧 [NOTIFICATION-SERVICE] Sending confirmation email for order %s\n", order.ID)
	}, bus.PriorityLow)

	// Analytics Service - tracks metrics (lowest priority)
	analyticsHandle := orderBus.SubscribeWithPriority("order.created", func(order Order) {
		fmt.Printf("📊 [ANALYTICS-SERVICE] Recording order metrics for %s\n", order.ID)
	}, bus.PriorityLow)

	defer func() {
		orderCreatedHandle.Unsubscribe()
		inventoryReserveHandle.Unsubscribe()
		inventoryReleaseHandle.Unsubscribe()
		paymentProcessHandle.Unsubscribe()
		paymentSuccessHandle.Unsubscribe()
		paymentFailureHandle.Unsubscribe()
		orderConfirmedHandle.Unsubscribe()
		orderCancelledHandle.Unsubscribe()
		notificationHandle.Unsubscribe()
		analyticsHandle.Unsubscribe()
	}()

	// Simulate creating orders
	fmt.Println("\n--- Creating successful order ---")
	orderBus.Publish("order.created", Order{
		ID:       "order-001",
		UserID:   "user-123",
		Amount:   299.99,
		Status:   "pending",
		Products: []string{"product-1", "product-2"},
	})

	time.Sleep(500 * time.Millisecond) // Allow processing

	fmt.Println("\n--- Creating order that will fail payment ---")
	orderBus.Publish("order.created", Order{
		ID:       "order-002",
		UserID:   "user-456",
		Amount:   1299.99, // This will trigger payment failure
		Status:   "pending",
		Products: []string{"product-3"},
	})
}
