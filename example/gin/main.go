// Townbell's Gin integration example starts a small HTTP service. Run it with:
//
//	go run .
//
// Then POST an order with:
//
//	curl -i -X POST http://localhost:8080/orders \
//	  -H 'Content-Type: application/json' \
//	  -d '{"order_id":"o-1001","user_id":"u-1","amount":49.90}'
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/townbell/bus"
)

type orderCreated struct {
	OrderID string  `json:"order_id" binding:"required"`
	UserID  string  `json:"user_id" binding:"required"`
	Amount  float64 `json:"amount" binding:"required,gt=0"`
}

func main() {
	eventBus := bus.NewTyped[orderCreated]()
	eventBus.SetLogger(bus.NewNoOpLogger())
	eventBus.SetErrorHandler(func(eventErr *bus.EventError) {
		log.Printf("async %q failed: %v", eventErr.Topic, eventErr.Err)
	})

	// Keep effects that affect the HTTP result synchronous. Slow side effects
	// run separately and report failures through ErrorHandler.
	mustSubscribe(eventBus.Subscribe("orders.created", func(ctx context.Context, event orderCreated) error {
		log.Printf("audit order=%s user=%s amount=%.2f", event.OrderID, event.UserID, event.Amount)
		return nil
	}, bus.HandlerPriority(bus.PriorityCritical)))
	mustSubscribe(eventBus.Subscribe("orders.created", func(ctx context.Context, event orderCreated) error {
		select {
		case <-time.After(100 * time.Millisecond): // Simulate a notification API.
			log.Printf("notified user=%s for order=%s", event.UserID, event.OrderID)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, bus.HandlerAsync(false), bus.HandlerMaxConcurrency(4)))

	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	router.POST("/orders", func(c *gin.Context) {
		var event orderCreated
		if err := c.ShouldBindJSON(&event); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// The request context bounds synchronous handlers. An async handler uses
		// its subscription context because it can outlive this HTTP request.
		if err := eventBus.PublishWithContext(c.Request.Context(), "orders.created", event); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusAccepted, gin.H{"order_id": event.OrderID, "status": "accepted"})
	})

	server := &http.Server{Addr: ":8080", Handler: router}
	shutdownSignal, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-shutdownSignal.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	}()

	fmt.Println("Gin example listening on http://localhost:8080; press Ctrl-C to stop")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	// Shutdown has stopped new requests. Drain work that was already started,
	// then close the bus so no handler can start afterwards.
	eventBus.WaitAsync()
	if err := eventBus.Close(); err != nil {
		log.Printf("bus close: %v", err)
	}
}

func mustSubscribe(_ *bus.Handle[orderCreated], err error) {
	if err != nil {
		log.Fatal(err)
	}
}
