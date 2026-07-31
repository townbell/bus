//go:build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/townbell/bus"
)

// EmailJob is a unit of background work produced by the application.
type EmailJob struct {
	ID string
}

func main() {
	eventBus := bus.NewTyped[EmailJob]()
	defer eventBus.Close()

	eventBus.SetErrorHandler(func(eventErr *bus.EventError) {
		log.Printf("job %q failed: %v", eventErr.Topic, eventErr.Err)
	})

	var active int32
	var peak int32
	_, err := eventBus.Subscribe("email.send", func(ctx context.Context, job EmailJob) error {
		current := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		for {
			previous := atomic.LoadInt32(&peak)
			if current <= previous || atomic.CompareAndSwapInt32(&peak, previous, current) {
				break
			}
		}

		select {
		case <-time.After(100 * time.Millisecond):
			fmt.Println("sent", job.ID)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	},
		bus.HandlerAsync(false),
		bus.HandlerMaxConcurrency(2),
	)
	if err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= 5; i++ {
		if err := eventBus.Publish("email.send", EmailJob{ID: fmt.Sprintf("email-%d", i)}); err != nil {
			log.Printf("enqueue email-%d: %v", i, err)
		}
	}

	// WaitAsync drains already-started background handlers before the process exits.
	eventBus.WaitAsync()
	fmt.Println("peak concurrent jobs:", atomic.LoadInt32(&peak))
}
