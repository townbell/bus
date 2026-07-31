//go:build ignore

// This example shows a command-line application's lifecycle: publish a small
// unit of work, wait for asynchronous effects before exiting, then close the
// bus. Run it with: go run cli_example.go -name Ada -repeat 2
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/townbell/bus"
)

type greetingRequested struct {
	Name string
}

func main() {
	name := flag.String("name", "world", "name to greet")
	repeat := flag.Int("repeat", 1, "number of greeting events")
	flag.Parse()
	if *repeat < 1 {
		log.Fatal("-repeat must be at least 1")
	}

	eventBus := bus.NewTyped[greetingRequested]()
	eventBus.SetErrorHandler(func(eventErr *bus.EventError) {
		log.Printf("background %q failed: %v", eventErr.Topic, eventErr.Err)
	})

	_, err := eventBus.Subscribe("greeting.requested", func(ctx context.Context, event greetingRequested) error {
		fmt.Printf("hello, %s\n", event.Name)
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = eventBus.Subscribe("greeting.requested", func(ctx context.Context, event greetingRequested) error {
		select {
		case <-time.After(25 * time.Millisecond): // Simulate a local audit write.
			fmt.Printf("audited greeting for %s\n", event.Name)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, bus.HandlerAsync(false), bus.HandlerSerial())
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < *repeat; i++ {
		if err := eventBus.Publish("greeting.requested", greetingRequested{Name: *name}); err != nil {
			log.Fatal(err)
		}
	}

	// A CLI must explicitly drain asynchronous work; otherwise main can return
	// before the audit has run. Close then rejects future publishes and waits
	// for any work which began concurrently.
	eventBus.WaitAsync()
	if err := eventBus.Close(); err != nil {
		log.Printf("bus close: %v", err)
	}
}
