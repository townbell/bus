<p align="center">
  <img src="assets/icon.png" alt="Townbell Bus Icon" width="120">
</p>
<h1 align="center">Townbell Bus</h1>
<p align="center">
  <strong>A High-Performance Event-Driven Architecture Library for Go</strong>
</p>


[![CI](https://github.com/townbell/bus/actions/workflows/ci.yml/badge.svg)](https://github.com/townbell/bus/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://golang.org/)
[![Go Reference](https://pkg.go.dev/badge/github.com/townbell/bus.svg)](https://pkg.go.dev/github.com/townbell/bus)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/townbell/bus)](https://goreportcard.com/report/github.com/townbell/bus)
[![Coverage](https://img.shields.io/badge/coverage-93.3%25-brightgreen.svg)](https://github.com/townbell/bus/actions/workflows/ci.yml)



A modern, high-performance Go event bus implementation with type safety, async processing, priority handling, filters, and enterprise-grade features.

```go
import "github.com/townbell/bus" // package bus
```

[中文文档](README_ZH.md)

> **v0.6.0 replaced the subscription API.** One `Subscribe` method with
> options, handlers of the form `func(ctx, T) error`, and `Publish` returns an
> error. Coming from v0.5.x? See [MIGRATION.md](MIGRATION.md) — the rewrite is
> mechanical. This API freezes at v1.0.0; design feedback is welcome now.

## ✨ Features

### 🔧 Core Features
- **Type Safety**: Go generics ensure compile-time type safety
- **Sync/Async**: Support for both synchronous and asynchronous event processing
- **Handle Pattern**: Precise subscription management with unsubscribe handles
- **Once Subscription**: One-time event handlers that auto-remove after execution

### 🚀 Enterprise Features
- **Priority Processing**: 4-level priority system (Critical, High, Normal, Low)
- **Event Filtering**: Custom event filters for fine-grained control
- **Context Support**: Context-based cancellation and timeout handling
- **Middleware**: Event processing middleware chain
- **Error Handling**: Comprehensive error handling and recovery
- **Monitoring**: Built-in performance metrics and statistics
- **Graceful Shutdown**: Proper resource cleanup and graceful termination

### 🔒 Reliability
- **Thread Safe**: Concurrent-safe design for multi-goroutine usage
- **Panic Recovery**: Automatic recovery from handler panics
- **Resource Management**: Automatic cleanup and memory management

## 📦 Installation

```bash
go get github.com/townbell/bus
```

The core module has no dependencies at all — importing it pulls in nothing but
the standard library. The optional Prometheus adapter lives in its own module,
so `client_golang` only enters your build if you ask for it:

```bash
go get github.com/townbell/bus/prometheus
```

## 🚀 Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"

    "github.com/townbell/bus"
)

type UserEvent struct {
    UserID string
    Action string
}

func main() {
    // Create a type-safe event bus
    eventBus := bus.NewTyped[UserEvent]()
    defer eventBus.Close()

    // Subscribe to events
    handle, err := eventBus.Subscribe("user.login", func(ctx context.Context, event UserEvent) error {
        fmt.Printf("User %s performed %s\n", event.UserID, event.Action)
        return nil
    })
    if err != nil {
        panic(err)
    }
    defer handle.Unsubscribe()

    // Publish events. The returned error joins any synchronous handler
    // failures and is safe to ignore when they do not matter to the caller.
    eventBus.Publish("user.login", UserEvent{
        UserID: "user123",
        Action: "login",
    })
}
```

## 📖 Advanced Usage

### Priority Processing

Handlers with different priorities execute in priority order:

```go
// High priority - security checks
securityHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("🔒 Security check")
    return nil
}, bus.HandlerPriority(bus.PriorityCritical))

// Normal priority - business logic
businessHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("📋 Business processing")
    return nil
}, bus.HandlerPriority(bus.PriorityNormal))

// Low priority - analytics
analyticsHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("📊 Analytics")
    return nil
}, bus.HandlerPriority(bus.PriorityLow))
```

### Event Filtering

Process only events that match specific criteria:

```go
// Only process admin user events
adminHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("Admin action: %s\n", event.UserID)
    return nil
}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
    return strings.HasPrefix(event.UserID, "admin_")
}))

// Only process sensitive operations
sensitiveHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("Sensitive operation alert: %s\n", event.Action)
    return nil
}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
    sensitiveActions := []string{"delete", "modify_permissions"}
    for _, action := range sensitiveActions {
        if event.Action == action {
            return true
        }
    }
    return false
}))
```

### Context Control

Use context for cancellation and timeout control:

```go
// Context cancellation: canceling the subscription context disables the handler
ctx, cancel := context.WithCancel(context.Background())
handle, _ := eventBus.Subscribe("user.session", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("Session event: %s\n", event.UserID)
    return nil
}, bus.HandlerContext(ctx))

// Cancel subscription
cancel()

// Timeout publishing. The handler's ctx is canceled when the deadline passes,
// so a cooperative handler can stop early instead of running to completion.
err := eventBus.PublishWithTimeout("user.action", event, 5*time.Second)
if err != nil {
    fmt.Printf("Publish timeout: %v\n", err)
}
```

### Error Handling

Handlers report business failures by returning an error. The failure is
counted in metrics, reported to the global `ErrorHandler`, and joined into the
publish call's return value — dispatch to the remaining handlers continues:

```go
eventBus.Subscribe("order.created", func(ctx context.Context, event OrderEvent) error {
    if err := reserveStock(ctx, event); err != nil {
        return fmt.Errorf("reserve stock: %w", err)
    }
    return nil
})

if err := eventBus.Publish("order.created", order); err != nil {
    log.Printf("delivery failures: %v", err) // errors.Is sees through the join
}
```

Set up a global error handler to observe every failure, including those from
asynchronous handlers (whose errors never reach the publish return value):

```go
eventBus.SetErrorHandler(func(err *bus.EventError) {
    log.Printf("Event processing error - Topic: %s, Error: %v", err.Topic, err.Err)
})
```

### Middleware

Add processing middleware:

```go
// Logging middleware
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    start := time.Now()
    log.Printf("Processing event: %s", topic)
    
    next() // Execute handlers
    
    log.Printf("Event processed: %s, Duration: %v", topic, time.Since(start))
    return nil
})

// Rate limiting middleware
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    if rateLimiter.Allow() {
        next()
        return nil
    }
    return fmt.Errorf("rate limit exceeded")
})
```

### Monitoring Metrics

Get runtime metrics:

```go
metrics := eventBus.GetMetrics()
published, processed, failed, subscribers := metrics.GetStats()

fmt.Printf("Published events: %d\n", published)
fmt.Printf("Processed events: %d\n", processed)
fmt.Printf("Failed events: %d\n", failed)
fmt.Printf("Active subscribers: %d\n", subscribers)

// Get topic information
topics := eventBus.GetTopics()
subscriberCount := eventBus.GetSubscriberCount("user.action")
```

Detailed metrics are available from the default metrics implementation:

```go
if detailed, ok := eventBus.GetMetrics().(*bus.DefaultMetrics); ok {
    topicStats := detailed.GetTopicStats()
    handlerStats := detailed.GetHandlerStats()
    fmt.Println(topicStats["user.action"].ProcessedEvents)
    fmt.Println(handlerStats)
}
```

Prometheus integration is available as an optional subpackage:

```go
import busprom "github.com/townbell/bus/prometheus"

promMetrics := busprom.New(busprom.Config{})
eventBus := bus.NewTyped[UserEvent](
    bus.WithMetrics[UserEvent](promMetrics),
)
```

### Asynchronous Processing

```go
// Async processing, non-transactional (concurrent execution)
_, err := eventBus.Subscribe("user.notification", func(ctx context.Context, event UserEvent) error {
    return sendEmail(ctx, event.UserID)
}, bus.HandlerAsync(false))

// Async processing, transactional (serial execution)
_, err = eventBus.Subscribe("user.audit", func(ctx context.Context, event UserEvent) error {
    return writeAuditLog(ctx, event)
}, bus.HandlerAsync(true))
```

### Handler Execution Control

Every subscription concern is a `HandlerOption`, and they compose freely in
one `Subscribe` call:

```go
handle, err := eventBus.Subscribe("payment.validate",
    func(ctx context.Context, event PaymentEvent) error {
        return validatePayment(ctx, event)
    },
    bus.HandlerTimeout(2*time.Second),           // cancels ctx when it elapses
    bus.HandlerRecoverPolicy(bus.RecoverAndStop), // a panic aborts the publish
    bus.HandlerSerial(),                          // one execution at a time
    bus.HandlerPriority(bus.PriorityHigh),
)
```

Available options: `HandlerPriority`, `HandlerFilter`, `HandlerContext`,
`HandlerAsync`, `HandlerOnce`, `HandlerTimeout`, `HandlerRecoverPolicy`,
`HandlerMaxConcurrency`, `HandlerSerial`.

## ✅ Behavior Contract

- `Publish` and `PublishWithContext` return the joined failures of the synchronous handlers (`errors.Is` sees through the join). The error is safe to ignore. Asynchronous handler failures are reported through `ErrorHandler` only, because the publish call may return before they run.
- A handler error does not stop dispatch: the remaining handlers still run. Dispatch stops early only when the publish context is canceled, the bus closes, or a handler panics under `RecoverAndStop`.
- Synchronous handlers run in the goroutine that calls publish and receive a context derived from the publish call; asynchronous handlers run in separate goroutines and receive the subscription context (`HandlerContext`), and `HandlerAsync(true)` serializes calls to the same handler.
- `HandlerTimeout` bounds how long the publish call waits and cancels the handler's context when it elapses; a handler that ignores its context keeps running in the background and is still awaited by `WaitAsync` and `Close`.
- Handler panics are recovered, counted as failures, and reported through `ErrorHandler`; `RecoverAndContinue` (the default) keeps dispatching, `RecoverAndStop` aborts the publish call. Either way the recovered panic appears in the publish error.
- Middleware must call `next()` to continue to the next middleware and handlers; skipping `next()` intercepts the event.
- `HandlerOnce` handlers execute successfully at most once, including when multiple one-time handlers share a topic.
- After `Close`, the bus rejects new publish and subscribe calls; already-started async handlers are allowed to finish.
- `Subscribe` returns `(nil, error)` when the subscription is rejected (nil callback, mismatched filter type, or a closed bus). A `nil` handle is safe to use: `Unsubscribe` returns an error and `IsActive` returns `false`, so ignoring the error and deferring `Unsubscribe` never panics.

## 🗺️ RoadMap

Townbell will keep its focus on being an in-process, type-safe, lightweight event bus. Future work may borrow ideas from Watermill, Blinker, MediatR, and Guava EventBus, but the core package will not try to become a full distributed messaging system.

| Priority | Status | Area | Notes |
| --- | --- | --- | --- |
| P0 | Done | Core correctness | Publish no longer holds the bus lock while running handlers; `SubscribeOnce` removal, middleware chaining, and race tests are covered |
| P0 | Done | API contract | Error returns for `Publish` / `PublishWithContext`, panic recovery, sync/async execution, and closed-bus behavior are documented |
| P1 | Done | Documentation alignment | README content now documents the current P1 behavior and examples |
| P1 | Done | Observability | Per-topic and per-handler published, processed, failed, and duration metrics are available, with an optional Prometheus adapter |
| P1 | Done | Execution control | `SubscribeWithOptions` supports handler-level timeout, recover policy, async/serial execution, and max concurrency |
| P1 | Done | Continuous integration | GitHub Actions runs build, vet, race tests, a gofmt gate and a coverage floor across a Go version matrix, and vets `example/` explicitly |
| P1 | Done | Dependency-free core | The Prometheus adapter moved into its own module, so importing `bus` pulls in nothing but the standard library |
| P1 | Done | Runnable documentation | Godoc `Example` functions execute in CI with verified output and render on pkg.go.dev |
| P0 | Preview (v0.6.0) | Subscription API convergence | One `Subscribe(topic, fn, opts...) (*Handle[T], error)` replaced the ten previous variants. Freezes at v1.0.0 |
| P0 | Preview (v0.6.0) | Handler error reporting | Handlers are `func(ctx, T) error`: business failures reach metrics, the `ErrorHandler`, and the publish return value without panicking. Unblocks result collection. Freezes at v1.0.0 |
| P0 | Preview (v0.6.0) | `Publish` error semantics | `Publish` returns the joined synchronous-handler failures; ignoring it stays legal. Freezes at v1.0.0 |
| P2 | Planned | Result collection | Borrow from Blinker and add a `PublishCollect`-style API for collecting handler results or errors |
| P2 | Partial | Topic enhancements | Wildcard (`*`) topics are implemented; hierarchical topics and no-subscriber hooks are still planned |
| P2 | Planned | Integration examples | Add practical examples for `net/http`, Gin, CLI apps, and workers |
| P3 | Planned | Broker bridges | Borrow from Watermill and explore NATS / Kafka / RabbitMQ adapters, preferably in separate subpackages |
| P3 | Planned | Mediator mode | Borrow from MediatR and add request / response, command, query, and notification support only if needed |
| P4 | Planned | Stateful features | Evaluate sticky events, event replay, and local persistence only when there is a clear use case |

## 🏗️ Architecture Design

The library is organized into separate modules for better maintainability:

### File Structure

- **`types.go`** - Core type definitions (Priority, EventError, filters, middleware)
- **`interfaces.go`** - Interface definitions (BusSubscriber, BusPublisher, BusController, Bus)
- **`metrics.go`** - Monitoring and metrics functionality
- **`handle.go`** - Subscription handle management and internal handler structures
- **`bus.go`** - Core EventBus implementation

### Interface Separation

```go
// Subscriber interface
type BusSubscriber[T any] interface {
    Subscribe(topic string, fn Handler[T], options ...HandlerOption) (*Handle[T], error)
}

// Publisher interface
type BusPublisher[T any] interface {
    Publish(topic string, event T) error
    PublishWithContext(ctx context.Context, topic string, event T) error
    PublishWithTimeout(topic string, event T, timeout time.Duration) error
}

// Controller interface
type BusController interface {
    GetMetrics() Metrics
    SetErrorHandler(handler ErrorHandler)
    AddMiddleware(middleware EventMiddleware[any])
    Close() error
    // ...
}
```

### Type System

```go
// Event handler
type Handler[T any] func(ctx context.Context, event T) error

// Event filter
type EventFilter[T any] func(topic string, event T) bool

// Event middleware
type EventMiddleware[T any] func(topic string, event T, next func()) error

// Error handler
type ErrorHandler func(err *EventError)

// Priority levels
type Priority int
const (
    PriorityLow Priority = iota
    PriorityNormal
    PriorityHigh
    PriorityCritical
)
```

## 🔧 Best Practices

### 1. Event Design Patterns

Following industry best practices, supports these event design patterns:

#### Event Notification
```go
type UserCreatedEvent struct {
    UserID    string    `json:"user_id"`
    Timestamp time.Time `json:"timestamp"`
    // Minimal data, subscribers fetch details themselves
}
```

#### Event-Carried State Transfer
```go
type UserUpdatedEvent struct {
    UserID       string                 `json:"user_id"`
    Timestamp    time.Time              `json:"timestamp"`
    OldState     map[string]interface{} `json:"old_state"`
    NewState     map[string]interface{} `json:"new_state"`
    ChangedFields []string              `json:"changed_fields"`
}
```

### 2. Naming Conventions

```go
// Use dot-separated hierarchical naming
"user.created"
"user.updated"
"user.deleted"
"order.placed"
"order.cancelled"
"payment.processed"
"payment.failed"

// Or use namespaces
"ecommerce.order.created"
"auth.user.login"
"notification.email.sent"
```

### 3. Error Handling Strategy

```go
// Set up retry mechanism
eventBus.SetErrorHandler(func(err *EventError) {
    switch err.Err.(type) {
    case *TemporaryError:
        // Temporary error, retry later
        retryQueue.Add(err.Topic, err.Event)
    case *PermanentError:
        // Permanent error, log and alert
        logger.Error("Permanent error", err)
        alerting.Send(err)
    default:
        // Unknown error, log details
        logger.Warn("Unknown error", err)
    }
})
```

### 4. Performance Optimization

```go
// Use async for non-critical paths
eventBus.Subscribe("analytics.track", func(ctx context.Context, event UserEvent) error {
    return analytics.Track(ctx, event) // Non-critical analytics
}, bus.HandlerAsync(false))

// Use sync for critical paths
eventBus.Subscribe("payment.validate", func(ctx context.Context, event PaymentEvent) error {
    return validatePayment(ctx, event) // Critical payment validation
})

// Use filters to reduce unnecessary processing
eventBus.Subscribe("user.activity", handler, bus.HandlerFilter(
    func(topic string, event UserEvent) bool {
        return event.IsImportant() // Only process important events
    }))
```

## 🔍 Comparison with Other Libraries

| Feature | Townbell | Guava EventBus | RxJava | Node.js EventEmitter |
|---------|---------|----------------|---------|---------------------|
| Type Safety | ✅ Generics | ✅ | ✅ | ❌ |
| Async Processing | ✅ | ❌ | ✅ | ✅ |
| Priority | ✅ | ❌ | ❌ | ❌ |
| Filters | ✅ | ❌ | ✅ | ❌ |
| Middleware | ✅ | ❌ | ✅ | ❌ |
| Error Handling | ✅ | ⚠️ | ✅ | ⚠️ |
| Monitoring | ✅ | ❌ | ❌ | ❌ |
| Context Support | ✅ | ❌ | ❌ | ❌ |

## 🧪 Testing

Run the complete test suite. The Prometheus adapter is a separate module, so it
needs its own invocation:

```bash
go test -race ./...
(cd prometheus && go test -race ./...)
```

Files under `example/` carry `//go:build ignore`, which means `go vet ./...`
skips them. Vet them by name:

```bash
for f in example/*.go; do go vet "$f"; done
```

Generate a coverage report:

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

**Current coverage: 93.3%** for the core module, 79.7% for the Prometheus
adapter. CI enforces a 90% floor on the core module, so this figure cannot
silently drift.

Run the benchmarks:

```bash
go test -bench=. -benchmem
```

## 📈 Performance

Measured on an Apple M5 (10 cores), Go 1.22.12, darwin/arm64, on 2026-07-26.
Every benchmark uses `b.RunParallel`, so `ns/op` is the aggregate cost across
all cores rather than single-goroutine latency.

| Benchmark | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `SyncPublish` (1 subscriber) | 839 | 392 | 11 |
| `AsyncPublish` (1 subscriber) | 1320 | 520 | 12 |
| `MultipleSubscribers` | 4470 | 752 | 29 |
| `WithPriority` | 532 | 472 | 15 |
| `WithFilter` | 240 | 376 | 10 |
| `ConcurrentSubscribeUnsubscribe` | 3613 | 1207 | 38 |
| `ChannelBaseline` (raw Go channel) | 50 | 0 | 0 |

Two things are worth reading off this table:

- **Asynchronous publishing is slower than synchronous publishing**, not faster.
  Every async dispatch starts a goroutine and touches a `WaitGroup` and a mutex.
  Reach for async to keep a slow handler off the publishing goroutine, not to
  raise throughput.
- **A raw channel is roughly 17x cheaper.** The bus buys you fan-out,
  priorities, filters, middleware and metrics. If all you need is to hand a
  value to one known goroutine, a channel is the better tool.

Numbers on your hardware will differ. Re-run the benchmarks rather than trusting
this table.

## 🤝 Contributing

We welcome Issues and Pull Requests!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

MIT License - see the [LICENSE](LICENSE) file for details

## 🙏 Acknowledgments

This project draws inspiration from these excellent open source projects and design patterns:

- [Guava EventBus](https://github.com/google/guava) - The classic Java implementation
- [MBassador](https://github.com/bennidi/mbassador) - High-performance Java EventBus
- [Node.js EventEmitter](https://nodejs.org/api/events.html) - JavaScript native event system
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/) - Enterprise integration patterns
