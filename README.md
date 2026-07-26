<p align="center">
  <img src="assets/icon.png" alt="Townbell Bus Icon" width="120">
</p>
<h1 align="center">Townbell Bus</h1>
<p align="center">
  <strong>A High-Performance Event-Driven Architecture Library for Go</strong>
</p>


[![Go Version](https://img.shields.io/badge/go-%3E%3D1.19-blue.svg)](https://golang.org/)
[![Go Reference](https://pkg.go.dev/badge/github.com/townbell/bus.svg)](https://pkg.go.dev/github.com/townbell/bus)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/townbell/bus)](https://goreportcard.com/report/github.com/townbell/bus)
[![Coverage](https://img.shields.io/badge/coverage-91.2%25-brightgreen.svg)](https://github.com/townbell/bus)



A modern, high-performance Go event bus implementation with type safety, async processing, priority handling, filters, and enterprise-grade features.

```go
import "github.com/townbell/bus" // package bus
```

[中文文档](README_ZH.md)

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

## 🚀 Quick Start

### Basic Usage

```go
package main

import (
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
    handle := eventBus.SubscribeWithHandle("user.login", func(event UserEvent) {
        fmt.Printf("User %s performed %s\n", event.UserID, event.Action)
    })
    defer handle.Unsubscribe()

    // Publish events
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
securityHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("🔒 Security check")
}, bus.PriorityCritical)

// Normal priority - business logic
businessHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("📋 Business processing")
}, bus.PriorityNormal)

// Low priority - analytics
analyticsHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("📊 Analytics")
}, bus.PriorityLow)
```

### Event Filtering

Process only events that match specific criteria:

```go
// Only process admin user events
adminHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
    fmt.Printf("Admin action: %s\n", event.UserID)
}, func(topic string, event UserEvent) bool {
    return strings.HasPrefix(event.UserID, "admin_")
})

// Only process sensitive operations
sensitiveHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
    fmt.Printf("Sensitive operation alert: %s\n", event.Action)
}, func(topic string, event UserEvent) bool {
    sensitiveActions := []string{"delete", "modify_permissions"}
    for _, action := range sensitiveActions {
        if event.Action == action {
            return true
        }
    }
    return false
})
```

### Context Control

Use context for cancellation and timeout control:

```go
// Context cancellation
ctx, cancel := context.WithCancel(context.Background())
handle := eventBus.SubscribeWithContext(ctx, "user.session", func(event UserEvent) {
    fmt.Printf("Session event: %s\n", event.UserID)
})

// Cancel subscription
cancel()

// Timeout publishing
err := eventBus.PublishWithTimeout("user.action", event, 5*time.Second)
if err != nil {
    fmt.Printf("Publish timeout: %v\n", err)
}
```

### Error Handling

Set up global error handler:

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
err := eventBus.SubscribeAsync("user.notification", func(event UserEvent) {
    sendEmail(event.UserID)
}, false)

// Async processing, transactional (serial execution)
err := eventBus.SubscribeAsync("user.audit", func(event UserEvent) {
    writeAuditLog(event)
}, true)
```

### Handler Execution Control

`SubscribeWithOptions` adds handler-level controls without changing the existing simple APIs:

```go
handle, err := eventBus.SubscribeWithOptions("payment.validate", func(event PaymentEvent) {
    validatePayment(event)
},
    bus.HandlerTimeout(2*time.Second),
    bus.HandlerRecoverPolicy(bus.RecoverAndStop),
    bus.HandlerSerial(),
    bus.HandlerPriority(bus.PriorityHigh),
)
```

## ✅ Behavior Contract

- `Publish` ignores returned errors; use `PublishWithContext` or `PublishWithTimeout` when cancellation, timeout, or closed-bus errors matter.
- Synchronous handlers run in the current goroutine by default; asynchronous handlers run in separate goroutines, and `transactional=true` serializes calls to the same handler.
- Handler timeouts bound how long the publish call waits; they do not forcibly stop a handler that has already started running.
- Handler panics are recovered by default, failed metrics are incremented, and the error is reported through `ErrorHandler`; `RecoverAndStop` makes a recovered panic stop the current publish call.
- Middleware must call `next()` to continue to the next middleware and handlers; skipping `next()` intercepts the event.
- `SubscribeOnce` / `SubscribeOnceAsync` handlers execute successfully at most once, including when multiple one-time handlers share a topic.
- After `Close`, the bus rejects new publish and subscribe calls; already-started async handlers are allowed to finish.

## 🗺️ RoadMap

Townbell will keep its focus on being an in-process, type-safe, lightweight event bus. Future work may borrow ideas from Watermill, Blinker, MediatR, and Guava EventBus, but the core package will not try to become a full distributed messaging system.

| Priority | Status | Area | Notes |
| --- | --- | --- | --- |
| P0 | Done | Core correctness | Publish no longer holds the bus lock while running handlers; `SubscribeOnce` removal, middleware chaining, and race tests are covered |
| P0 | Done | API contract | Error returns for `Publish` / `PublishWithContext`, panic recovery, sync/async execution, and closed-bus behavior are documented |
| P1 | Done | Documentation alignment | README content now documents the current P1 behavior and examples |
| P1 | Done | Observability | Per-topic and per-handler published, processed, failed, and duration metrics are available, with an optional Prometheus adapter |
| P1 | Done | Execution control | `SubscribeWithOptions` supports handler-level timeout, recover policy, async/serial execution, and max concurrency |
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
    Subscribe(topic string, fn func(T)) error
    SubscribeWithPriority(topic string, fn func(T), priority Priority) *Handle[T]
    SubscribeWithFilter(topic string, fn func(T), filter EventFilter[T]) *Handle[T]
    SubscribeWithContext(ctx context.Context, topic string, fn func(T)) *Handle[T]
    // ...
}

// Publisher interface
type BusPublisher[T any] interface {
    Publish(topic string, event T)
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
eventBus.SubscribeAsync("analytics.track", func(event UserEvent) {
    // Non-critical analytics
    analytics.Track(event)
}, false)

// Use sync for critical paths
eventBus.Subscribe("payment.validate", func(event PaymentEvent) {
    // Critical payment validation
    validatePayment(event)
})

// Use filters to reduce unnecessary processing
eventBus.SubscribeWithFilter("user.activity", handler, func(topic string, event UserEvent) bool {
    return event.IsImportant() // Only process important events
})
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

Run the complete test suite:

```bash
go test -v ./...
```

Generate test coverage report:

```bash
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

**Current test coverage: 91.2%** (whole module, `go test -coverprofile ./...`) - We maintain high test coverage to ensure reliability and stability.

Run performance benchmarks:

```bash
go test -bench=. -benchmem
```

## 📈 Performance

Benchmark results on Go 1.19+:

- **Sync Publishing**: ~2,000,000 events/sec
- **Async Publishing**: ~5,000,000 events/sec
- **Memory Usage**: Minimal GC pressure
- **Concurrency**: Excellent multi-core scaling

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
