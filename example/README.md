# Townbell Examples

This directory contains comprehensive examples demonstrating the features and capabilities of the Townbell library.

[中文文档](README_ZH.md)

## Requirements

- Go 1.19 or higher
- All examples use `//go:build ignore` to prevent conflicts when running `go test ./...`
- Examples must be run individually using `go run filename.go`

## Examples Overview

### 1. `basic_example.go` - Basic Usage
**What it demonstrates:**
- Basic event subscription and publishing
- Type-safe event buses with generics
- Synchronous and asynchronous handlers
- Context-based cancellation
- Multiple subscribers for the same event
- Once-only event handlers
- Checking callback existence

**Run with:**
```bash
go run basic_example.go
```

### 2. `advanced_usage.go` - Advanced Features
**What it demonstrates:**
- Type-safe event buses with generics
- Priority-based event processing
- Event filtering with custom filters
- Context-based cancellation
- Error handling and recovery
- Monitoring and metrics collection
- Timeout handling

**Run with:**
```bash
go run advanced_usage.go
```

### 3. `middleware_example.go` - Middleware System
**What it demonstrates:**
- Request tracking middleware
- Timing middleware for performance monitoring
- Rate limiting middleware
- Middleware chaining and execution order

**Run with:**
```bash
go run middleware_example.go
```

### 4. `e_commerce_example.go` - Real-World Scenario
**What it demonstrates:**
- Complex event-driven workflow (e-commerce order processing)
- Multiple event buses for different domains
- Service interaction through events
- Priority-based processing (security, business logic, notifications)
- Error scenarios and compensation patterns

**Run with:**
```bash
go run e_commerce_example.go
```

## Key Concepts Demonstrated

### Type Safety with Generics
```go
// Create type-safe event bus
eventBus := bus.NewTyped[UserEvent]()

// All events must match the specified type
eventBus.Publish("user.login", UserEvent{...})
```

### Priority Processing
```go
// Critical priority - executes first
securityHandle := eventBus.SubscribeWithPriority("user.action", 
    securityHandler, bus.PriorityCritical)

// Low priority - executes last
analyticsHandle := eventBus.SubscribeWithPriority("user.action", 
    analyticsHandler, bus.PriorityLow)
```

### Event Filtering
```go
// Only process admin users
adminHandle := eventBus.SubscribeWithFilter("user.action", handler,
    func(topic string, event UserEvent) bool {
        return strings.HasPrefix(event.UserID, "admin_")
    })
```

### Context Cancellation
```go
ctx, cancel := context.WithCancel(context.Background())
handle := eventBus.SubscribeWithContext(ctx, "user.session", handler)

// Cancel all subscriptions with this context
cancel()
```

### Error Handling
```go
eventBus.SetErrorHandler(func(err *bus.EventError) {
    log.Printf("Event error in topic '%s': %v", err.Topic, err.Err)
})
```

### Middleware
```go
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    // Pre-processing
    log.Printf("Processing: %s", topic)
    
    next() // Execute handlers
    
    // Post-processing
    log.Printf("Completed: %s", topic)
    return nil
})
```

## Running All Examples

To run all examples sequentially:

```bash
# Basic usage
echo "=== Running Basic Example ===" && go run basic_example.go

# Advanced features
echo "=== Running Advanced Example ===" && go run advanced_usage.go

# Middleware system
echo "=== Running Middleware Example ===" && go run middleware_example.go

# E-commerce workflow
echo "=== Running E-commerce Example ===" && go run e_commerce_example.go
```

## Best Practices Demonstrated

1. **Resource Management**: Always use `defer` to unsubscribe handles
2. **Error Handling**: Set up global error handlers for monitoring
3. **Performance**: Use appropriate priorities and async processing
4. **Type Safety**: Leverage generics for compile-time type checking
5. **Graceful Shutdown**: Always close event buses with `defer eventBus.Close()`

## Performance Considerations

- **Sync vs Async**: Use async for non-critical operations
- **Priority Levels**: Use priorities to ensure critical operations execute first
- **Filters**: Apply filters to reduce unnecessary processing
- **Context**: Use context for timeout and cancellation control
- **Middleware**: Keep middleware lightweight to avoid performance impact

## Common Patterns

### Event Sourcing
```go
// Store all events for replay
eventBus.Subscribe("*", func(event interface{}) {
    eventStore.Save(event)
})
```

### CQRS (Command Query Responsibility Segregation)
```go
// Separate command and query handling
commandBus := bus.NewTyped[Command]()
queryBus := bus.NewTyped[Query]()
```

### Saga Pattern
```go
// Coordinate distributed transactions
orderBus.Subscribe("order.created", func(order Order) {
    // Start saga
    paymentBus.Publish("payment.process", Payment{...})
})

paymentBus.Subscribe("payment.failed", func(payment Payment) {
    // Compensate
    orderBus.Publish("order.cancel", Order{...})
})
``` 