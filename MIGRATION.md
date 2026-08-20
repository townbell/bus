# Migration guide

[中文版](MIGRATION_ZH.md)

## v0.12.x to v0.13.0

v0.13.0 deliberately spends the remaining pre-v1 compatibility budget on
contracts that would otherwise be expensive to correct after the API freeze.

### Middleware is typed

Middleware now receives the bus event type instead of `any`:

```go
// Before
b.AddMiddleware(func(topic string, event any, next func()) error { ... })

// After, for *EventBus[Order]
b.AddMiddleware(func(topic string, event Order, next func()) error { ... })
```

This removes type assertions and makes invalid middleware fail at compile time.

### Broad bus interfaces were removed

`Bus[T]` and `BusController` mixed unrelated capabilities and forced test
doubles to implement methods their consumers did not use. Accept
`BusPublisher[T]`, `BusSubscriber[T]`, or `BusResultCollector[T]` where that is
the actual dependency. If a component needs a different combination, declare
that small interface next to the component. Code that intentionally needs the
whole implementation can accept `*EventBus[T]`.

`BusSubscriber[T]` now contains only `Subscribe`; configure dead-event handling
on the concrete bus during construction.

### Metrics, logging, and Prometheus

- Read `DefaultMetrics` aggregates through `GetStats`; its mutable counters are
  no longer exported.
- `Logger` now requires only `Debug`, `Error`, and `GetLevel`. Configure a
  `DefaultLogger` through its concrete `SetLevel` method before passing it to
  the bus. `NewDefaultLoggerWithOutput` now accepts any `io.Writer`.
- `prometheus.New` now returns `(*Metrics, error)` so registration failures can
  be handled. Use `prometheus.MustNew` only when registration failure should
  abort process startup.

```go
metrics, err := prometheus.New(prometheus.Config{Registerer: registry})
if err != nil {
    return err
}
```

### Error and lifecycle contracts

- `EventError` unwraps its cause, and handler timeouts wrap
  `context.DeadlineExceeded`, so callers can use `errors.Is` and `errors.As`.
- `PublishCollect` preserves synchronous handler errors even when middleware
  also fails; middleware errors follow handler errors as the chain unwinds.
- A positive `HandlerQueueCapacity` without bounded asynchronous delivery is
  rejected with `ErrInvalidHandlerOptions` instead of being silently ignored.
- Call `WaitAsync` only after publishers are quiescent. Use `Close` as the
  concurrent shutdown barrier, and call it from the bus owner rather than an
  async handler on that bus.

## v0.5.x to v0.6.0

v0.6.0 replaces the subscription API. This is the API that will freeze at
v1.0.0; v0.6.0 is its trial run, and feedback that surfaces design flaws can
still change it. There are three changes, and they compose into one mechanical
rewrite of each subscription site.

### 1. The handler signature is now `func(ctx, T) error`

```go
// Before
func(event OrderEvent) { process(event) }

// After
func(ctx context.Context, event OrderEvent) error {
    return process(ctx, event)
}
```

Why: handlers can now report business failures without panicking, and they can
observe cancellation. When a handler timeout elapses or the publish context is
canceled, the handler's `ctx` is canceled — previously the bus could only stop
*waiting* for a handler; now the handler can actually stop working.

A handler with nothing to report returns `nil`. A returned error is counted in
metrics as a failed delivery, reported to the `ErrorHandler`, and joined into
the publish call's return value; it does **not** stop dispatch to the
remaining handlers.

### 2. One `Subscribe` method replaces all ten variants

Every subscription is now `Subscribe(topic, fn, opts...) (*Handle[T], error)`:

| v0.5.x | v0.6.0 |
| --- | --- |
| `Subscribe(topic, fn) error` | `Subscribe(topic, fn)` |
| `SubscribeWithHandle(topic, fn) *Handle` | `Subscribe(topic, fn)` |
| `SubscribeAsync(topic, fn, tx) error` | `Subscribe(topic, fn, HandlerAsync(tx))` |
| `SubscribeAsyncWithHandle(topic, fn, tx) *Handle` | `Subscribe(topic, fn, HandlerAsync(tx))` |
| `SubscribeOnce(topic, fn) error` | `Subscribe(topic, fn, HandlerOnce())` |
| `SubscribeOnceAsync(topic, fn) error` | `Subscribe(topic, fn, HandlerOnce(), HandlerAsync(false))` |
| `SubscribeWithPriority(topic, fn, p) *Handle` | `Subscribe(topic, fn, HandlerPriority(p))` |
| `SubscribeWithFilter(topic, fn, filter) *Handle` | `Subscribe(topic, fn, HandlerFilter(filter))` |
| `SubscribeWithContext(ctx, topic, fn) *Handle` | `Subscribe(topic, fn, HandlerContext(ctx))` |
| `SubscribeWithOptions(topic, fn, opts...) (*Handle, error)` | `Subscribe(topic, fn, opts...)` |

`HandlerFilter` is new; every other option already existed. All options
compose freely in one call.

The return shape is always `(*Handle[T], error)`. The helpers that used to
return only a handle failed *silently* — a closed bus or nil callback gave you
a nil handle and no explanation. Now the reason comes back as an error. If you
ignore the error, the nil handle is still safe: `Unsubscribe` returns an error
and `IsActive` returns `false` instead of panicking.

### 3. `Publish` returns an error

```go
// Before: errors were silently discarded
bus.Publish("order.created", order)

// After: same call still compiles — the error is ignorable —
bus.Publish("order.created", order)

// — but now you can also ask what failed:
if err := bus.Publish("order.created", order); err != nil {
    log.Printf("delivery failures: %v", err)
}
```

The returned error joins the failures of the **synchronous** handlers
(`errors.Is` works through the join). Asynchronous handler failures are
reported through the `ErrorHandler` only, because the publish call may return
before they run.

### Prometheus adapter

The adapter module follows the same version: upgrade both together.

```bash
go get github.com/townbell/bus@v0.6.0
go get github.com/townbell/bus/prometheus@v0.6.0
```

### Mechanical rewrite hints

The handler-body change (`return nil` insertion) resists sed; expect to do
that part in your editor. The call-site renames are regex-friendly:

```
SubscribeWithHandle\((.*?)\)            → Subscribe($1)
SubscribeAsync\((.*?), (true|false)\)   → Subscribe($1, bus.HandlerAsync($2))
SubscribeOnce\((.*?)\)                  → Subscribe($1, bus.HandlerOnce())
SubscribeWithPriority\((.*?), (.*?)\)   → Subscribe($1, bus.HandlerPriority($2))
SubscribeWithFilter\((.*?), (.*?)\)     → Subscribe($1, bus.HandlerFilter($2))
SubscribeWithOptions\(                  → Subscribe(
```

`SubscribeWithContext(ctx, topic, fn)` moves its context to the end:
`Subscribe(topic, fn, bus.HandlerContext(ctx))`.

After rewriting, `go vet ./...` finds any call site the regexes missed — every
old method name is now an undefined symbol.
