# Migrating from v0.5.x to v0.6.0

[中文版](MIGRATION_ZH.md)

v0.6.0 replaces the subscription API. This is the API that will freeze at
v1.0.0; v0.6.0 is its trial run, and feedback that surfaces design flaws can
still change it. There are three changes, and they compose into one mechanical
rewrite of each subscription site.

## 1. The handler signature is now `func(ctx, T) error`

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

## 2. One `Subscribe` method replaces all ten variants

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

## 3. `Publish` returns an error

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

## Prometheus adapter

The adapter module follows the same version: upgrade both together.

```bash
go get github.com/townbell/bus@v0.6.0
go get github.com/townbell/bus/prometheus@v0.6.0
```

## Mechanical rewrite hints

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
