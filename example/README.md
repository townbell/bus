# Townbell examples

Each file is a standalone program. It carries `//go:build ignore`, so it does
not conflict with the library test suite.

[中文文档](README_ZH.md) · [Back to the project README](../README.md)

## Run one

```bash
go run worker_example.go
```

Run commands from this directory; do not use `go run .` because every file is a
separate `main` package. The Gin example is a self-contained nested module, so
run it from `example/gin` with `go run .`.

## Example map

| File | Scenario | Concepts |
| --- | --- | --- |
| `basic_example.go` | First event bus | Typed subscriptions, sync/async work, context, once handlers |
| `advanced_usage.go` | Dispatch policy | Priority, filters, error handling, timeout, metrics |
| `middleware_example.go` | Cross-cutting work | Logging, timing, rate limiting, middleware order |
| `e_commerce_example.go` | Business flow | Domain buses, priorities, failure compensation |
| `http_example.go` | HTTP service | Request context, patterns, dead events, graceful shutdown |
| `worker_example.go` | Local background jobs | Async handlers, bounded concurrency, error hook, draining |
| `gin/` | Gin HTTP service | JSON binding, request-context dispatch, async side effects, signal shutdown |
| `cli_example.go` | Command-line app | Flags, synchronous command result, draining before process exit |

## Integration guides

### Gin

```bash
cd gin && go run .
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"o-1001","user_id":"u-1","amount":49.90}'
```

The nested module keeps Gin out of the core dependency graph. The route returns
an error only for synchronous dispatch; notification work is asynchronous and
reports failures to `ErrorHandler`. On `SIGINT` or `SIGTERM`, HTTP shutdown
finishes active requests before the example drains and closes the bus.

### CLI

```bash
go run cli_example.go -name Ada -repeat 2
```

Use synchronous handlers for work that decides the command's exit status. Call
`WaitAsync` before returning from `main`, then call `Close` to reject new work
and wait for any handler that began concurrently.

## Important boundaries

- These are **in-process** examples. `HandlerMaxConcurrency` bounds work in the
  current process; it is not a durable queue or a multi-process worker system.
- Check the `error` returned by `Subscribe` and synchronous `Publish` when
  delivery failures matter. Async failures arrive at `ErrorHandler`.
- Use `WaitAsync` before an orderly process exit, then `Close` to reject new
  work and wait for already-started handlers.

For API semantics and the smallest copyable snippet, see the
[main README](../README.md).
