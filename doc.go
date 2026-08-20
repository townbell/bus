// Package bus provides a dependency-free, type-safe event bus for in-process
// event dispatch.
//
// An EventBus is safe for concurrent use by multiple goroutines. Publish
// delivers events synchronously by default; subscriptions may opt into
// asynchronous delivery, priority, filtering, timeouts, and panic recovery.
// User-supplied handlers, middleware, filters, error hooks, and implementations
// of optional interfaces such as Metrics and Logger must be safe for the
// concurrency enabled by a bus. WaitAsync requires publishers to be quiescent;
// use Close as the concurrent shutdown barrier.
package bus
