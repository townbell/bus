# 迁移指南

[English](MIGRATION.md)

## 从 v0.12.x 迁移到 v0.13.0

v0.13.0 有意在 V1 冻结前使用最后的兼容性调整窗口，修正那些一旦进入 V1 就会
代价高昂的公开契约。

### middleware 改为类型安全

middleware 现在接收总线的事件类型，而不是 `any`：

```go
// 之前
b.AddMiddleware(func(topic string, event any, next func()) error { ... })

// 之后，b 的类型为 *EventBus[Order]
b.AddMiddleware(func(topic string, event Order, next func()) error { ... })
```

这样不再需要类型断言，错误的 middleware 会在编译期失败。

### 移除宽泛的总线接口

`Bus[T]` 和 `BusController` 混合了无关能力，迫使测试替身实现调用方根本不用的
方法。调用方应按真实依赖接收 `BusPublisher[T]`、`BusSubscriber[T]` 或
`BusResultCollector[T]`；需要其他组合时，在消费方旁边声明一个小接口。确实依赖
完整实现的代码可以直接接收 `*EventBus[T]`。

`BusSubscriber[T]` 现在只包含 `Subscribe`；dead-event handler 应在构造阶段通过
具体总线配置。

### Metrics、Logger 与 Prometheus

- 通过 `GetStats` 读取 `DefaultMetrics` 聚合指标；它的可变计数字段不再导出。
- `Logger` 只要求 `Debug`、`Error` 和 `GetLevel`。把 logger 交给总线之前，通过
  `DefaultLogger` 的具体 `SetLevel` 方法完成配置。`NewDefaultLoggerWithOutput`
  现在接受任意 `io.Writer`。
- `prometheus.New` 现在返回 `(*Metrics, error)`，注册失败可由调用方处理；只有当
  注册失败必须终止启动时才使用 `prometheus.MustNew`。

```go
metrics, err := prometheus.New(prometheus.Config{Registerer: registry})
if err != nil {
    return err
}
```

### 错误与生命周期契约

- `EventError` 可解包底层错误，handler 超时会包装
  `context.DeadlineExceeded`，调用方可以使用 `errors.Is` / `errors.As`。
- 即使 middleware 同时失败，`PublishCollect` 也会保留同步 handler 错误；
  middleware 错误在 handler 错误之后按调用链回退顺序返回。
- 未配置有界异步派发时，正数 `HandlerQueueCapacity` 会返回
  `ErrInvalidHandlerOptions`，不再被静默忽略。
- 只在发布方已停止后调用 `WaitAsync`。并发关闭使用 `Close`，并由总线 owner
  调用，不能在本总线的异步 handler 内调用。

## 从 v0.5.x 迁移到 v0.6.0

v0.6.0 替换了订阅 API。这套 API 将在 v1.0.0 冻结；v0.6.0 是它的试运行版本，
如果反馈暴露出设计缺陷仍可能调整。共有三处变更，合起来是对每个订阅点的一次机械性改写。

### 1. handler 签名变为 `func(ctx, T) error`

```go
// 之前
func(event OrderEvent) { process(event) }

// 之后
func(ctx context.Context, event OrderEvent) error {
    return process(ctx, event)
}
```

原因：handler 现在可以不通过 panic 上报业务失败，并且能感知取消。当 handler 超时
或发布方的 context 被取消时，handler 收到的 `ctx` 会被取消——以前总线只能放弃
*等待* handler，现在 handler 自己可以真正停下来。

无需上报时返回 `nil`。返回的错误会计入失败指标、上报给 `ErrorHandler`，并合并进
发布调用的返回值；它**不会**中断对后续 handler 的派发。

### 2. 一个 `Subscribe` 取代全部十个变体

所有订阅统一为 `Subscribe(topic, fn, opts...) (*Handle[T], error)`：

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

`HandlerFilter` 是新增选项；其余选项此前已存在，全部选项可在一次调用中自由组合。

返回形状统一为 `(*Handle[T], error)`。过去只返回 handle 的方法是*静默*失败的——
总线已关闭或回调为 nil 时你只拿到 nil handle，不知道原因；现在原因通过 error 返回。
即使忽略 error，nil handle 依然安全：`Unsubscribe` 返回错误、`IsActive` 返回
`false`，不会 panic。

### 3. `Publish` 返回 error

```go
// 之前：错误被静默丢弃
bus.Publish("order.created", order)

// 之后：同样的写法依旧编译通过——错误可以忽略——
bus.Publish("order.created", order)

// ——但现在也可以知道哪里失败了：
if err := bus.Publish("order.created", order); err != nil {
    log.Printf("投递失败: %v", err)
}
```

返回的错误合并了**同步** handler 的全部失败（`errors.Is` 可以穿透合并后的错误）。
异步 handler 的失败只通过 `ErrorHandler` 上报，因为发布调用可能在它们运行前就已返回。

### Prometheus 适配器

适配器模块跟随同一版本号，请一起升级：

```bash
go get github.com/townbell/bus@v0.6.0
go get github.com/townbell/bus/prometheus@v0.6.0
```

### 机械性改写提示

handler 函数体的改动（补 `return nil`）不适合 sed，建议在编辑器里完成。
调用点的改名是正则友好的：

```
SubscribeWithHandle\((.*?)\)            → Subscribe($1)
SubscribeAsync\((.*?), (true|false)\)   → Subscribe($1, bus.HandlerAsync($2))
SubscribeOnce\((.*?)\)                  → Subscribe($1, bus.HandlerOnce())
SubscribeWithPriority\((.*?), (.*?)\)   → Subscribe($1, bus.HandlerPriority($2))
SubscribeWithFilter\((.*?), (.*?)\)     → Subscribe($1, bus.HandlerFilter($2))
SubscribeWithOptions\(                  → Subscribe(
```

`SubscribeWithContext(ctx, topic, fn)` 的 context 移到末尾：
`Subscribe(topic, fn, bus.HandlerContext(ctx))`。

改写完成后，`go vet ./...` 会找出正则漏掉的调用点——所有旧方法名现在都是未定义符号。
