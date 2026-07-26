# 从 v0.5.x 迁移到 v0.6.0

[English](MIGRATION.md)

v0.6.0 替换了订阅 API。这套 API 将在 v1.0.0 冻结；v0.6.0 是它的试运行版本，
如果反馈暴露出设计缺陷仍可能调整。共有三处变更，合起来是对每个订阅点的一次机械性改写。

## 1. handler 签名变为 `func(ctx, T) error`

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

## 2. 一个 `Subscribe` 取代全部十个变体

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

## 3. `Publish` 返回 error

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

## Prometheus 适配器

适配器模块跟随同一版本号，请一起升级：

```bash
go get github.com/townbell/bus@v0.6.0
go get github.com/townbell/bus/prometheus@v0.6.0
```

## 机械性改写提示

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
