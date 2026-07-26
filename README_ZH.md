<p align="center">
  <img src="assets/icon.png" alt="Townbell Bus Icon" width="120">
</p>
<h1 align="center">Townbell Bus</h1>
<p align="center">
  <strong>一个高性能、支持泛型的 Go 事件驱动架构库</strong>
</p>


[![CI](https://github.com/townbell/bus/actions/workflows/ci.yml/badge.svg)](https://github.com/townbell/bus/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue.svg)](https://golang.org/)
[![Go Reference](https://pkg.go.dev/badge/github.com/townbell/bus.svg)](https://pkg.go.dev/github.com/townbell/bus)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/townbell/bus)](https://goreportcard.com/report/github.com/townbell/bus)
[![Coverage](https://img.shields.io/badge/coverage-93.3%25-brightgreen.svg)](https://github.com/townbell/bus/actions/workflows/ci.yml)



一个现代化的、高性能的 Go 事件总线实现，支持泛型、异步处理、优先级、过滤器等企业级功能。

```go
import "github.com/townbell/bus" // 包名为 bus
```

[English Documentation](README.md)

> **v0.6.0 替换了订阅 API。** 一个带选项的 `Subscribe`、形如 `func(ctx, T) error`
> 的 handler、返回 error 的 `Publish`。从 v0.5.x 升级请看
> [MIGRATION_ZH.md](MIGRATION_ZH.md)——改写是机械性的。这套 API 将在 v1.0.0
> 冻结，现在正是提设计反馈的时候。

## ✨ 特性

### 🔧 核心功能
- **类型安全**: 使用 Go 泛型确保编译时类型安全
- **同步/异步**: 支持同步和异步事件处理
- **Handle 模式**: 支持精确的订阅取消
- **一次性订阅**: 支持只触发一次的事件处理器

### 🚀 企业级功能
- **优先级处理**: 支持 4 级优先级（Critical, High, Normal, Low）
- **事件过滤**: 支持自定义过滤器
- **上下文支持**: 支持 context.Context 取消和超时
- **中间件**: 支持事件处理中间件链
- **错误处理**: 完善的错误处理和恢复机制
- **监控指标**: 内置性能监控和统计
- **优雅关闭**: 支持优雅关闭和资源清理

### 🔒 可靠性
- **并发安全**: 线程安全设计
- **Panic 恢复**: 自动恢复处理器 panic
- **资源管理**: 自动资源清理和内存管理

## 📦 安装

```bash
go get github.com/townbell/bus
```

核心模块**没有任何依赖**，引入它只会带进标准库。可选的 Prometheus 适配器是独立模块，
只有你主动安装时 `client_golang` 才会进入你的构建：

```bash
go get github.com/townbell/bus/prometheus
```

## 🚀 快速开始

### 基本使用

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
    // 创建类型安全的事件总线
    eventBus := bus.NewTyped[UserEvent]()
    defer eventBus.Close()

    // 订阅事件
    handle, err := eventBus.Subscribe("user.login", func(ctx context.Context, event UserEvent) error {
        fmt.Printf("用户 %s 执行了 %s\n", event.UserID, event.Action)
        return nil
    })
    if err != nil {
        panic(err)
    }
    defer handle.Unsubscribe()

    // 发布事件。返回值合并了同步 handler 的失败，不关心时可以直接忽略。
    eventBus.Publish("user.login", UserEvent{
        UserID: "user123",
        Action: "login",
    })
}
```

## 📖 详细使用

### 优先级处理

不同优先级的处理器会按优先级顺序执行：

```go
// 高优先级 - 安全检查
securityHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("🔒 安全检查")
    return nil
}, bus.HandlerPriority(bus.PriorityCritical))

// 普通优先级 - 业务逻辑
businessHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("📋 业务处理")
    return nil
}, bus.HandlerPriority(bus.PriorityNormal))

// 低优先级 - 统计分析
analyticsHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Println("📊 数据统计")
    return nil
}, bus.HandlerPriority(bus.PriorityLow))
```

### 事件过滤

只处理符合条件的事件：

```go
// 只处理管理员用户的事件
adminHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("管理员操作: %s\n", event.UserID)
    return nil
}, bus.HandlerFilter(func(topic string, event UserEvent) bool {
    return strings.HasPrefix(event.UserID, "admin_")
}))

// 只处理敏感操作
sensitiveHandle, _ := eventBus.Subscribe("user.action", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("敏感操作告警: %s\n", event.Action)
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

### Topic 模式

订阅的 topic 可以是模式。`"*"` 接收所有事件；末尾的 `".*"` 接收某个前缀之下的
全部事件——`orders.*` 匹配 `orders.created` 和 `orders.created.eu`，但不匹配
`orders` 本身。模式 handler 会与该 topic 的精确 handler 合并，按优先级顺序执行：

```go
// 审计 orders 之下的一切
eventBus.Subscribe("orders.*", func(ctx context.Context, event OrderEvent) error {
    return audit.Record(ctx, event)
}, bus.HandlerPriority(bus.PriorityLow))
```

### Dead Events

发布到无人订阅的 topic 的事件通常意味着 topic 拼错了。dead-event handler
让它们显形，而不是无声消失：

```go
eventBus.SetDeadEventHandler(func(topic string, event OrderEvent) {
    log.Printf("没有订阅者的 topic %q: %+v", topic, event)
})
```

### 上下文控制

使用 context 进行取消和超时控制：

```go
// 上下文取消：取消订阅 context 会停用该 handler
ctx, cancel := context.WithCancel(context.Background())
handle, _ := eventBus.Subscribe("user.session", func(ctx context.Context, event UserEvent) error {
    fmt.Printf("会话事件: %s\n", event.UserID)
    return nil
}, bus.HandlerContext(ctx))

// 取消订阅
cancel()

// 超时发布。截止时间一到，handler 的 ctx 会被取消，
// 配合的 handler 可以提前停止，而不是继续跑完。
err := eventBus.PublishWithTimeout("user.action", event, 5*time.Second)
if err != nil {
    fmt.Printf("发布超时: %v\n", err)
}
```

### 错误处理

handler 通过返回 error 上报业务失败。失败会计入指标、上报给全局 `ErrorHandler`，
并合并进发布调用的返回值——对后续 handler 的派发继续进行：

```go
eventBus.Subscribe("order.created", func(ctx context.Context, event OrderEvent) error {
    if err := reserveStock(ctx, event); err != nil {
        return fmt.Errorf("预留库存: %w", err)
    }
    return nil
})

if err := eventBus.Publish("order.created", order); err != nil {
    log.Printf("投递失败: %v", err) // errors.Is 可以穿透合并后的错误
}
```

设置全局错误处理器可以观察到全部失败，包括异步 handler 的
（它们的错误不会出现在发布调用的返回值里）：

```go
eventBus.SetErrorHandler(func(err *bus.EventError) {
    log.Printf("事件处理错误 - 主题: %s, 错误: %v", err.Topic, err.Err)
})
```

### 中间件

添加处理中间件：

```go
// 日志中间件
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    start := time.Now()
    log.Printf("开始处理事件: %s", topic)
    
    next() // 执行处理器
    
    log.Printf("事件处理完成: %s, 耗时: %v", topic, time.Since(start))
    return nil
})

// 限流中间件
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    if rateLimiter.Allow() {
        next()
        return nil
    }
    return fmt.Errorf("rate limit exceeded")
})
```

### 监控指标

获取运行时指标：

```go
metrics := eventBus.GetMetrics()
published, processed, failed, subscribers := metrics.GetStats()

fmt.Printf("发布事件: %d\n", published)
fmt.Printf("处理事件: %d\n", processed)
fmt.Printf("失败事件: %d\n", failed)
fmt.Printf("活跃订阅者: %d\n", subscribers)

// 获取主题信息
topics := eventBus.GetTopics()
subscriberCount := eventBus.GetSubscriberCount("user.action")
```

默认指标实现还提供 topic / handler 维度的快照：

```go
if detailed, ok := eventBus.GetMetrics().(*bus.DefaultMetrics); ok {
    topicStats := detailed.GetTopicStats()
    handlerStats := detailed.GetHandlerStats()
    fmt.Println(topicStats["user.action"].ProcessedEvents)
    fmt.Println(handlerStats)
}
```

Prometheus 集成放在可选子包中：

```go
import busprom "github.com/townbell/bus/prometheus"

promMetrics := busprom.New(busprom.Config{})
eventBus := bus.NewTyped[UserEvent](
    bus.WithMetrics[UserEvent](promMetrics),
)
```

### 异步处理

```go
// 异步处理，非事务性（并发执行）
_, err := eventBus.Subscribe("user.notification", func(ctx context.Context, event UserEvent) error {
    return sendEmail(ctx, event.UserID)
}, bus.HandlerAsync(false))

// 异步处理，事务性（串行执行）
_, err = eventBus.Subscribe("user.audit", func(ctx context.Context, event UserEvent) error {
    return writeAuditLog(ctx, event)
}, bus.HandlerAsync(true))
```

### Handler 执行控制

所有订阅关注点都是 `HandlerOption`，可以在一次 `Subscribe` 调用中自由组合：

```go
handle, err := eventBus.Subscribe("payment.validate",
    func(ctx context.Context, event PaymentEvent) error {
        return validatePayment(ctx, event)
    },
    bus.HandlerTimeout(2*time.Second),            // 超时到达时取消 ctx
    bus.HandlerRecoverPolicy(bus.RecoverAndStop), // panic 中止本次发布
    bus.HandlerSerial(),                          // 同一时间只执行一个
    bus.HandlerPriority(bus.PriorityHigh),
)
```

可用选项：`HandlerPriority`、`HandlerFilter`、`HandlerContext`、`HandlerAsync`、
`HandlerOnce`、`HandlerTimeout`、`HandlerRecoverPolicy`、`HandlerMaxConcurrency`、
`HandlerSerial`。

## ✅ 行为约定

- `Publish` 和 `PublishWithContext` 返回同步 handler 的失败合并（`errors.Is` 可以穿透）。这个错误可以安全忽略。异步 handler 的失败只通过 `ErrorHandler` 上报，因为发布调用可能在它们运行前就已返回。
- handler 返回错误不会中断派发：后续 handler 照常执行。只有发布 context 被取消、总线关闭、或 handler 在 `RecoverAndStop` 策略下 panic 时才会提前终止派发。
- 同步 handler 在调用发布的 goroutine 中执行，收到从发布调用派生的 context；异步 handler 在独立 goroutine 中执行，收到订阅 context（`HandlerContext`），`HandlerAsync(true)` 时同一 handler 串行执行。
- `HandlerTimeout` 限制发布调用的等待时间，并在超时到达时取消 handler 的 context；无视 context 的 handler 会继续在后台运行，仍会被 `WaitAsync` 和 `Close` 等待。
- handler panic 会被恢复、计入失败并通过 `ErrorHandler` 上报；`RecoverAndContinue`（默认）继续派发，`RecoverAndStop` 中止本次发布。两种情况下恢复的 panic 都会出现在发布错误里。
- middleware 必须调用 `next()` 才会继续执行后续 middleware 和 handler；不调用 `next()` 可用于拦截事件。
- `HandlerOnce` 的 handler 只会成功执行一次，即使同一 topic 下有多个一次性 handler。
- `Close` 后不再接受新发布或订阅；已启动的异步 handler 会在关闭流程中等待完成。
- 订阅被拒绝时（callback 为 nil、filter 类型不匹配、或总线已关闭），`Subscribe` 返回 `(nil, error)`。`nil` handle 可以安全使用：`Unsubscribe` 返回错误、`IsActive` 返回 `false`，因此忽略错误后 `defer handle.Unsubscribe()` 也不会 panic。
- 模式订阅：`"*"` 匹配所有 topic，`"prefix.*"` 匹配严格位于 `prefix.` 之下的所有 topic（不含 `prefix` 本身）。命中的模式 handler 与精确 topic 的 handler 合并后按优先级执行；模式名经过排序，同优先级下顺序是确定的。`HasCallback` 和 `GetSubscriberCount` 仍是精确键查询。
- dead-event handler 在发布找到零个订阅 handler 时触发，先于 middleware。filter 拒绝了事件的订阅者依然算订阅者，不会触发 dead event。它在发布方 goroutine 中同步执行。

## 🗺️ RoadMap

Townbell 会优先保持“进程内、类型安全、轻量事件总线”的定位。后续迭代会参考 Watermill、Blinker、MediatR、Guava EventBus 等项目，但不会把核心库扩成完整的分布式消息系统。

| 优先级 | 是否已完成 | 方向 | 说明 |
| --- | --- | --- | --- |
| P0 | 已完成 | 核心正确性 | 发布路径不再持锁执行 handler，修复 `SubscribeOnce` 移除语义和 middleware 执行链，并补充 race test |
| P0 | 已完成 | API 契约 | 明确 `Publish` / `PublishWithContext` 的错误返回、panic 恢复、同步/异步执行、关闭等边界行为 |
| P1 | 已完成 | 文档对齐 | README 已补充当前 P1 能力和示例 |
| P1 | 已完成 | 可观测性 | 已提供 topic / handler 维度的发布数、处理数、失败数、耗时统计，并提供可选 Prometheus 适配 |
| P1 | 已完成 | 执行控制 | `SubscribeWithOptions` 支持 handler 级 timeout、recover 策略、异步/串行执行和最大并发数 |
| P1 | 已完成 | 持续集成 | GitHub Actions 在 Go 版本矩阵上执行 build、vet、race 测试、gofmt 门禁和覆盖率下限，并显式 vet `example/` |
| P1 | 已完成 | 核心零依赖 | Prometheus 适配器拆为独立模块，引入 `bus` 只会带进标准库 |
| P1 | 已完成 | 可执行文档 | godoc `Example` 函数在 CI 中带输出校验执行，并展示在 pkg.go.dev 上 |
| P0 | 试运行（v0.6.0） | 订阅 API 收敛 | 一个 `Subscribe(topic, fn, opts...) (*Handle[T], error)` 取代了此前的十个变体。将在 v1.0.0 冻结 |
| P0 | 试运行（v0.6.0） | handler 错误上报 | handler 变为 `func(ctx, T) error`：业务失败无需 panic 即可进入指标、`ErrorHandler` 和发布返回值。解锁返回值收集。将在 v1.0.0 冻结 |
| P0 | 试运行（v0.6.0） | `Publish` 错误语义 | `Publish` 返回同步 handler 失败的合并；忽略它依然合法。将在 v1.0.0 冻结 |
| P2 | 未完成 | 返回值收集 | 参考 Blinker，提供 `PublishCollect` 一类 API，收集多个 handler 的返回值或错误 |
| P2 | 已完成 | Topic 增强 | 通配符（`*`）topic、层级 `prefix.*` 模式、以及无订阅者时的 dead-event hook |
| P2 | 部分完成 | 集成示例 | `net/http` 示例已提供；Gin、CLI、worker 待补充 |
| P3 | 未完成 | Broker 桥接 | 参考 Watermill，探索 NATS / Kafka / RabbitMQ 适配器；优先放在独立子包，避免拖重核心库 |
| P3 | 未完成 | Mediator 模式 | 参考 MediatR，按需提供 request / response、command、query、notification 子包 |
| P4 | 未完成 | 状态型能力 | 评估 sticky event、事件回放、本地持久化等能力；仅在有明确场景时加入 |

## 🏗️ 架构设计

该库采用模块化设计，提高了代码的可维护性：

### 文件结构

- **`types.go`** - 核心类型定义（Priority、EventError、过滤器、中间件）
- **`interfaces.go`** - 接口定义（BusSubscriber、BusPublisher、BusController、Bus）
- **`metrics.go`** - 监控和指标功能
- **`handle.go`** - 订阅句柄管理和内部处理器结构
- **`bus.go`** - 核心 EventBus 实现

### 接口分离

```go
// 订阅者接口
type BusSubscriber[T any] interface {
    Subscribe(topic string, fn Handler[T], options ...HandlerOption) (*Handle[T], error)
}

// 发布者接口
type BusPublisher[T any] interface {
    Publish(topic string, event T) error
    PublishWithContext(ctx context.Context, topic string, event T) error
    PublishWithTimeout(topic string, event T, timeout time.Duration) error
}

// 控制器接口
type BusController interface {
    GetMetrics() Metrics
    SetErrorHandler(handler ErrorHandler)
    AddMiddleware(middleware EventMiddleware[any])
    Close() error
    // ...
}
```

### 类型系统

```go
// 事件处理器
type Handler[T any] func(ctx context.Context, event T) error

// 事件过滤器
type EventFilter[T any] func(topic string, event T) bool

// 事件中间件
type EventMiddleware[T any] func(topic string, event T, next func()) error

// 错误处理器
type ErrorHandler func(err *EventError)

// 优先级
type Priority int
const (
    PriorityLow Priority = iota
    PriorityNormal
    PriorityHigh
    PriorityCritical
)
```

## 🔧 最佳实践

### 1. 事件设计模式

参考业界最佳实践，支持以下事件设计模式：

#### Event Notification（事件通知）
```go
type UserCreatedEvent struct {
    UserID    string    `json:"user_id"`
    Timestamp time.Time `json:"timestamp"`
    // 最小化数据，订阅者自行获取详细信息
}
```

#### Event-Carried State Transfer（状态传输）
```go
type UserUpdatedEvent struct {
    UserID       string                 `json:"user_id"`
    Timestamp    time.Time              `json:"timestamp"`
    OldState     map[string]interface{} `json:"old_state"`
    NewState     map[string]interface{} `json:"new_state"`
    ChangedFields []string              `json:"changed_fields"`
}
```

### 2. 命名约定

```go
// 使用点分层级命名
"user.created"
"user.updated"
"user.deleted"
"order.placed"
"order.cancelled"
"payment.processed"
"payment.failed"

// 或使用命名空间
"ecommerce.order.created"
"auth.user.login"
"notification.email.sent"
```

### 3. 错误处理策略

```go
// 设置重试机制
eventBus.SetErrorHandler(func(err *EventError) {
    switch err.Err.(type) {
    case *TemporaryError:
        // 临时错误，稍后重试
        retryQueue.Add(err.Topic, err.Event)
    case *PermanentError:
        // 永久错误，记录并告警
        logger.Error("Permanent error", err)
        alerting.Send(err)
    default:
        // 未知错误，记录详情
        logger.Warn("Unknown error", err)
    }
})
```

### 4. 性能优化

```go
// 使用异步处理非关键路径
eventBus.Subscribe("analytics.track", func(ctx context.Context, event UserEvent) error {
    return analytics.Track(ctx, event) // 非关键的数据统计
}, bus.HandlerAsync(false))

// 关键路径使用同步处理
eventBus.Subscribe("payment.validate", func(ctx context.Context, event PaymentEvent) error {
    return validatePayment(ctx, event) // 关键的支付验证
})

// 使用过滤器减少不必要的处理
eventBus.Subscribe("user.activity", handler, bus.HandlerFilter(
    func(topic string, event UserEvent) bool {
        return event.IsImportant() // 只处理重要事件
    }))
```

## 🔍 与其他库的对比

| 特性 | Townbell | Guava EventBus | RxJava | Node.js EventEmitter |
|------|---------|----------------|---------|---------------------|
| 类型安全 | ✅ 泛型 | ✅ | ✅ | ❌ |
| 异步处理 | ✅ | ❌ | ✅ | ✅ |
| 优先级 | ✅ | ❌ | ❌ | ❌ |
| 过滤器 | ✅ | ❌ | ✅ | ❌ |
| 中间件 | ✅ | ❌ | ✅ | ❌ |
| 错误处理 | ✅ | ⚠️ | ✅ | ⚠️ |
| 监控指标 | ✅ | ❌ | ❌ | ❌ |
| 上下文支持 | ✅ | ❌ | ❌ | ❌ |

## 🧪 测试

运行完整测试套件。Prometheus 适配器是独立模块，需要单独执行：

```bash
go test -race ./...
(cd prometheus && go test -race ./...)
```

`example/` 下的文件带有 `//go:build ignore`，`go vet ./...` 会跳过它们，需要逐个显式指定：

```bash
for f in example/*.go; do go vet "$f"; done
```

生成测试覆盖率报告：

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

**当前覆盖率：核心模块 93.3%**，Prometheus 适配器 79.7%。CI 对核心模块设了 90% 的下限门槛，这个数字不会再悄悄漂移。

运行性能测试：

```bash
go test -bench=. -benchmem
```

## 📈 性能

测试环境：Apple M5（10 核）、Go 1.22.12、darwin/arm64，测量日期 2026-07-26。
所有基准测试都使用 `b.RunParallel`，因此 `ns/op` 是全部核心上的聚合成本，不是单
goroutine 延迟。

| 基准测试 | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `SyncPublish`（1 个订阅者） | 839 | 392 | 11 |
| `AsyncPublish`（1 个订阅者） | 1320 | 520 | 12 |
| `MultipleSubscribers` | 4470 | 752 | 29 |
| `WithPriority` | 532 | 472 | 15 |
| `WithFilter` | 240 | 376 | 10 |
| `ConcurrentSubscribeUnsubscribe` | 3613 | 1207 | 38 |
| `ChannelBaseline`（裸 Go channel） | 50 | 0 | 0 |

这张表里有两点值得注意：

- **异步发布比同步发布慢，而不是快。** 每次异步派发都要启动 goroutine，并操作
  `WaitGroup` 和互斥锁。选择异步是为了把慢 handler 挪出发布 goroutine，不是为了
  提高吞吐。
- **裸 channel 大约便宜 17 倍。** 事件总线换来的是扇出、优先级、过滤器、中间件和
  监控指标；如果你只需要把一个值交给某个已知的 goroutine，channel 才是更合适的工具。

不同硬件上的数字会有差异。请自己重跑基准测试，不要直接采信这张表。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 这个仓库
2. 创建你的特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交你的改动 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启一个 Pull Request

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

本项目参考了以下优秀的开源项目和设计模式：

- [Guava EventBus](https://github.com/google/guava) - Java 生态的经典实现
- [MBassador](https://github.com/bennidi/mbassador) - 高性能 Java EventBus
- [Node.js EventEmitter](https://nodejs.org/api/events.html) - JavaScript 原生事件系统
- [Enterprise Integration Patterns](https://www.enterpriseintegrationpatterns.com/) - 企业集成模式 
