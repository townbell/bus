<p align="center">
  <img src="assets/icon.png" alt="Townbell Bus Icon" width="120">
</p>
<h1 align="center">Townbell Bus</h1>
<p align="center">
  <strong>一个高性能、支持泛型的 Go 事件驱动架构库</strong>
</p>


[![Go Version](https://img.shields.io/badge/go-%3E%3D1.19-blue.svg)](https://golang.org/)
[![Go Reference](https://pkg.go.dev/badge/github.com/townbell/bus.svg)](https://pkg.go.dev/github.com/townbell/bus)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/townbell/bus)](https://goreportcard.com/report/github.com/townbell/bus)
[![Coverage](https://img.shields.io/badge/coverage-91.2%25-brightgreen.svg)](https://github.com/townbell/bus)



一个现代化的、高性能的 Go 事件总线实现，支持泛型、异步处理、优先级、过滤器等企业级功能。

```go
import "github.com/townbell/bus" // 包名为 bus
```

[English Documentation](README.md)

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

## 🚀 快速开始

### 基本使用

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
    // 创建类型安全的事件总线
    eventBus := bus.NewTyped[UserEvent]()
    defer eventBus.Close()

    // 订阅事件
    handle := eventBus.SubscribeWithHandle("user.login", func(event UserEvent) {
        fmt.Printf("用户 %s 执行了 %s\n", event.UserID, event.Action)
    })
    defer handle.Unsubscribe()

    // 发布事件
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
securityHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("🔒 安全检查")
}, bus.PriorityCritical)

// 普通优先级 - 业务逻辑
businessHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("📋 业务处理")
}, bus.PriorityNormal)

// 低优先级 - 统计分析
analyticsHandle := eventBus.SubscribeWithPriority("user.action", func(event UserEvent) {
    fmt.Println("📊 数据统计")
}, bus.PriorityLow)
```

### 事件过滤

只处理符合条件的事件：

```go
// 只处理管理员用户的事件
adminHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
    fmt.Printf("管理员操作: %s\n", event.UserID)
}, func(topic string, event UserEvent) bool {
    return strings.HasPrefix(event.UserID, "admin_")
})

// 只处理敏感操作
sensitiveHandle := eventBus.SubscribeWithFilter("user.action", func(event UserEvent) {
    fmt.Printf("敏感操作告警: %s\n", event.Action)
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

### 上下文控制

使用 context 进行取消和超时控制：

```go
// 上下文取消
ctx, cancel := context.WithCancel(context.Background())
handle := eventBus.SubscribeWithContext(ctx, "user.session", func(event UserEvent) {
    fmt.Printf("会话事件: %s\n", event.UserID)
})

// 取消订阅
cancel()

// 超时发布
err := eventBus.PublishWithTimeout("user.action", event, 5*time.Second)
if err != nil {
    fmt.Printf("发布超时: %v\n", err)
}
```

### 错误处理

设置全局错误处理器：

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
err := eventBus.SubscribeAsync("user.notification", func(event UserEvent) {
    sendEmail(event.UserID)
}, false)

// 异步处理，事务性（串行执行）
err := eventBus.SubscribeAsync("user.audit", func(event UserEvent) {
    writeAuditLog(event)
}, true)
```

### Handler 执行控制

`SubscribeWithOptions` 可以为单个 handler 设置 timeout、recover 策略和并发控制：

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

## ✅ 行为约定

- `Publish` 会忽略返回错误；需要感知取消、超时或关闭错误时，请使用 `PublishWithContext` 或 `PublishWithTimeout`。
- 同步 handler 默认在当前 goroutine 执行；异步 handler 会在独立 goroutine 执行，`transactional=true` 时同一 handler 串行执行。
- handler timeout 限制的是发布调用等待时间，不会强制终止已经开始运行的 handler。
- handler panic 默认会被恢复，失败计数会增加，并通过 `ErrorHandler` 上报；`RecoverAndStop` 会让已恢复的 panic 停止当前发布流程。
- middleware 必须调用 `next()` 才会继续执行后续 middleware 和 handler；不调用 `next()` 可用于拦截事件。
- `SubscribeOnce` / `SubscribeOnceAsync` 的 handler 只会成功执行一次，即使同一 topic 下有多个一次性 handler。
- `Close` 后不再接受新发布或订阅；已启动的异步 handler 会在关闭流程中等待完成。

## 🗺️ RoadMap

Townbell 会优先保持“进程内、类型安全、轻量事件总线”的定位。后续迭代会参考 Watermill、Blinker、MediatR、Guava EventBus 等项目，但不会把核心库扩成完整的分布式消息系统。

| 优先级 | 是否已完成 | 方向 | 说明 |
| --- | --- | --- | --- |
| P0 | 已完成 | 核心正确性 | 发布路径不再持锁执行 handler，修复 `SubscribeOnce` 移除语义和 middleware 执行链，并补充 race test |
| P0 | 已完成 | API 契约 | 明确 `Publish` / `PublishWithContext` 的错误返回、panic 恢复、同步/异步执行、关闭等边界行为 |
| P1 | 已完成 | 文档对齐 | README 已补充当前 P1 能力和示例 |
| P1 | 已完成 | 可观测性 | 已提供 topic / handler 维度的发布数、处理数、失败数、耗时统计，并提供可选 Prometheus 适配 |
| P1 | 已完成 | 执行控制 | `SubscribeWithOptions` 支持 handler 级 timeout、recover 策略、异步/串行执行和最大并发数 |
| P2 | 未完成 | 返回值收集 | 参考 Blinker，提供 `PublishCollect` 一类 API，收集多个 handler 的返回值或错误 |
| P2 | 部分完成 | Topic 增强 | 通配符（`*`）topic 已实现；层级 topic、无订阅者事件 hook 仍待开发 |
| P2 | 未完成 | 集成示例 | 补充 `net/http`、Gin、CLI、worker 等实际项目中的使用方式 |
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
    Subscribe(topic string, fn func(T)) error
    SubscribeWithPriority(topic string, fn func(T), priority Priority) *Handle[T]
    SubscribeWithFilter(topic string, fn func(T), filter EventFilter[T]) *Handle[T]
    SubscribeWithContext(ctx context.Context, topic string, fn func(T)) *Handle[T]
    // ...
}

// 发布者接口
type BusPublisher[T any] interface {
    Publish(topic string, event T)
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
eventBus.SubscribeAsync("analytics.track", func(event UserEvent) {
    // 非关键的数据统计
    analytics.Track(event)
}, false)

// 关键路径使用同步处理
eventBus.Subscribe("payment.validate", func(event PaymentEvent) {
    // 关键的支付验证
    validatePayment(event)
})

// 使用过滤器减少不必要的处理
eventBus.SubscribeWithFilter("user.activity", handler, func(topic string, event UserEvent) bool {
    return event.IsImportant() // 只处理重要事件
})
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

运行完整测试套件：

```bash
go test -v ./...
```

生成测试覆盖率报告：

```bash
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

**当前测试覆盖率：91.2%**（全模块口径，`go test -coverprofile ./...`）- 我们保持高测试覆盖率以确保可靠性和稳定性。

运行性能测试：

```bash
go test -bench=. -benchmem
```

## 📈 性能

基于 Go 1.19+ 的基准测试结果：

- **同步发布**: ~2,000,000 events/sec
- **异步发布**: ~5,000,000 events/sec
- **内存使用**: 极低的 GC 压力
- **并发性能**: 优秀的多核扩展性

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
