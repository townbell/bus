# Townbell 示例

这个目录包含了展示 Townbell 库功能和特性的全面示例。

## 环境要求

- Go 1.19 或更高版本
- 所有示例使用 `//go:build ignore` 来避免运行 `go test ./...` 时发生冲突
- 示例必须单独运行，使用 `go run filename.go`

## 示例概览

### 1. `basic_example.go` - 基础用法
**演示内容：**
- 基本事件订阅和发布
- 使用泛型的类型安全事件总线
- 同步和异步处理器
- 基于上下文的取消
- 同一事件的多个订阅者
- 一次性事件处理器
- 检查回调是否存在

**运行方式：**
```bash
go run basic_example.go
```

### 2. `advanced_usage.go` - 高级功能
**演示内容：**
- 使用泛型的类型安全事件总线
- 基于优先级的事件处理
- 使用自定义过滤器的事件过滤
- 基于上下文的取消
- 错误处理和恢复
- 监控和指标收集
- 超时处理

**运行方式：**
```bash
go run advanced_usage.go
```

### 3. `middleware_example.go` - 中间件系统
**演示内容：**
- 请求跟踪中间件
- 性能监控的计时中间件
- 限流中间件
- 中间件链和执行顺序

**运行方式：**
```bash
go run middleware_example.go
```

### 4. `e_commerce_example.go` - 真实世界场景
**演示内容：**
- 复杂事件驱动工作流（电商订单处理）
- 不同领域的多个事件总线
- 通过事件进行服务交互
- 基于优先级的处理（安全、业务逻辑、通知）
- 错误场景和补偿模式

**运行方式：**
```bash
go run e_commerce_example.go
```

## 核心概念演示

### 使用泛型的类型安全
```go
// 创建类型安全的事件总线
eventBus := bus.NewTyped[UserEvent]()

// 所有事件必须匹配指定类型
eventBus.Publish("user.login", UserEvent{...})
```

### 优先级处理
```go
// 关键优先级 - 最先执行
securityHandle := eventBus.SubscribeWithPriority("user.action", 
    securityHandler, bus.PriorityCritical)

// 低优先级 - 最后执行
analyticsHandle := eventBus.SubscribeWithPriority("user.action", 
    analyticsHandler, bus.PriorityLow)
```

### 事件过滤
```go
// 只处理管理员用户
adminHandle := eventBus.SubscribeWithFilter("user.action", handler,
    func(topic string, event UserEvent) bool {
        return strings.HasPrefix(event.UserID, "admin_")
    })
```

### 上下文取消
```go
ctx, cancel := context.WithCancel(context.Background())
handle := eventBus.SubscribeWithContext(ctx, "user.session", handler)

// 取消所有使用此上下文的订阅
cancel()
```

### 错误处理
```go
eventBus.SetErrorHandler(func(err *bus.EventError) {
    log.Printf("主题 '%s' 中的事件错误: %v", err.Topic, err.Err)
})
```

### 中间件
```go
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    // 预处理
    log.Printf("处理中: %s", topic)
    
    next() // 执行处理器
    
    // 后处理
    log.Printf("完成: %s", topic)
    return nil
})
```

## 运行所有示例

按顺序运行所有示例：

```bash
# 基础用法
echo "=== 运行基础示例 ===" && go run basic_example.go

# 高级功能
echo "=== 运行高级示例 ===" && go run advanced_usage.go

# 中间件系统
echo "=== 运行中间件示例 ===" && go run middleware_example.go

# 电商工作流
echo "=== 运行电商示例 ===" && go run e_commerce_example.go
```

## 演示的最佳实践

1. **资源管理**: 总是使用 `defer` 来取消订阅句柄
2. **错误处理**: 设置全局错误处理器进行监控
3. **性能**: 使用适当的优先级和异步处理
4. **类型安全**: 利用泛型进行编译时类型检查
5. **优雅关闭**: 总是使用 `defer eventBus.Close()` 关闭事件总线

## 性能考虑

- **同步 vs 异步**: 对非关键操作使用异步
- **优先级**: 使用优先级确保关键操作优先执行
- **过滤器**: 应用过滤器减少不必要的处理
- **上下文**: 使用上下文进行超时和取消控制
- **中间件**: 保持中间件轻量以避免性能影响

## 常见模式

### 事件溯源
```go
// 存储所有事件以便重放
eventBus.Subscribe("*", func(event interface{}) {
    eventStore.Save(event)
})
```

### CQRS (命令查询责任分离)
```go
// 分离命令和查询处理
commandBus := bus.NewTyped[Command]()
queryBus := bus.NewTyped[Query]()
```

### Saga 模式
```go
// 协调分布式事务
orderBus.Subscribe("order.created", func(order Order) {
    // 开始 saga
    paymentBus.Publish("payment.process", Payment{...})
})

paymentBus.Subscribe("payment.failed", func(payment Payment) {
    // 补偿
    orderBus.Publish("order.cancel", Order{...})
})
```

## 故障排除

### 常见问题

1. **构建错误**: 如果遇到 "main redeclared" 错误，确保使用 `go run filename.go` 而不是 `go run .`
2. **导入错误**: 确保你的项目有正确的 `go.mod` 文件
3. **版本兼容性**: 确保使用 Go 1.19 或更高版本

### 调试技巧

```go
// 启用详细日志
eventBus.AddMiddleware(func(topic string, event interface{}, next func()) error {
    fmt.Printf("DEBUG: 处理事件 %s: %+v\n", topic, event)
    next()
    return nil
})

// 检查订阅者数量
fmt.Printf("主题 '%s' 的订阅者数量: %d\n", topic, eventBus.GetSubscriberCount(topic))

// 获取所有主题
fmt.Printf("所有主题: %v\n", eventBus.GetTopics())
```

## 进一步学习

- 查看主项目的 [README.md](../README.md) 了解完整的 API 文档
- 查看 [README_ZH.md](../README_ZH.md) 了解中文版本的完整文档
- 查看源代码了解实现细节
- 运行基准测试了解性能特征: `go test -bench=. -benchmem` 