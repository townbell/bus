# Townbell 示例

每个文件都是独立程序，并带有 `//go:build ignore`，不会与库本身的测试套件冲突。

[English](README.md) · [返回项目 README](../README_ZH.md)

## 运行一个示例

```bash
go run worker_example.go
```

请在当前目录执行命令；不要使用 `go run .`，因为每个文件都是独立的 `main` 包。Gin
示例是一个独立的嵌套模块，请进入 `example/gin` 后执行 `go run .`。

## 示例地图

| 文件 | 场景 | 涵盖概念 |
| --- | --- | --- |
| `basic_example.go` | 第一个事件总线 | 类型安全订阅、同步/异步、context、一次性 handler |
| `advanced_usage.go` | 派发策略 | 优先级、过滤、错误处理、超时、指标 |
| `middleware_example.go` | 横切逻辑 | 日志、计时、限流、中间件顺序 |
| `e_commerce_example.go` | 业务流程 | 领域总线、优先级、失败补偿 |
| `http_example.go` | HTTP 服务 | 请求 context、模式、dead event、优雅关闭 |
| `worker_example.go` | 本地后台任务 | 异步 handler、限并发、错误钩子、排空 |
| `gin/` | Gin HTTP 服务 | JSON 绑定、请求 context 派发、异步副作用、信号关闭 |
| `cli_example.go` | 命令行应用 | 参数解析、同步命令结果、进程退出前排空 |

## 集成指南

### Gin

```bash
cd gin && go run .
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"order_id":"o-1001","user_id":"u-1","amount":49.90}'
```

嵌套模块确保 Gin 不会进入核心模块的依赖图。路由只会将同步派发错误返回给
客户端；通知等异步工作由 `ErrorHandler` 报告失败。收到 `SIGINT` 或 `SIGTERM`
后，示例会先完成活跃 HTTP 请求，再排空并关闭总线。

### CLI

```bash
go run cli_example.go -name Ada -repeat 2
```

影响命令退出状态的工作应使用同步 handler。`main` 返回前调用 `WaitAsync`；随后
调用 `Close` 拒绝新任务并等待期间已启动的 handler。

## 重要边界

- 这些都是**进程内**示例。`HandlerMaxConcurrency` 限制的是当前进程的工作量；它不是持久化队列，也不是多进程 worker 系统。
- 当派发失败会影响业务结果时，请检查 `Subscribe` 和同步 `Publish` 返回的 `error`。异步失败会进入 `ErrorHandler`。
- 进程有序退出前使用 `WaitAsync` 排空已启动的异步工作；再调用 `Close` 拒绝新工作并等待已有 handler。

完整 API 语义和最小可复制代码请看[项目 README](../README_ZH.md)。
