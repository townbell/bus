# Townbell 示例

每个文件都是独立程序，并带有 `//go:build ignore`，不会与库本身的测试套件冲突。

[English](README.md) · [返回项目 README](../README_ZH.md)

## 运行一个示例

```bash
go run worker_example.go
```

请在当前目录执行命令；不要使用 `go run .`，因为每个文件都是独立的 `main` 包。

## 示例地图

| 文件 | 场景 | 涵盖概念 |
| --- | --- | --- |
| `basic_example.go` | 第一个事件总线 | 类型安全订阅、同步/异步、context、一次性 handler |
| `advanced_usage.go` | 派发策略 | 优先级、过滤、错误处理、超时、指标 |
| `middleware_example.go` | 横切逻辑 | 日志、计时、限流、中间件顺序 |
| `e_commerce_example.go` | 业务流程 | 领域总线、优先级、失败补偿 |
| `http_example.go` | HTTP 服务 | 请求 context、模式、dead event、优雅关闭 |
| `worker_example.go` | 本地后台任务 | 异步 handler、限并发、错误钩子、排空 |

## 重要边界

- 这些都是**进程内**示例。`HandlerMaxConcurrency` 限制的是当前进程的工作量；它不是持久化队列，也不是多进程 worker 系统。
- 当派发失败会影响业务结果时，请检查 `Subscribe` 和同步 `Publish` 返回的 `error`。异步失败会进入 `ErrorHandler`。
- 进程有序退出前使用 `WaitAsync` 排空已启动的异步工作；再调用 `Close` 拒绝新工作并等待已有 handler。

完整 API 语义和最小可复制代码请看[项目 README](../README_ZH.md)。
