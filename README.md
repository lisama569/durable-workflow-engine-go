# Durable Workflow Engine (Go)

一个零外部依赖的本地编排引擎：把由多个步骤组成的工作流跑成**最终状态一致、
过程可解释**的执行。同一条工作流在并发执行、失败重试、进程中断重启后，
已完成的步骤不会被重复执行；重复提交同一次请求也不会重复触发副作用；
工作流定义升级后，历史执行记录会得到稳定的兼容判定。

## 特性

- **DAG 编排**：步骤可声明先后依赖，无依赖的步骤并发执行，并发度可配；
  上游失败时下游自动跳过，绝不会在依赖没跑通的情况下继续执行。
- **并发隔离**：每个步骤有独立的 `StepState` 与尝试键（`AttemptKey`），
  并发分支只能通过上游输出（只读快照）交换数据，互不污染状态。
- **失败重试**：每步独立配置最大尝试次数、初始退避、上限与倍率（指数退避）。
- **断点恢复**：每次尝试前先把 `running + AttemptKey` 落盘，进程退出后重启，
  未完成的尝试被丢弃、重试预算保留，成功步骤不再执行。
- **幂等提交**：同一 idempotency key 的重复提交（含并发双发、跨进程重启）
  解析到同一条执行，副作用最多触发一次。
- **定义兼容**：定义指纹（hash）+ 定义快照随执行持久化。新增步骤安全续跑；
  改动已成功步骤的语义/依赖默认拒绝并保留断点；显式接受新语义时只重放
  受影响步骤及其后继，不影响其他断点。
- **可解释**：状态文件内有 append-only 事件日志，运行日志打印每步输入、
  尝试次数和判定依据。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `workflow/` | 工作流/步骤定义、执行与步骤状态、定义指纹与快照 |
| `store/` | 本地 JSON 持久化（原子写）、跨进程锁、幂等键 |
| `internal/retry/` | 指数退避计算与默认值归一化 |
| `internal/compat/` | 定义 diff 与兼容判定 |
| `engine/` | 调度、并发、重试、恢复、幂等、兼容迁移（含全部场景测试） |
| `cmd/demo/` | 本地可运行的演示程序 |
| `docs/design.md` | 执行模型、重试/恢复语义、兼容规则的完整说明 |

## 快速开始

需要 Go 1.25+。

```bash
# 运行全部测试（无任何外部服务依赖）
go test ./...

# 带竞态检测
go test -race ./...

# 演示：正常执行（含一次重试成功的步骤）
go run ./cmd/demo reset

# 演示：模拟进程中途退出 -> 在新进程里恢复
go run ./cmd/demo crash
go run ./cmd/demo resume

# 演示：同一 idempotency key 重复提交
go run ./cmd/demo idem
```

演示状态写在系统临时目录的 `durable-workflow-demo/` 下，可直接打开其中的
`exec-*.json` 查看每个步骤的状态、尝试历史与事件日志。

## 最小用法

```go
st, _ := store.New("./.workflow-state")
eng := engine.New(st, engine.Options{MaxConcurrency: 4})

def := &workflow.Definition{
    Name: "orders",
    Steps: []workflow.StepDef{
        {ID: "charge", Retry: workflow.RetryPolicy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond},
         Run: func(ctx context.Context, in workflow.StepInput) (any, error) { /* ... */ return nil, nil }},
        {ID: "ship", DependsOn: []string{"charge"},
         Run: func(ctx context.Context, in workflow.StepInput) (any, error) {
             // in.Inputs["charge"] 是上游输出；in.AttemptKey 用于下游去重
             return nil, nil
         }},
    },
}

// idempotencyKey 非空时，重复提交会复用同一条执行
state, err := eng.Run(ctx, "exec-001", "request-abc", def)
```

步骤函数应针对 `AttemptKey` 做到**至少一次投递下的幂等**：引擎保证“成功的
步骤不再执行”，但进程在副作用发生后、落盘前崩溃时，同一尝试键可能在恢复后
再次出现——步骤可用它对外部系统去重。
