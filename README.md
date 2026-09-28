# Durable Workflow Engine (Go)

一个纯本地、零外部依赖的持久化工作流编排引擎。工作流被建模为步骤的 **DAG**：
步骤之间可以串行依赖，也可以在依赖满足后**并发执行**。引擎把每一步的状态写入
磁盘，因此同一条工作流在并发、失败重试、进程中断重启之后，最终状态仍然一致、
可重放、可解释。

## 目录结构

```
workflow/
  types.go       运行 / 步骤 / 事件的核心类型
  definition.go  定义校验（重复 id、未知依赖、环）与语义指纹
  executor.go    用户步骤执行器接口与定义注册表
  store.go       Store 接口、FileStore（原子写 + flock）、MemoryStore
  scheduler.go   DAG 调度循环：就绪判定、并发、重试退避、跳过传播、崩溃恢复
  engine.go      Submit 幂等、定义兼容判定（STRICT / ALLOW_ADDITIVE）
  logging.go     结构化逐行日志（输入 / 尝试次数 / 判定依据）
cmd/demo/        本地可运行的示例（并发分支 + flaky 重试 + 崩溃开关）
workflow/*_test.go  17 个测试，含真实子进程崩溃-重启与跨进程竞争
```

## 执行模型

- 一条工作流 = 一个 `Definition`（步骤集合 + 依赖边）；一次执行 = 一个
  `RunID` 对应的 `RunState`。
- 调度按“波次（wave）”推进：
  1. 所有依赖都到达终态（`SUCCEEDED` / `FAILED` / `SKIPPED`）的 `PENDING`
     步骤成为**就绪步骤**；
  2. 就绪步骤通过大小为 `Options.Concurrency` 的信号量并发执行；
  3. 一波全部结束后重新计算，直到没有可推进的步骤。
- **下游保护**：默认策略 `FAIL_ON_DEPENDENCY_FAILURE` 下，任一依赖
  `FAILED`/`SKIPPED`，该步骤立即标记 `SKIPPED` 且**永不执行**，失败沿 DAG
  传播；独立分支不受影响，仍然执行。显式声明
  `ALLOW_DEPENDENCY_FAILURE` 的步骤在依赖失败时照跑，失败依赖不出现在其
  `Inputs` 中，可在 `DependencyStatus` 中查看。
- **并发隔离**：步骤启动前对上游成功输出做一次快照（`StepRequest.Inputs`）；
  并发分支之间不共享可变对象。`MemoryStore` 对状态做深拷贝，`FileStore`
  对每个 run 用 `flock` 串行化写入。
- 终态：所有要求的步骤成功 → `SUCCEEDED`；存在 `FAILED`/`SKIPPED` →
  `FAILED`。

### 状态机

```
PENDING ──依赖满足──▶ RUNNING ──成功──▶ SUCCEEDED
   ▲                    │
   │ 崩溃恢复/重试       ├──失败且仍有额度──▶ (退避后再次 RUNNING)
   └────────────────────┤
                        └──失败且额度耗尽──▶ FAILED
PENDING ──依赖失败且默认策略──▶ SKIPPED
RUN: PENDING → RUNNING → SUCCEEDED | FAILED（终态不可变）
```

## 重试语义

- 每个步骤独立配置 `RetryPolicy`：`MaxAttempts`（含首次，1 = 不重试）、
  `InitialBackoff`、`Multiplier`、`MaxBackoff`。
- 退避公式：`backoff(n) = min(InitialBackoff * Multiplier^(n-1), MaxBackoff)`。
- 每次尝试前后都持久化状态并追加事件（`STEP_STARTED` / `STEP_RETRY` /
  `STEP_SUCCEEDED` / `STEP_FAILED`），detail 中写明错误、当前是第几次尝试、
  退避多久以及判定依据。
- 步骤执行器契约是 **at-least-once**：只有进程在“结果提交之前”崩溃这一种
  情况会导致同一副作用可能发生两次，业务步骤应尽量幂等。

## 断点恢复与幂等语义

- **提交时机**：执行器返回后，引擎先原子写 `state.json`（temp + fsync +
  rename），再向 `events.log` 追加解释事件并 fsync。崩溃只能暴露“上一个
  状态”或“新状态”，不会出现半写状态。
- **重启恢复**：再次对同一 `RunID` 调用 `Submit` 时——
  - `SUCCEEDED` 的步骤**绝不重新执行**；
  - 崩溃时停留在 `RUNNING` 的步骤（结果未知）重置为 `PENDING` 重新调度，
    事件日志中保留历史尝试并记录一条 `STEP_RETRY`（`recovered interrupted
    run`）；
  - 事件序号从日志中恢复，保证单调连续。
- **重复提交幂等**：终态 run 再次 `Submit` 直接返回已存终态，不触发任何
  副作用；同一进程内用 run 级互斥锁串行化，跨进程靠 `flock` +
  “仅当 state.json 不存在时创建”保证只有一个创建者胜出。
- 磁盘布局（`FileStore`）：

```
<root>/<run-id>/
  state.json   最新快照（原子替换，恢复的权威依据）
  events.log   追加式 JSONL 判定日志
  lock         flock 文件
```

## 定义兼容规则

定义在注册时计算一个 SHA-256 **语义指纹**，参与哈希的只有影响执行的字段：
步骤 id、依赖集合、失败策略、重试策略、静态输入；步骤声明顺序、依赖列出
顺序的变化不改变指纹。每条 run 创建时把当时的指纹（以及每个步骤自己的
单步指纹）写入状态。

1. **STRICT（默认）**：历史 run 的指纹与当前定义不一致时，`Submit` 返回
   `ErrDefinitionChanged`，**不改动磁盘上的断点，也不执行任何步骤**——
   旧记录不会被静默按新语义执行，也不会被丢弃。注册表同样拒绝用同名注册
   语义不同的新定义；不兼容的新版本应注册为新名字（版本化定义，如
   `svc-v2`），旧 run 继续用旧定义恢复。
2. **ALLOW_ADDITIVE（显式逃生舱）**：仅允许“纯新增步骤”的升级——
   - run 已知的每个步骤，其单步指纹必须与新定义完全一致（改依赖/策略/输入
     仍然报 `ErrDefinitionChanged`）；
   - 删除已知步骤同样被拒绝；
   - 新增步骤被初始化为 `PENDING` 后继续调度；
   - **已经到达终态的 run 在任何模式下都不会被重新打开**，新增步骤不会
     补跑。

## 日志（可解释性）

每行包含 run id、全局递增 seq、事件类型、步骤 id、尝试次数与判定细节，例如：

```
run=r1 seq=6 kind=STEP_RETRY step=flaky attempt=2 detail="error=\"boom\"; verdict=retry 3/3 after 20ms backoff"
run=r1 seq=9 kind=STEP_SKIPPED step=after detail="dependency boom ended FAILED; step never executes"
```

## 本地验证

无需任何外部服务（无数据库、无网络），只需要 Go 1.25：

```bash
# 全部测试（-race 可选；含真实子进程崩溃重启，约 10 秒）
go test -race -count=1 ./...

# 只跑崩溃重启场景并观察详细日志
go test -count=1 -v ./workflow/ -run TestRealProcessCrashAndRestart

# 交互式 demo：并发分支 + flaky 步骤前两次失败
go run ./cmd/demo -run demo-1 -data ./workflow-data

# 再跑一次：完全相同的 RunID，所有步骤 0 次执行（幂等重放）
go run ./cmd/demo -run demo-1 -data ./workflow-data

# 模拟“进程在 merge 步骤前硬退出”，再用同一 RunID 恢复
go run ./cmd/demo -run demo-2 -data ./workflow-data -crash merge
go run ./cmd/demo -run demo-2 -data ./workflow-data
```

测试覆盖的关键场景：

| 测试 | 断言 |
| --- | --- |
| `TestConcurrentBranchesIsolated` | 三分支真实重叠执行；各自只看到自己上游的输出；join 看到全部输出；每步恰好 1 次 |
| `TestRetryThenSuccessPersistsAttempts` | 失败 2 次第 3 次成功，`Attempts==3`，事件序列 STARTED,RETRY,RETRY,SUCCEEDED |
| `TestRetryExhaustedSkipsDownstream` | 重试用尽后下游 `SKIPPED` 且 0 次执行，独立分支仍成功 |
| `TestRealProcessCrashAndRestart` | 子进程 `os.Exit(1)` 后新进程恢复：已完成步骤跨进程 0 次重跑，终态 `SUCCEEDED` |
| `TestDuplicateSubmitIsIdempotent` / `...AcrossProcesses` | 同 RunID 重复提交（进程内并发 / 跨进程）副作用恰好 1 次 |
| `TestDefinitionChangeStrictKeepsCheckpoint` | 指纹不一致报错，断点原样保留，0 次执行 |
| `TestDefinitionChangeAdditive` | 纯新增步骤可恢复续跑；改动已知步骤仍拒绝；终态 run 不重开 |
