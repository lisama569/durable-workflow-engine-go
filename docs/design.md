# 设计说明：执行模型、重试与恢复、定义兼容

## 1. 执行模型

### 1.1 DAG 调度

- 每个 `StepDef` 用 `DependsOn` 声明依赖；定义校验拒绝重复 id、未知依赖、
  自依赖和有环图（`workflow.Definition.Validate`）。
- 调度器循环做决策：所有依赖都为 `succeeded` 的步骤**就绪**；任一依赖为
  `failed`/`skipped` 的步骤标记为 `skipped`；其余等待。因此上游失败时
  下游永远不会启动（"依赖没跑通就不往下走"）。
- 无依赖/依赖均已成功的步骤通过带缓冲的 concurrency 信号量并发执行，
  最大并发度由 `Options.MaxConcurrency` 限定。
- 步骤函数在**不持有调度状态锁**时运行（用户代码、退避等待期间不阻塞
  其他并发步骤）；状态变更与持久化在短临界区内完成。并发步骤的状态分属
  `ExecutionState.Steps` 里不同的 `StepState`，步骤间只能读到上游步骤的
  输入快照（在尝试开始时拷贝的 `Inputs`），因此分支间状态互不污染。

- 执行终态：全部非跳过步骤成功 => `succeeded`；存在失败步骤 => `failed`
  （失败步骤的下游全部为 `skipped`，独立分支仍可成功）。
- 独立分支不受其他分支失败影响：仅失败步骤的传递依赖被跳过。

### 1.2 状态机

```
pending -> running -> succeeded          （成功，终态）
                   \-> pending  (失败但还有重试额度，等退避后继续)
                   \-> failed   （尝试额度耗尽，终态）
pending -> skipped                        （上游 failed/skipped，终态）
running -> pending (crash 恢复 / 尝试中取消，不计新的尝试)
```

注：尝试中遇到 context 取消时，该尝试**不占用重试预算**；步骤停留在
`pending`，历史记录标记为 interrupted，留待恢复。

### 1.3 持久化与跨进程互斥

- 每个执行一个 JSON 文档（`exec-<id>.json`），用临时文件 + fsync + rename
  原子替换，保证读到的永远是完整的某一版状态。
- 每个执行一个锁文件（`locks/<id>.lock`），同一时刻只有一个调度器在跑
  同一条执行；锁内记录 PID/主机名/进程启动时间，属主进程消失后自动回收
  （仅同主机、且确认是不同的已退出进程时才回收）。
- **锁顺序固定为 idem-key 锁 → execution 锁**：幂等键锁在 `Run` 入口获取并
  持有到结束，随后才获取执行锁。反序（先前实现）会在两个相同 key、不同
  executionID 的并发提交间形成环路死锁，导致一次提交超时后重复建执行。
- 锁文件采用 O_EXCL 创建，创建与写载荷之间存在极短窗口，期间文件可能为空。
  回收逻辑凭 PID + 进程启动时间识别属主：**本进程持有的锁（含尚未写全的空
  文件）绝不回收**，避免同进程多 goroutine 互相误删活跃锁。
- 持久化时机：执行创建时；每个波次决策后；每次尝试开始前；每次尝试结束
  （成功/失败/中断）后；执行终态时。

### 1.4 幂等提交

- 提交时可用 `idempotencyKey`。引擎先在“加 per-key 锁 + 原子发布”保护下
  抢占 key→executionID 的映射文件（`idem-<hash>.json`）；抢输的一方读取
  映射并**采纳赢家的 executionID**，后续全部在同一条执行上进行。
- 已完成执行再次提交：加载到的已是终态，调度器没有任何可推进的步骤，直接
  返回原状态，不会触发任何步骤函数。
- 因此同一次业务请求（相同 key）在并发双发或重启后重放时，每个成功步骤
  最多执行一次。

## 1.5 尝试键与副作用去重

每次尝试开始前生成全局唯一的 `AttemptKey = "<execID>:<stepID>:a<n>"`，
随状态落盘并传入步骤函数（`StepInput.AttemptKey`）。它用于：

1. 日志/历史中区分每一次尝试；
2. 若进程在**外部副作用已发生但结果尚未落盘**的窗口内崩溃，恢复后会用
   **相同的尝试序号与键**重新调用步骤——步骤（或其下游外部系统）应据此
   对外部操作去重。这是"至少一次投递 + 键控幂等"模型：引擎内部保证
   成功结果不丢、成功步骤不重复执行，但外部副作用的精确去重需要步骤
   配合 AttemptKey 实现。

## 2. 重试与恢复语义

### 2.1 重试策略（每步独立配置）

`RetryPolicy{MaxAttempts, BaseDelay, MaxDelay, Multiplier}`：

- `MaxAttempts` 是**包含首次在内的总尝试次数**；`1` 表示不重试；`0` 走
  默认值（3 次）；负数归一化为 1。
- 退避：第 n 次尝试前等待 `BaseDelay * Multiplier^(n-2)`，超过 `MaxDelay`
  则截断；负数 `MaxDelay` 表示不设上限。默认 100ms 起步、2 倍增长、5s 封顶。
- 每次失败都落盘历史（`AttemptRecord`）并记录决策（`decision=retry` /
  `decision=fail_exhausted`）。退避等待可被 context 取消，取消时步骤保持
  `pending`、尝试次数不变，下次运行接着退避/重试。

### 2.2 中断恢复

- 启动或续跑时，若发现步骤停在 `running`（上次进程在尝试窗口内退出），
  恢复逻辑把它重置为 `pending`，清空 AttemptKey，在历史里把该尝试标注为
  "interrupted by process exit before attempt completed"，并记事件
  `attempt_recovered`。**尝试计数保留**（重试预算确实在崩溃边界前被消耗），
  仅未完成尝试本身被丢弃。
- 停在退避等待中的步骤本身就是 `pending`，正常续跑。
- `succeeded` 步骤有结果快照，直接跳过，不再次执行。
- 整个恢复过程可从状态文件与事件日志完整解释。

## 2.3 并发正确性要点

- 用户函数在锁外运行；所有 `StepState` 读写与 `Save` 都在同一把状态锁内，
  且每次状态变更后立即持久化，避免跨步骤的状态交错。
- 并发信号量在步骤 goroutine **内部**获取（从不在持状态锁时阻塞等槽位），
  否则槽满 + 完成步骤要抢锁记账会自死锁。
- 完成通道按步骤总数缓冲，收尾 goroutine 不会因为调度器暂时没收消息而
  阻塞；调度器每收到至少一个完成信号就重新决策。

## 3. 定义兼容规则

### 3.1 指纹与快照

- `Definition.Hash()` 对**步骤 id、依赖边、每步 Version、Metadata、Retry
  策略**以及工作流名做稳定哈希（声明顺序无关）。Go 函数无法反射比较，
  因此修改步骤体语义时必须显式提升 `StepDef.Version`（或改 Metadata/
  Retry），否则引擎无法感知——这是刻意的显式声明设计。
- 执行状态里同时持久化 `definition_hash` 和完整的**函数无关定义快照**
  （`DefinitionSnapshot`），即使进程里没有旧版本 Go 代码，也能对历史记录
  做稳定 diff，而不是用当前代码静默重解释旧断点。
- 顶层 `Definition.Version` 本身**不进哈希**：纯版本号变化不构成语义变化；
  迁移后会更新 `DefinitionVer` 与 `MigratedFromHash` 作为溯源。

### 3.2 diff 分类与判定（`internal/compat`）

| 变化 | 对历史记录的影响 | 默认判定 |
| --- | --- | --- |
| 新增步骤 | 从未运行过，其上游输出齐全，续跑时自然执行 | 安全续跑 |
| 删除从未成功的步骤 | 无记录可质疑 | 安全续跑 |
| 删除已成功步骤 / 其直接下游已成功 | 旧断点包含已不存在的语义 | **拒绝**（不断点丢弃） |
| 修改步骤语义（Version/Metadata/Retry）但该步从未成功 | 新语义在首次执行时自然生效 | 安全续跑 |
| 修改已成功步骤的语义 | 记录结果按旧语义产生，存疑 | **拒绝** |
| 修改已成功步骤的依赖边 | 本步结果及其后继结果的输入关系变了 | **拒绝** |

- 默认模式 `RejectOnConflict`：不安全时返回 `ErrDefinitionConflict`
  （`ConflictError` 里带逐项变化清单），**磁盘断点原封不动**，不会静默按
  新语义执行，也不会丢弃断点。
- `AcceptCompatible`：仅当 diff 不影响任何已有结果（纯新增/只涉及没跑过
  的步骤）时自动续跑。
- `AcceptNewSemantics`：显式接受新定义。受影响步骤（语义/依赖变化的步骤）
  及其**在新 DAG 上的全部后继**若有历史记录则重置为 `pending` 重放，其余
  步骤保留断点；被删除的步骤移除其断点。状态记录 `definition_migrated`
  事件、每步 `step_reset` 事件以及 `MigratedFromHash`，全程可解释。
- 注意：删除已成功步骤在任何自动模式下都不迁移（需要人工裁决），
  `AcceptNewSemantics` 只重放仍存在于新定义中的步骤。

### 3.3 为什么不静默升级

旧记录里的成功结果可能已对外产生副作用；用新定义的语义悄悄重判会让
"最终状态"无法对应实际发生过的事情。因此策略是：**能证明无影响才自动
继续；有影响必须显式选择（拒绝并保留现场，或显式迁移并重放）**。

## 4. 本地验证

```bash
go test ./...          # 全量单测/集成测试
go test -race ./...    # 竞态检测（CI 建议）
go vet ./...
go run ./cmd/demo reset
go run ./cmd/demo crash && go run ./cmd/demo resume
go run ./cmd/demo idem
```

### 4.1 测试覆盖的场景（`engine/engine_test.go`）

1. `TestDAGRunsInOrderAndConcurrently`：依赖顺序 + 独立步骤真实重叠执行，
   断言每步恰好一次、join 收到两个上游的正确输出（输入隔离）。
2. `TestConcurrentBranchesStateIsolation`：20 条并发分支 + join，断言每步
   一次、输出不串扰。
3. `TestFailureSkipsDownstream`：上游失败 => 下游 skipped 且执行 0 次；
   独立分支仍成功；日志含判定依据。
4. `TestRetryThenSuccess` / `TestRetryExhausted`：断言尝试次数、退避后
   成功与额度耗尽失败。
5. `TestResumeAfterCrashSkipsSucceededSteps` /
   `TestResumeSkipsAlreadySucceeded`：用取消模拟进程退出，重启后续跑，
   断言成功步骤不重复执行、中断步骤按尝试计数恢复。
6. `TestResumeAfterCrashDuringBackoff`：在退避等待中"杀死"进程，新引擎
   从同一目录恢复并成功。
7. `TestIdempotentDoubleSubmit`：相同 key 并发双发只产生一条执行、
   步骤只跑一次。
8. `TestResubmitFinishedExecution`：终态执行重复提交直接返回，不重放。
9. `TestDefinitionAddStepSafe` / `TestDefinitionChangeRejectedByDefault` /
   `TestDefinitionChangeAcceptNewSemantics`：新增步骤续跑、冲突拒绝且
   断点保留、显式迁移只重放受影响步骤及后继。

所有测试只用临时目录和内存同步原语，**不依赖任何数据库、网络或外部
服务**。

## 5. 已知边界

- 单机本地存储，不做多机协调；跨主机的陈旧锁不会被自动回收（避免误杀
  其他机器上仍存活的进程）。
- 外部副作用的精确一次需要步骤自身基于 `AttemptKey` 幂等；引擎保证
  内部状态/成功结果的精确一次。
- 工作流定义必须通过同一组 id 提交；id 改名等价于"删旧增新"。
