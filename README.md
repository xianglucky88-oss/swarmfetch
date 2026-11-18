# swarmfetch — 抓取前端：URL 规一与近似去重、优先级队列、并发速率、崩溃恢复。

运行栈：Python 3.14 + 标准库 `sqlite3` + `httpx` + `pytest`（离线，无需联网）

## 运行方式
读 `fixtures/input.json` → `loader` → 处理阶段链 → `keyer` → 幂等落 `sqlite`。
```text
make run     # 运行离线流程，输出摄取数量和落库行数
make test    # 运行项目测试（流水线、幂等、生命周期不变量）
```

无子命令时 `python -m swarmfetch.cli` 行为与基线一致（跑一遍 fixtures 并打印 JSON 摘要）。
生命周期操作通过子命令完成：

```text
python -m swarmfetch.cli ingest URL... [--priority N]   # 摄取原始 URL（保留原始输入）
python -m swarmfetch.cli claim --owner W [--limit N] [--ttl S] [--host H]
python -m swarmfetch.cli respond TASK --owner W --epoch E --outcome success|retryable|permanent
python -m swarmfetch.cli confirm TASK --owner W --epoch E
python -m swarmfetch.cli fail    TASK --owner W --epoch E [--permanent] [--reason R]
python -m swarmfetch.cli recover [--batch N] [--category C]
python -m swarmfetch.cli status  TASK     # 状态、依据（事件流）与结果
python -m swarmfetch.cli stats            # 队列深度 / 落库行数
```
全局 `--db PATH`、`--now TS` 可放在子命令前或后（`--now` 用于回放/测试）。

## 数据流与模块边界
每个业务阶段保持单一职责；`pipeline.py` 只负责按顺序调用阶段，不在编排层加入业务判断。
业务模块：`dedupe`, `frontier`, `ratelimit`, `recovery`, `canonicalize`, `state`。
通用模块：`loader / keyer / store / pipeline / cli`。

## 生命周期设计
**URL 身份**（`canonicalize`）
- 规范化保留有业务含义的查询参数（含重复键），仅丢弃明确的跟踪参数（`utm_*`、`gclid`…）。
- 统一 scheme/host 大小写、IDNA、默认端口、点段与百分号编码；片段不参与资源身份。
- 幂等；同一资源保留全部原始 URL 输入（`url_inputs`），规则集带版本（`url-canon-v1`）。

**近似去重 / 身份判定**（`dedupe`）
- 只有证据充分才判为同一资源：完整重定向链（每跳状态码与端点连续且终点匹配），
  或内容指纹高度一致（sha256 相同 / 分片 Jaccard ≥ 阈值）。
- 灰区与相互矛盾的证据记为 `conflict`，不合并；所有判定写入 `identity_decisions`，
  带规则版本与证据快照，可追溯、可按版本复查。

**优先级队列**（`frontier`）
- 任务身份 = 规范化 URL；同 `url_key` 幂等入队，不复活终态任务。
- 排序键 = 基础优先级 + 随时间增长的老化加成，低优先级不会饥饿。
- 候选查询有界（候选池上限），从不全表扫描。

**限速**（`ratelimit`）
- 全局 + 主机两级持久化令牌桶，领取任务必须同时通过两级；不足则整体不扣减。
- 桶状态落库，重启不重置；时钟回拨不补充令牌、不倒退水位。
- 每次领取（含重试）都消耗令牌，重试不能额外获得额度。

**任务状态与租约**（`state`）
- 状态：`pending → leased → responded → succeeded | failed_retryable → failed_permanent`，
  另有 `cancelled`、`uncertain`。
- 领取以递增 `epoch`（可识别代次）做条件更新，同一任务同一时刻只有一个有效执行者。
- 任何回写都要求：持有者一致、`epoch` 一致、租约未过期、当前状态允许；
  过期或时钟回拨后的 worker 写入被拒绝，不能把成功任务改回处理中。
- 结果按 `(task_id, epoch)` 唯一落库；终态严格由已落库结果推导，
  结果与确认保持一致，重复消息/重放不产生额外效果。
- 主机在租上限约束并发；全局额度不足时直接停止领取。

**故障恢复**（`recovery`，有界分批，不从头扫描全队列）
1. `responded` 未确认 → 按已落库结果收敛终态。
2. 过期 `leased`：有结果则收敛（不重复抓取），无结果则退避重试或转永久失败。
3. `failed_retryable`：次数耗尽转 `failed_permanent`，并清理残留租约字段。
4. `failed_permanent`：终态不再领取；核对结果与状态自洽，异常记入不确定范围。
5. `pending`：修复非法可用时间/归属字段，不丢任务。
6. 状态损坏（未知状态、成功缺结果、结果引用悬空、租约字段不全、死状态）→ 记入
   `uncertainty` 的起止范围，标记为 `uncertain`，不做猜测式修复。

每轮恢复各分类最多扫描 `batch` 行，返回 `truncated` 表示仍有待续工作。

## 可追溯性
- `events`：入队、领取、响应、终态、失败、取消、恢复等事件流，含 `epoch` 与依据。
- `identity_decisions`：身份判定与冲突，含规则版本与证据。
- `status` 子命令汇总某任务的当前状态、事件依据与结果。

## 持久化与测试
数据通过项目内 SQLite 存储（WAL），使用稳定键执行幂等写入；测试夹具位于
项目测试目录，运行方式以 `Makefile` 为准。`tests/test_lifecycle.py` 覆盖 URL 身份、
租约 fencing、限速、恢复分类、幂等与旧 URL 输入保留等不变量。
