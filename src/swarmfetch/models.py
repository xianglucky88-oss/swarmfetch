"""共享数据模型与生命周期常量。

这里只定义跨模块使用的数据结构/常量，不含业务判断。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# --- 任务状态 -----------------------------------------------------------
# pending        已入队、尚未被领取（available_at 控制何时可领）。
# leased         已被某个 worker 以某代租约领取，独占执行。
# responded      抓取结果已落库，但终态尚未确认（worker 可能在确认前崩溃）。
# succeeded      终态：成功。
# failed_retryable 非终态：可重试失败，到达 available_at 后可再次领取。
# failed_permanent 终态：永久失败（不可重试或重试耗尽）。
# cancelled      终态：被取消（例如判定为重复资源）。
# uncertain      状态损坏/待人工判定；uncertainty 表记录可疑范围。
STATE_PENDING = "pending"
STATE_LEASED = "leased"
STATE_RESPONDED = "responded"
STATE_SUCCEEDED = "succeeded"
STATE_FAILED_RETRYABLE = "failed_retryable"
STATE_FAILED_PERMANENT = "failed_permanent"
STATE_CANCELLED = "cancelled"
STATE_UNCERTAIN = "uncertain"

TERMINAL_STATES = frozenset(
    {STATE_SUCCEEDED, STATE_FAILED_PERMANENT, STATE_CANCELLED}
)
NON_TERMINAL_STATES = frozenset(
    {STATE_PENDING, STATE_LEASED, STATE_RESPONDED, STATE_FAILED_RETRYABLE, STATE_UNCERTAIN}
)
ALL_STATES = frozenset(TERMINAL_STATES | NON_TERMINAL_STATES)

# --- 结果分类 -----------------------------------------------------------
OUTCOME_SUCCESS = "success"
OUTCOME_RETRYABLE = "retryable"
OUTCOME_PERMANENT = "permanent"
ALL_OUTCOMES = frozenset({OUTCOME_SUCCESS, OUTCOME_RETRYABLE, OUTCOME_PERMANENT})


@dataclass(frozen=True)
class CanonicalURL:
    """一次 URL 规范化的产物；保留原始输入与每条已应用规则，便于追溯。"""

    original_url: str
    canonical_url: str
    url_key: str
    host: str
    scheme: str
    path: str
    params: dict[str, list[str]] = field(default_factory=dict)
    dropped_params: tuple[tuple[str, str], ...] = ()
    ruleset_version: str = ""
    rules: tuple[str, ...] = ()
    fragment: str = ""
    is_valid: bool = True
    invalid_reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "original_url": self.original_url,
            "canonical_url": self.canonical_url,
            "url_key": self.url_key,
            "host": self.host,
            "scheme": self.scheme,
            "path": self.path,
            "params": {k: list(v) for k, v in self.params.items()},
            "dropped_params": [list(p) for p in self.dropped_params],
            "ruleset_version": self.ruleset_version,
            "rules": list(self.rules),
            "fragment": self.fragment,
            "is_valid": self.is_valid,
            "invalid_reason": self.invalid_reason,
        }


@dataclass(frozen=True)
class Lease:
    """一次领取的成功结果；epoch 是可识别的租约代次（fencing token）。"""

    task_id: str
    url_key: str
    canonical_url: str
    host: str
    owner: str
    epoch: int
    priority: int
    expires_at: float
    attempt: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "url_key": self.url_key,
            "canonical_url": self.canonical_url,
            "host": self.host,
            "owner": self.owner,
            "epoch": self.epoch,
            "priority": self.priority,
            "expires_at": self.expires_at,
            "attempt": self.attempt,
        }


@dataclass(frozen=True)
class IdentityDecision:
    """两条 URL/资源身份判定结论；evidence 必须可序列化并可审计。"""

    verdict: str  # same | distinct | conflict
    reason: str
    evidence: dict[str, Any]
    ruleset_version: str
    url_key_a: str = ""
    url_key_b: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "reason": self.reason,
            "evidence": dict(self.evidence),
            "ruleset_version": self.ruleset_version,
            "url_key_a": self.url_key_a,
            "url_key_b": self.url_key_b,
        }


@dataclass
class Record:
    key: str
    data: dict[str, Any] = field(default_factory=dict)


class LifecycleError(Exception):
    """生命周期里的可预期拒绝（过期租约、终态冲突等）。"""


class StaleLease(LifecycleError):
    """租约代次/持有者不匹配：过期 worker 的写入必须被拒绝。"""


class LeaseExpired(LifecycleError):
    """租约已过期：结果不得回写。"""


class InvalidTransition(LifecycleError):
    """当前状态不允许该迁移。"""
