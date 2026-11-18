"""单一职责：限速（持久化令牌桶，全局 + 主机两级）。

不变量：
- 领取任务必须同时通过全局桶与主机桶；任一不足则不领取（不部分扣减）。
- 桶状态持久化：重启不会把桶重置为满额。
- 时钟回拨不产生负时间差，也不把 ``last_refill`` 往回拨，因此不会额外获得额度。
- 每次领取（含重试）都消耗令牌，重试不能绕过限速。
"""
from __future__ import annotations

from dataclasses import dataclass

from . import config as _config
from . import store

GLOBAL_SCOPE = "global"
HOST_PREFIX = "host:"

RULESET_VERSION = "ratelimit-v1"


@dataclass(frozen=True)
class BucketState:
    scope: str
    capacity: float
    tokens: float
    rate: float
    last_refill: float

    def as_dict(self) -> dict:
        return {
            "scope": self.scope,
            "capacity": self.capacity,
            "tokens": round(self.tokens, 6),
            "rate": self.rate,
            "last_refill": self.last_refill,
        }


def host_scope(host: str) -> str:
    return HOST_PREFIX + (host or "")


def _default_for(scope: str) -> tuple[float, float]:
    if scope == GLOBAL_SCOPE:
        return _config.GLOBAL_RATE_CAPACITY, _config.GLOBAL_RATE_REFILL
    return _config.HOST_RATE_CAPACITY, _config.HOST_RATE_REFILL


def _refilled(row, now: float) -> tuple[float, float]:
    """返回 (tokens, last_refill)。回拨时 elapsed 归零且不倒退 last_refill。"""
    last = row["last_refill"]
    elapsed = now - last
    if elapsed < 0:
        # 时钟回拨：不补充、不倒退水位，防止之后重复计费时间。
        return row["tokens"], last
    tokens = min(row["capacity"], row["tokens"] + elapsed * row["rate"])
    return tokens, now


def _ensure(con, scope: str, now: float, capacity=None, rate=None) -> BucketState:
    row = con.execute("SELECT * FROM rate_buckets WHERE scope=?", (scope,)).fetchone()
    if row is None:
        default_capacity, default_rate = _default_for(scope)
        capacity = default_capacity if capacity is None else capacity
        rate = default_rate if rate is None else rate
        con.execute(
            "INSERT INTO rate_buckets(scope,capacity,tokens,rate,last_refill) VALUES(?,?,?,?,?) "
            "ON CONFLICT(scope) DO NOTHING",
            (scope, capacity, capacity, rate, now),
        )
        row = con.execute("SELECT * FROM rate_buckets WHERE scope=?", (scope,)).fetchone()
    tokens, last = _refilled(row, now)
    if tokens != row["tokens"] or last != row["last_refill"]:
        con.execute(
            "UPDATE rate_buckets SET tokens=?, last_refill=? WHERE scope=?",
            (tokens, last, scope),
        )
    return BucketState(scope, row["capacity"], tokens, row["rate"], last)


def peek(con, scope: str, now: float, *, capacity=None, rate=None) -> BucketState:
    """查看桶状态（会顺带按时间补充令牌，但不消耗）。"""
    with store.transaction(con):
        return _ensure(con, scope, now, capacity, rate)


def try_consume(
    con,
    scopes: list[str],
    now: float,
    *,
    cost: float = 1.0,
    overrides: dict | None = None,
) -> bool:
    """原子地尝试从全部 scope 各扣 cost；任一不足则整体失败、不扣任何桶。"""
    overrides = overrides or {}
    ordered = sorted(set(scopes))
    with store.transaction(con):
        states = {}
        for scope in ordered:
            cap, rate = overrides.get(scope, (None, None))
            states[scope] = _ensure(con, scope, now, cap, rate)
        if any(b.tokens < cost for b in states.values()):
            return False
        for scope, bucket in states.items():
            con.execute(
                "UPDATE rate_buckets SET tokens=? WHERE scope=?",
                (bucket.tokens - cost, scope),
            )
    return True


def claim_scopes(host: str) -> list[str]:
    return [GLOBAL_SCOPE, host_scope(host)]


def allows_claim(con, host: str, now: float, *, cost: float = 1.0) -> bool:
    """只读检查两级桶是否都足够（不消耗）。"""
    for scope in claim_scopes(host):
        state = peek(con, scope, now)
        if state.tokens < cost:
            return False
    return True


def next_refill_eta(con, host: str, now: float) -> float:
    """距离任一受限桶补足 1 个令牌的最短等待秒数（0 表示立即可领）。"""
    wait = 0.0
    for scope in claim_scopes(host):
        state = peek(con, scope, now)
        if state.tokens < 1.0 and state.rate > 0:
            need = (1.0 - state.tokens) / state.rate
            wait = max(wait, need)
    return round(wait, 6)


def reset(con, scopes: list[str] | None = None, *, now: float | None = None) -> None:
    """仅用于测试/运维：清空指定桶（不改变默认配置）。"""
    with store.transaction(con):
        if scopes is None:
            con.execute("DELETE FROM rate_buckets")
        else:
            for scope in scopes:
                con.execute("DELETE FROM rate_buckets WHERE scope=?", (scope,))


def process(record):
    """流水线阶段入口：标注记录将适用的限速域（不做实际扣减）。"""
    rec = dict(record)
    host = rec.get("host", "")
    rec.setdefault("_stage_ratelimit", "bucketed")
    rec["rate_scopes"] = claim_scopes(host)
    return rec
