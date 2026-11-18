"""单一职责：优先级队列（入队、去重、候选选择与防饥饿老化）。

设计要点：
- 任务身份由规范化 URL 决定：同一 ``url_key`` 只入队一次（幂等）。
- 排序使用 **有效优先级** = 基础优先级 + 老化加成；等待越久加成越高，防止饥饿。
- 候选查询有界（候选池上限），从不全表扫描。
"""
from __future__ import annotations

import math

from . import config as _config
from . import store
from .models import CanonicalURL

RULESET_VERSION = "frontier-v1"

TASK_PREFIX = "t:"


def task_id_for(url_key: str) -> str:
    return TASK_PREFIX + url_key


def effective_priority(priority: int, enqueued_at: float, now: float,
                       *, aging_interval: float | None = None) -> int:
    """基础优先级 + 老化加成（整数，越大越先领）。"""
    interval = _config.AGING_INTERVAL if aging_interval is None else aging_interval
    if interval <= 0:
        return int(priority)
    waited = max(0.0, now - enqueued_at)
    return int(priority) + int(waited // interval)


def _candidate_limit(limit: int) -> int:
    return min(_config.CLAIM_POOL_MAX, max(limit, limit * _config.CLAIM_POOL_FACTOR))


def enqueue(
    con,
    canon: CanonicalURL,
    *,
    now: float,
    priority: int = 0,
    max_attempts: int = 5,
) -> str:
    """幂等入队；返回 task_id。已存在且未终结时只可能提升优先级。"""
    if not canon.is_valid:
        raise ValueError(f"cannot enqueue invalid url: {canon.invalid_reason}")
    task_id = task_id_for(canon.url_key)
    with store.transaction(con):
        existing = con.execute(
            "SELECT state, priority FROM tasks WHERE task_id=?", (task_id,)
        ).fetchone()
        if existing is None:
            con.execute(
                """
                INSERT INTO tasks(task_id, url_key, canonical_url, host, state, priority,
                                  enqueued_at, available_at, attempt, max_attempts,
                                  owner, epoch, lease_expires_at, result_ref, reason,
                                  ruleset_version, created_at, updated_at)
                VALUES(?,?,?,?, 'pending', ?, ?, ?, 0, ?, NULL, 0, NULL, NULL, NULL, ?, ?, ?)
                """,
                (
                    task_id, canon.url_key, canon.canonical_url, canon.host,
                    int(priority), now, now, int(max_attempts),
                    RULESET_VERSION, now, now,
                ),
            )
            con.execute(
                "INSERT INTO events(task_id,url_key,epoch,kind,detail,created_at) "
                "VALUES(?,?,NULL,'enqueue',?,?)",
                (task_id, canon.url_key, store.dumps({"priority": int(priority)}), now),
            )
        elif existing["state"] in ("pending", "failed_retryable") and \
                int(priority) > int(existing["priority"]):
            # 已入队且可再领取：只提升优先级，不重置进度/退避。
            con.execute(
                "UPDATE tasks SET priority=?, updated_at=? WHERE task_id=?",
                (int(priority), now, task_id),
            )
            con.execute(
                "INSERT INTO events(task_id,url_key,epoch,kind,detail,created_at) "
                "VALUES(?,?,NULL,'reprioritize',?,?)",
                (task_id, canon.url_key,
                 store.dumps({"from": int(existing["priority"]), "to": int(priority)}), now),
            )
        # 其他情况（已领取/终态）保持原样：重复入队无额外效果。
    return task_id


def candidates(con, now: float, *, limit: int = 32, hosts: list[str] | None = None) -> list[dict]:
    """按有效优先级返回可领取候选（有界候选池）。"""
    pool = _candidate_limit(limit)
    # 老化间隔是本地常量（非用户输入），安全地内联进 SQL 以免参数位置混乱。
    aging = float(_config.AGING_INTERVAL) if _config.AGING_INTERVAL > 0 else 1.0
    params: list = [now, now]
    host_clause = ""
    if hosts:
        placeholders = ",".join("?" for _ in hosts)
        host_clause = f" AND host IN ({placeholders})"
        params.extend(hosts)
    params.append(pool)
    rows = con.execute(
        f"""
        SELECT task_id, url_key, canonical_url, host, priority, enqueued_at,
               available_at, attempt, max_attempts, state,
               (priority + CAST((? - enqueued_at) / {aging} AS INTEGER)) AS eff_priority
        FROM tasks
        WHERE state IN ('pending','failed_retryable')
          AND available_at <= ?
          AND attempt < max_attempts
          {host_clause}
        ORDER BY eff_priority DESC, enqueued_at ASC, task_id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def depth(con) -> dict[str, int]:
    """各类状态的任务深度（用于吞吐/饥饿观测）。"""
    rows = con.execute("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state").fetchall()
    return {r["state"]: r["n"] for r in rows}


def process(record):
    """流水线阶段入口：为记录派生任务标识（不落库，入队由生命周期显式调用）。"""
    rec = dict(record)
    if rec.get("url_key"):
        rec["task_id"] = task_id_for(rec["url_key"])
        rec.setdefault("_stage_frontier", "queued")
    else:
        rec.setdefault("_stage_frontier", "skipped")
    return rec
