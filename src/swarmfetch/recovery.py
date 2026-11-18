"""单一职责：故障恢复与状态修复（有界、可继续的分批扫描）。

恢复分类（彼此独立、各自幂等）：
1. ``responded``：结果已落库但未确认 → 按结果收敛终态。
2. 过期 ``leased``：租约过期且无结果 → 重试退避或永久失败；有结果 → 按结果收敛。
3. ``failed_retryable``：重试次数已耗尽 → 永久失败。
4. ``pending``：保持可领取；过期/非法的可用时间归零，不丢任务。
5. 状态损坏：未知状态、终态缺结果、结果引用悬空、租约字段不全 → 标记不确定范围。

所有查询都走索引并带 ``LIMIT``，不从头扫描全队列；批满时返回 ``truncated`` 供下次继续。
"""
from __future__ import annotations

from . import config as _config
from . import state as _state
from . import store
from .models import (
    ALL_STATES,
    STATE_FAILED_PERMANENT,
    STATE_FAILED_RETRYABLE,
    STATE_LEASED,
    STATE_PENDING,
    STATE_RESPONDED,
    STATE_SUCCEEDED,
    STATE_UNCERTAIN,
)

RULESET_VERSION = "recovery-v1"

_TERMINAL = (STATE_SUCCEEDED, STATE_FAILED_PERMANENT)


def _event(con, task_id, url_key, epoch, kind, detail, now) -> None:
    con.execute(
        "INSERT INTO events(task_id,url_key,epoch,kind,detail,created_at) VALUES(?,?,?,?,?,?)",
        (task_id, url_key, epoch, kind, store.dumps(detail), now),
    )


def _row_val(row, name, default=None):
    try:
        return row[name]
    except (IndexError, KeyError):
        return default


def _mark_range(con, kind: str, rows, now: float) -> dict:
    """把一批损坏行标记为不确定范围（start/end），不逐条猜测。"""
    ids = sorted(r["task_id"] for r in rows)
    start, end = ids[0], ids[-1]
    with store.transaction(con):
        con.execute(
            "INSERT INTO uncertainty(scope,start_task,end_task,reason,created_at,resolved) "
            "VALUES(?,?,?,?,?,0)",
            (kind, start, end, f"{kind}:{len(ids)} task(s)", now),
        )
        for r in rows:
            con.execute(
                "UPDATE tasks SET state=?, reason=?, updated_at=? WHERE task_id=?",
                (STATE_UNCERTAIN, f"corrupt:{kind}", now, r["task_id"]),
            )
            _event(con, r["task_id"], _row_val(r, "url_key"), _row_val(r, "epoch"),
                   "uncertain_range", {"kind": kind, "start": start, "end": end}, now)
    return {"kind": kind, "start": start, "end": end, "count": len(ids)}


def _recover_responded(con, now: float, batch: int) -> dict:
    rows = con.execute(
        "SELECT task_id FROM tasks WHERE state='responded' ORDER BY updated_at LIMIT ?",
        (batch,),
    ).fetchall()
    finalized = 0
    for r in rows:
        out = _state.finalize_responded(con, r["task_id"], now=now)
        if out.get("state") in (STATE_SUCCEEDED, STATE_FAILED_PERMANENT,
                                STATE_FAILED_RETRYABLE):
            finalized += 1
    return {"scanned": len(rows), "finalized": finalized, "truncated": len(rows) == batch}


def _recover_expired_leases(con, now: float, batch: int) -> dict:
    rows = con.execute(
        """
        SELECT task_id, url_key, host, epoch, attempt, max_attempts
        FROM tasks
        WHERE state='leased' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?
        ORDER BY lease_expires_at
        LIMIT ?
        """,
        (now, batch),
    ).fetchall()
    requeued = expired_finalized = 0
    for r in rows:
        with store.transaction(con):
            result = con.execute(
                "SELECT result_id FROM results WHERE task_id=? AND epoch=?",
                (r["task_id"], r["epoch"]),
            ).fetchone()
            if result is not None:
                # 结果已在，只是确认前崩溃：按结果收敛，避免重复抓取。
                cur = con.execute(
                    "UPDATE tasks SET state='responded', result_ref=?, updated_at=?, "
                    "owner=NULL, lease_expires_at=NULL "
                    "WHERE task_id=? AND epoch=? AND state='leased'",
                    (result["result_id"], now, r["task_id"], r["epoch"]),
                )
                if cur.rowcount == 1:
                    _state.finalize_responded(
                        con, r["task_id"], now=now, reason="recovery_leased_with_result"
                    )
                    expired_finalized += 1
                continue

            if r["attempt"] < r["max_attempts"]:
                new_state = STATE_FAILED_RETRYABLE
                available_at = now + _config.backoff_delay(r["attempt"])
            else:
                new_state = STATE_FAILED_PERMANENT
                available_at = now
            cur = con.execute(
                "UPDATE tasks SET state=?, available_at=?, reason=?, updated_at=?, "
                "owner=NULL, lease_expires_at=NULL "
                "WHERE task_id=? AND epoch=? AND state='leased'",
                (new_state, available_at, "lease_expired", now,
                 r["task_id"], r["epoch"]),
            )
            if cur.rowcount == 1:
                requeued += 1
                _event(con, r["task_id"], r["url_key"], r["epoch"], "lease_expired",
                       {"state": new_state, "available_at": available_at}, now)
    return {"scanned": len(rows), "requeued": requeued,
            "finalized": expired_finalized, "truncated": len(rows) == batch}


def _recover_retryable(con, now: float, batch: int) -> dict:
    """可重试失败：耗尽次数者转永久失败；未耗尽者修复可领取时间不早于 now 之外的状态。"""
    exhausted = con.execute(
        "SELECT task_id, url_key, epoch FROM tasks "
        "WHERE state='failed_retryable' AND attempt >= max_attempts "
        "ORDER BY available_at LIMIT ?",
        (batch,),
    ).fetchall()
    for r in exhausted:
        with store.transaction(con):
            con.execute(
                "UPDATE tasks SET state='failed_permanent', reason='attempts_exhausted', "
                "updated_at=? WHERE task_id=? AND state='failed_retryable'",
                (now, r["task_id"]),
            )
            _event(con, r["task_id"], r["url_key"], r["epoch"], "attempts_exhausted",
                   {"state": STATE_FAILED_PERMANENT}, now)

    remaining = max(0, batch - len(exhausted))
    normalized = []
    if remaining:
        # 可重试但残留持有者/租约字段（异常中断）：清空，保留退避时间。
        normalized = con.execute(
            "SELECT task_id, url_key, epoch FROM tasks "
            "WHERE state='failed_retryable' AND (owner IS NOT NULL "
            "OR lease_expires_at IS NOT NULL) ORDER BY available_at LIMIT ?",
            (remaining,),
        ).fetchall()
        for r in normalized:
            with store.transaction(con):
                con.execute(
                    "UPDATE tasks SET owner=NULL, lease_expires_at=NULL, updated_at=? "
                    "WHERE task_id=? AND state='failed_retryable'",
                    (now, r["task_id"]),
                )
                _event(con, r["task_id"], r["url_key"], r["epoch"], "recovery_retryable",
                       {"reason": "cleared_lease_fields"}, now)

    scanned = len(exhausted) + len(normalized)
    return {"scanned": scanned, "failed_permanent": len(exhausted),
            "normalized": len(normalized), "truncated": scanned >= batch}


def _recover_pending(con, now: float, batch: int) -> dict:
    """未领取任务：修复非法可用时间/归属字段，不丢任务。"""
    rows = con.execute(
        "SELECT task_id, url_key, epoch FROM tasks "
        "WHERE state='pending' AND (available_at > ? OR owner IS NOT NULL "
        "OR lease_expires_at IS NOT NULL) "
        "ORDER BY available_at LIMIT ?",
        (now, batch),
    ).fetchall()
    for r in rows:
        with store.transaction(con):
            con.execute(
                "UPDATE tasks SET owner=NULL, lease_expires_at=NULL, updated_at=? "
                "WHERE task_id=? AND state='pending'",
                (now, r["task_id"]),
            )
            _event(con, r["task_id"], r["url_key"], r["epoch"], "recovery_pending",
                   {"reason": "normalized_pending"}, now)
    return {"scanned": len(rows), "normalized": len(rows), "truncated": len(rows) == batch}


def _recover_corruption(con, now: float, batch: int) -> dict:
    """有界地找出违反不变量的任务，标出不确定范围。"""
    marked: list[dict] = []
    known = ",".join("?" for _ in ALL_STATES)

    unknown = con.execute(
        f"SELECT task_id, url_key, epoch FROM tasks WHERE state NOT IN ({known}) "
        "ORDER BY task_id LIMIT ?",
        (*sorted(ALL_STATES), batch),
    ).fetchall()
    if unknown:
        marked.append(_mark_range(con, "unknown_state", unknown, now))

    remaining = batch - sum(m["count"] for m in marked)
    if remaining > 0:
        # 成功必须有结果；永久失败可以没有结果（重试耗尽/worker 上报失败均为合法终态）。
        success_no_result = con.execute(
            "SELECT task_id, url_key, epoch FROM tasks "
            "WHERE state='succeeded' AND result_ref IS NULL "
            "ORDER BY task_id LIMIT ?",
            (remaining,),
        ).fetchall()
        if success_no_result:
            marked.append(_mark_range(con, "success_without_result", success_no_result, now))
            remaining = batch - sum(m["count"] for m in marked)

    if remaining > 0:
        dangling = con.execute(
            "SELECT t.task_id, t.url_key, t.epoch FROM tasks t "
            "LEFT JOIN results r ON r.result_id = t.result_ref "
            "WHERE t.result_ref IS NOT NULL AND r.result_id IS NULL "
            "ORDER BY t.task_id LIMIT ?",
            (remaining,),
        ).fetchall()
        if dangling:
            marked.append(_mark_range(con, "dangling_result_ref", dangling, now))
            remaining = batch - sum(m["count"] for m in marked)

    if remaining > 0:
        malformed_lease = con.execute(
            "SELECT task_id, url_key, epoch FROM tasks "
            "WHERE state='leased' AND (owner IS NULL OR lease_expires_at IS NULL) "
            "ORDER BY task_id LIMIT ?",
            (remaining,),
        ).fetchall()
        if malformed_lease:
            marked.append(_mark_range(con, "malformed_lease", malformed_lease, now))
            remaining = batch - sum(m["count"] for m in marked)

    if remaining > 0:
        # 可领取却永远领不到（次数已耗尽）属于死状态，标为不确定。
        stuck = con.execute(
            "SELECT task_id, url_key, epoch FROM tasks "
            "WHERE state='pending' AND attempt >= max_attempts "
            "ORDER BY task_id LIMIT ?",
            (remaining,),
        ).fetchall()
        if stuck:
            marked.append(_mark_range(con, "stuck_pending_exhausted", stuck, now))

    return {"ranges": marked, "count": sum(m["count"] for m in marked)}


def _recover_permanent(con, now: float, batch: int) -> dict:
    """永久失败：终态不再领取；仅核对结果与状态是否自洽，异常记入不确定范围。"""
    rows = con.execute(
        "SELECT task_id, url_key, epoch, result_ref FROM tasks "
        "WHERE state='failed_permanent' ORDER BY updated_at LIMIT ?",
        (batch,),
    ).fetchall()
    reconciled = 0
    inconsistent = []
    for r in rows:
        if r["result_ref"] is None:
            continue
        result = con.execute(
            "SELECT outcome FROM results WHERE result_id=?", (r["result_ref"],)
        ).fetchone()
        if result is None:
            inconsistent.append(r)  # 引用悬空，交由 corruption 处理
            continue
        if result["outcome"] == "success":
            # 结果成功却被标成永久失败：状态与结果不一致，标出范围。
            inconsistent.append(r)
            continue
        reconciled += 1
    if inconsistent:
        _mark_range(con, "permanent_result_mismatch", inconsistent, now)
    return {"scanned": len(rows), "reconciled": reconciled,
            "inconsistent": len(inconsistent), "truncated": len(rows) == batch}


def recover(
    con,
    *,
    now: float,
    batch: int | None = None,
    categories: list[str] | None = None,
) -> dict:
    """执行一轮恢复。默认覆盖全部类别；每类最多扫描 ``batch`` 行。"""
    size = int(_config.RECOVERY_BATCH if batch is None else batch)
    if size <= 0:
        raise ValueError("batch must be positive")
    wanted = set(categories) if categories else {
        "responded", "expired_leases", "retryable", "pending", "permanent", "corruption"
    }
    handlers = {
        "responded": lambda: _recover_responded(con, now, size),
        "expired_leases": lambda: _recover_expired_leases(con, now, size),
        "retryable": lambda: _recover_retryable(con, now, size),
        "pending": lambda: _recover_pending(con, now, size),
        "permanent": lambda: _recover_permanent(con, now, size),
        "corruption": lambda: _recover_corruption(con, now, size),
    }
    report: dict = {"now": now, "batch": size}
    total = 0
    for name in ("responded", "expired_leases", "retryable", "pending",
                 "permanent", "corruption"):
        if name not in wanted:
            continue
        outcome = handlers[name]()
        report[name] = outcome
        if name == "corruption":
            total += outcome.get("count", 0)
        else:
            total += outcome.get("scanned", 0)
    report["actions"] = total
    # 有界批满则提示存在待继续工作，但绝不整表扫描。
    report["truncated"] = any(
        isinstance(v, dict) and v.get("truncated") for v in report.values()
    )
    return report


def open_uncertainty(con) -> list[dict]:
    rows = con.execute(
        "SELECT * FROM uncertainty WHERE resolved=0 ORDER BY id"
    ).fetchall()
    return [
        {"id": r["id"], "scope": r["scope"], "start_task": r["start_task"],
         "end_task": r["end_task"], "reason": r["reason"], "created_at": r["created_at"]}
        for r in rows
    ]


def process(record):
    """流水线阶段入口：标注记录进入了可恢复的持久状态（不触发扫描）。"""
    rec = dict(record)
    rec.setdefault("_stage_recovery", "durable")
    rec.setdefault("recoverable", True)
    return rec
