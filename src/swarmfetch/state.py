"""单一职责：任务状态机与租约（fencing）。

核心不变量：
- 领取使用递增的 ``epoch``（可识别代次）；一次领取 = 一个唯一有效执行者。
- 任何回写都要同时满足：持有者一致、epoch 一致、租约未过期、当前状态允许。
  因此过期 worker 的响应/确认会被拒绝，不能把成功任务改回处理中。
- 终态确认与结果一致：确认从已落库的结果推导，重复消息不产生额外效果。
- 领取同时受全局与主机令牌桶约束（见 ``ratelimit``）。
"""
from __future__ import annotations

import hashlib

from . import config as _config
from . import frontier as _frontier
from . import ratelimit as _ratelimit
from . import store
from .models import (
    ALL_OUTCOMES,
    OUTCOME_PERMANENT,
    OUTCOME_RETRYABLE,
    OUTCOME_SUCCESS,
    STATE_FAILED_PERMANENT,
    STATE_FAILED_RETRYABLE,
    STATE_LEASED,
    STATE_PENDING,
    STATE_RESPONDED,
    STATE_SUCCEEDED,
    STATE_UNCERTAIN,
    InvalidTransition,
    Lease,
    LeaseExpired,
    StaleLease,
)

RULESET_VERSION = "state-v1"

_CLAIMABLE = (STATE_PENDING, STATE_FAILED_RETRYABLE)


class _RateLimited(Exception):
    """内部信号：令牌不足，回滚本候选的预留。"""


# --- 读取辅助 ------------------------------------------------------------
def task_row(con, task_id: str):
    return con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()


def _require_row(con, task_id: str):
    row = task_row(con, task_id)
    if row is None:
        raise InvalidTransition(f"unknown task: {task_id}")
    return row


def _check_owner_epoch(row, lease: Lease) -> None:
    if row["owner"] != lease.owner or row["epoch"] != lease.epoch:
        raise StaleLease(
            f"stale lease for {row['task_id']}: "
            f"held epoch={row['epoch']} owner={row['owner']!r}, "
            f"presented epoch={lease.epoch} owner={lease.owner!r}"
        )


def _check_not_expired(row, now: float) -> None:
    # 时钟回拨保护：当前时间早于该任务最后一次状态更新，说明墙钟倒退，
    # 无法证明租约仍有效（也可能掩盖已过期）。按过期处理，拒绝回写。
    if now < row["updated_at"]:
        raise LeaseExpired(
            f"clock rollback for {row['task_id']}: now={now} precedes "
            f"lease activity at {row['updated_at']}; refusing write-back"
        )
    if row["lease_expires_at"] is not None and now > row["lease_expires_at"]:
        raise LeaseExpired(
            f"lease for {row['task_id']} epoch={row['epoch']} expired at "
            f"{row['lease_expires_at']}, now={now}"
        )


def _event(con, task_id, url_key, epoch, kind, detail, now) -> None:
    con.execute(
        "INSERT INTO events(task_id,url_key,epoch,kind,detail,created_at) "
        "VALUES(?,?,?,?,?,?)",
        (task_id, url_key, epoch, kind, store.dumps(detail), now),
    )


def _result_for(con, task_id: str, epoch: int):
    return con.execute(
        "SELECT * FROM results WHERE task_id=? AND epoch=?", (task_id, epoch)
    ).fetchone()


def _inflight_for_host(con, host: str, now: float) -> int:
    row = con.execute(
        "SELECT COUNT(*) AS n FROM tasks "
        "WHERE host=? AND state='leased' AND lease_expires_at > ?",
        (host, now),
    ).fetchone()
    return int(row["n"])


# --- 领取 ---------------------------------------------------------------
def claim(
    con,
    *,
    owner: str,
    now: float,
    limit: int = 1,
    lease_ttl: float | None = None,
    hosts: list[str] | None = None,
) -> list[Lease]:
    """领取至多 ``limit`` 个任务。全局/主机限速、主机在租上限与优先级共同约束。"""
    if not owner:
        raise ValueError("owner is required for a fenced lease")
    ttl = float(_config.DEFAULT_LEASE_TTL if lease_ttl is None else lease_ttl)
    if ttl <= 0:
        raise ValueError("lease_ttl must be positive")
    if limit <= 0:
        return []

    # 有界候选池；老化排序在 SQL 内完成，避免全队列扫描。
    pool = _frontier.candidates(con, now, limit=limit, hosts=hosts)
    leases: list[Lease] = []
    for cand in pool:
        if len(leases) >= limit:
            break
        # 全局额度不足时，后续任何领取都不可能成功，直接停止。
        global_state = _ratelimit.peek(con, _ratelimit.GLOBAL_SCOPE, now)
        if global_state.tokens < 1.0:
            break
        lease = _try_claim_candidate(con, cand["task_id"], owner=owner, now=now, ttl=ttl)
        if lease is not None:
            leases.append(lease)
    return leases


def _try_claim_candidate(con, task_id: str, *, owner: str, now: float, ttl: float):
    try:
        with store.transaction(con):
            row = task_row(con, task_id)
            if row is None:
                return None
            if row["state"] not in _CLAIMABLE:
                return None
            if row["available_at"] > now:
                return None
            if row["attempt"] >= row["max_attempts"]:
                return None
            if _inflight_for_host(con, row["host"], now) >= _config.MAX_INFLIGHT_PER_HOST:
                return None

            # Fencing：以 (state, epoch) 做条件更新；并发领取只有一个能成功。
            cur = con.execute(
                """
                UPDATE tasks
                SET state='leased', owner=?, epoch=epoch+1, lease_expires_at=?,
                    attempt=attempt+1, updated_at=?
                WHERE task_id=? AND state=? AND epoch=? AND attempt < max_attempts
                  AND available_at <= ?
                """,
                (owner, now + ttl, now, task_id, row["state"], row["epoch"], now),
            )
            if cur.rowcount != 1:
                return None

            # 预留成功后才扣减令牌；不足则整体回滚（含上面的状态预留）。
            scopes = _ratelimit.claim_scopes(row["host"])
            if not _ratelimit.try_consume(con, scopes, now):
                raise _RateLimited()

            fresh = task_row(con, task_id)
            _event(
                con, task_id, fresh["url_key"], fresh["epoch"], "claim",
                {"owner": owner, "lease_expires_at": fresh["lease_expires_at"],
                 "attempt": fresh["attempt"], "priority": fresh["priority"]},
                now,
            )
            return Lease(
                task_id=task_id,
                url_key=fresh["url_key"],
                canonical_url=fresh["canonical_url"],
                host=fresh["host"],
                owner=owner,
                epoch=fresh["epoch"],
                priority=fresh["priority"],
                expires_at=fresh["lease_expires_at"],
                attempt=fresh["attempt"],
            )
    except _RateLimited:
        # 事务已回滚，状态预留与令牌扣减都不生效。
        return None


# --- 响应（结果落库，尚未确认终态）-------------------------------------
def respond(
    con,
    lease: Lease,
    *,
    now: float,
    outcome: str,
    status_code: int | None = None,
    content=None,
    content_hash: str | None = None,
    body_ref: str | None = None,
    data: dict | None = None,
) -> dict:
    """写入抓取结果并进入 ``responded``。重复消息幂等，不产生额外结果行。"""
    if outcome not in ALL_OUTCOMES:
        raise ValueError(f"unknown outcome: {outcome}")
    if content_hash is None and content is not None:
        content_hash = hashlib.sha256(
            content if isinstance(content, bytes) else str(content).encode("utf-8")
        ).hexdigest()

    with store.transaction(con):
        row = _require_row(con, lease.task_id)

        existing = _result_for(con, lease.task_id, lease.epoch)
        if existing is not None:
            # 同 epoch 已有结果：重复/重放消息无额外效果。
            # 若任务已终态并引用该结果（确认后重放），直接返回；
            # 否则仍要求有效持有者，避免伪造租约读取他人结果。
            already_terminal = (
                row["state"] in (STATE_SUCCEEDED, STATE_FAILED_PERMANENT,
                                 STATE_FAILED_RETRYABLE)
                and row["result_ref"] == existing["result_id"]
            )
            if already_terminal:
                return _result_dict(existing)
            _check_owner_epoch(row, lease)
            return _result_dict(existing)

        _check_owner_epoch(row, lease)
        _check_not_expired(row, now)
        if row["state"] != STATE_LEASED:
            raise InvalidTransition(
                f"respond requires state=leased, got {row['state']} "
                f"for {lease.task_id} epoch={lease.epoch}"
            )

        payload = store.dumps(data or {})
        cur = con.execute(
            """
            INSERT INTO results(task_id, epoch, url_key, outcome, status_code,
                                content_hash, body_ref, data, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(task_id, epoch) DO NOTHING
            """,
            (lease.task_id, lease.epoch, row["url_key"], outcome, status_code,
             content_hash, body_ref, payload, now),
        )
        if cur.rowcount != 1:
            existing = _result_for(con, lease.task_id, lease.epoch)
            return _result_dict(existing)

        result_id = cur.lastrowid
        con.execute(
            "UPDATE tasks SET state=?, result_ref=?, updated_at=? "
            "WHERE task_id=? AND epoch=? AND state='leased'",
            (STATE_RESPONDED, result_id, now, lease.task_id, lease.epoch),
        )
        _event(
            con, lease.task_id, row["url_key"], lease.epoch, "respond",
            {"outcome": outcome, "status_code": status_code,
             "content_hash": content_hash, "result_id": result_id},
            now,
        )
        return _result_dict(_result_for(con, lease.task_id, lease.epoch))


def _result_dict(row) -> dict:
    return {
        "result_id": row["result_id"],
        "task_id": row["task_id"],
        "epoch": row["epoch"],
        "url_key": row["url_key"],
        "outcome": row["outcome"],
        "status_code": row["status_code"],
        "content_hash": row["content_hash"],
        "body_ref": row["body_ref"],
        "data": store.loads(row["data"]),
        "created_at": row["created_at"],
    }


# --- 终态确认 -----------------------------------------------------------
def confirm(con, lease: Lease, *, now: float, outcome: str | None = None) -> dict:
    """由 worker 确认终态；终态严格由已落库结果推导，保证结果与状态一致。"""
    with store.transaction(con):
        row = _require_row(con, lease.task_id)
        result = _result_for(con, lease.task_id, lease.epoch)
        if result is not None and outcome is not None and outcome != result["outcome"]:
            # 结果与确认声明冲突：无论持有者是否仍在，都必须拒绝（可追溯）。
            raise InvalidTransition(
                f"confirm outcome {outcome!r} conflicts with stored result "
                f"{result['outcome']!r} for {lease.task_id} epoch={lease.epoch}"
            )
        if result is not None:
            # 幂等：同 epoch 已收敛到与结果一致的终态 → 无额外效果。
            # 该分支先于持有者校验，允许崩溃后重放的同一条确认消息。
            if row["state"] in (STATE_SUCCEEDED, STATE_FAILED_PERMANENT,
                                STATE_FAILED_RETRYABLE) \
                    and row["epoch"] == lease.epoch \
                    and row["result_ref"] == result["result_id"] \
                    and _terminal_matches(row["state"], result["outcome"]):
                return _state_dict(row)

        _check_owner_epoch(row, lease)
        if result is None:
            raise InvalidTransition(
                f"confirm requires a stored result for {lease.task_id} epoch={lease.epoch}"
            )

        _check_not_expired(row, now)
        if row["state"] not in (STATE_RESPONDED, STATE_LEASED):
            raise InvalidTransition(
                f"confirm requires responded state, got {row['state']} for {lease.task_id}"
            )
        return _apply_outcome(con, row, result, now, reason="worker_confirm", epoch=lease.epoch)


def finalize_responded(con, task_id: str, *, now: float, reason: str = "recovery_finalize") -> dict:
    """系统路径：把 ``responded`` 任务按已落库结果收敛到终态（无需活动租约）。"""
    with store.transaction(con):
        row = _require_row(con, task_id)
        if row["state"] != STATE_RESPONDED:
            return _state_dict(row)
        result = _result_for(con, task_id, row["epoch"])
        if result is None:
            return _mark_uncertain(
                con, task_id, now, reason="responded_without_result", epoch=row["epoch"]
            )
        return _apply_outcome(con, row, result, now, reason=reason, epoch=row["epoch"])


def _terminal_matches(state: str, outcome: str) -> bool:
    if outcome == OUTCOME_SUCCESS:
        return state == STATE_SUCCEEDED
    if outcome == OUTCOME_PERMANENT:
        return state == STATE_FAILED_PERMANENT
    return state == STATE_FAILED_RETRYABLE


def _apply_outcome(con, row, result, now: float, *, reason: str, epoch: int) -> dict:
    task_id = row["task_id"]
    outcome = result["outcome"]
    result_id = result["result_id"]

    if outcome == OUTCOME_SUCCESS:
        new_state = STATE_SUCCEEDED
        available_at = row["available_at"]
    elif outcome == OUTCOME_PERMANENT:
        new_state = STATE_FAILED_PERMANENT
        available_at = row["available_at"]
    else:  # retryable
        if row["attempt"] < row["max_attempts"]:
            new_state = STATE_FAILED_RETRYABLE
            available_at = now + _config.backoff_delay(row["attempt"])
        else:
            new_state = STATE_FAILED_PERMANENT
            available_at = row["available_at"]

    # 只有仍在同 epoch 的“进行中”状态才迁移，防止把终态改回处理中。
    cur = con.execute(
        """
        UPDATE tasks
        SET state=?, available_at=?, result_ref=?, reason=?, updated_at=?,
            owner=NULL, lease_expires_at=NULL
        WHERE task_id=? AND epoch=? AND state IN ('responded','leased')
        """,
        (new_state, available_at, result_id, reason, now, task_id, epoch),
    )
    fresh = task_row(con, task_id)
    if cur.rowcount == 1:
        _event(
            con, task_id, fresh["url_key"], epoch, "terminal",
            {"state": new_state, "outcome": outcome, "reason": reason,
             "result_id": result_id, "available_at": available_at},
            now,
        )
    return _state_dict(fresh)


# --- 失败上报 -----------------------------------------------------------
def fail(con, lease: Lease, *, now: float, retryable: bool, reason: str) -> dict:
    """执行失败上报：可重试进入退避队列，否则永久失败。"""
    with store.transaction(con):
        row = _require_row(con, lease.task_id)
        _check_owner_epoch(row, lease)
        _check_not_expired(row, now)
        if row["state"] != STATE_LEASED:
            raise InvalidTransition(
                f"fail requires state=leased, got {row['state']} for {lease.task_id}"
            )
        if retryable and row["attempt"] < row["max_attempts"]:
            new_state = STATE_FAILED_RETRYABLE
            available_at = now + _config.backoff_delay(row["attempt"])
        else:
            new_state = STATE_FAILED_PERMANENT
            available_at = row["available_at"]
            if retryable:
                reason = f"{reason} (attempts exhausted)"
        cur = con.execute(
            """
            UPDATE tasks
            SET state=?, available_at=?, reason=?, updated_at=?,
                owner=NULL, lease_expires_at=NULL
            WHERE task_id=? AND epoch=? AND state='leased'
            """,
            (new_state, available_at, reason, now, lease.task_id, lease.epoch),
        )
        fresh = task_row(con, lease.task_id)
        if cur.rowcount == 1:
            _event(
                con, lease.task_id, fresh["url_key"], lease.epoch, "fail",
                {"state": new_state, "reason": reason, "available_at": available_at},
                now,
            )
        return _state_dict(fresh)


# --- 续租 / 取消 / 不确定 ------------------------------------------------
def renew(con, lease: Lease, *, now: float, lease_ttl: float | None = None) -> Lease:
    """续租：仅当前有效持有者可续；过期后拒绝。"""
    ttl = float(_config.DEFAULT_LEASE_TTL if lease_ttl is None else lease_ttl)
    with store.transaction(con):
        row = _require_row(con, lease.task_id)
        _check_owner_epoch(row, lease)
        _check_not_expired(row, now)
        if row["state"] != STATE_LEASED:
            raise InvalidTransition(f"renew requires leased, got {row['state']}")
        expires = now + ttl
        con.execute(
            "UPDATE tasks SET lease_expires_at=?, updated_at=? "
            "WHERE task_id=? AND epoch=? AND state='leased'",
            (expires, now, lease.task_id, lease.epoch),
        )
        _event(con, lease.task_id, row["url_key"], lease.epoch, "renew",
               {"lease_expires_at": expires}, now)
        fresh = task_row(con, lease.task_id)
        return Lease(
            task_id=fresh["task_id"], url_key=fresh["url_key"],
            canonical_url=fresh["canonical_url"], host=fresh["host"],
            owner=fresh["owner"], epoch=fresh["epoch"], priority=fresh["priority"],
            expires_at=fresh["lease_expires_at"], attempt=fresh["attempt"],
        )


def cancel(con, task_id: str, *, now: float, reason: str) -> dict:
    """取消未终结任务（例如判定为重复资源）。终态任务保持不变。"""
    with store.transaction(con):
        row = _require_row(con, task_id)
        if row["state"] in (STATE_SUCCEEDED, STATE_FAILED_PERMANENT):
            return _state_dict(row)
        cur = con.execute(
            "UPDATE tasks SET state='cancelled', reason=?, updated_at=?, "
            "owner=NULL, lease_expires_at=NULL "
            "WHERE task_id=? AND state NOT IN ('succeeded','failed_permanent','cancelled')",
            (reason, now, task_id),
        )
        fresh = task_row(con, task_id)
        if cur.rowcount == 1:
            _event(con, task_id, fresh["url_key"], fresh["epoch"], "cancel",
                   {"reason": reason}, now)
        return _state_dict(fresh)


def _mark_uncertain(con, task_id: str, now: float, *, reason: str, epoch=None) -> dict:
    con.execute(
        "UPDATE tasks SET state=?, reason=?, updated_at=?, owner=NULL, lease_expires_at=NULL "
        "WHERE task_id=?",
        (STATE_UNCERTAIN, reason, now, task_id),
    )
    con.execute(
        "INSERT INTO uncertainty(scope,start_task,end_task,reason,created_at,resolved) "
        "VALUES(?,?,?,?,?,0)",
        (f"task:{task_id}", task_id, task_id, reason, now),
    )
    fresh = task_row(con, task_id)
    _event(con, task_id, fresh["url_key"], epoch, "uncertain",
           {"reason": reason}, now)
    return _state_dict(fresh)


# --- 查询/可观测 --------------------------------------------------------
def _state_dict(row) -> dict:
    if row is None:
        return {}
    return {
        "task_id": row["task_id"],
        "url_key": row["url_key"],
        "canonical_url": row["canonical_url"],
        "host": row["host"],
        "state": row["state"],
        "priority": row["priority"],
        "attempt": row["attempt"],
        "max_attempts": row["max_attempts"],
        "owner": row["owner"],
        "epoch": row["epoch"],
        "lease_expires_at": row["lease_expires_at"],
        "available_at": row["available_at"],
        "result_ref": row["result_ref"],
        "reason": row["reason"],
        "updated_at": row["updated_at"],
    }


def status(con, task_id: str) -> dict:
    row = task_row(con, task_id)
    if row is None:
        return {}
    out = _state_dict(row)
    result = None
    if row["result_ref"] is not None:
        r = con.execute("SELECT * FROM results WHERE result_id=?", (row["result_ref"],)).fetchone()
        result = _result_dict(r) if r else None
    out["result"] = result
    return out


def events(con, task_id: str) -> list[dict]:
    rows = con.execute(
        "SELECT * FROM events WHERE task_id=? ORDER BY id", (task_id,)
    ).fetchall()
    return [
        {"id": r["id"], "kind": r["kind"], "epoch": r["epoch"],
         "detail": store.loads(r["detail"]), "created_at": r["created_at"]}
        for r in rows
    ]


def process(record):
    """流水线阶段入口：补充任务状态默认值（不落库）。"""
    rec = dict(record)
    rec.setdefault("state", STATE_PENDING)
    rec.setdefault("_stage_state", "known")
    return rec
