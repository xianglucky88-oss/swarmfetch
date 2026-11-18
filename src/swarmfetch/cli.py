"""命令行入口：离线摄取 / 领取 / 结果写入 / 确认 / 恢复 / 状态查询。

默认行为（无子命令）保持与原基线一致：跑一遍 fixtures 摄取并打印 JSON 摘要。
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from . import config as _config
from . import frontier, pipeline, recovery, state, store
from .models import Lease


def _now(value: str | None) -> float:
    return time.time() if value is None else float(value)


def cmd_run(args) -> dict:
    return pipeline.run(now=_now(args.now))


def cmd_ingest(args) -> dict:
    con = store.connect(args.db)
    try:
        return pipeline.ingest_urls(
            con, args.urls, now=_now(args.now),
            priority=args.priority, max_attempts=args.max_attempts,
        )
    finally:
        con.close()


def cmd_claim(args) -> dict:
    con = store.connect(args.db)
    try:
        leases = state.claim(
            con, owner=args.owner, now=_now(args.now),
            limit=args.limit, lease_ttl=args.ttl, hosts=args.host or None,
        )
        return {"leases": [l.as_dict() for l in leases]}
    finally:
        con.close()


def _lease(con, args) -> Lease:
    row = con.execute("SELECT * FROM tasks WHERE task_id=?", (args.task_id,)).fetchone()
    if row is None:
        raise SystemExit(f"unknown task: {args.task_id}")
    return Lease(
        task_id=row["task_id"], url_key=row["url_key"],
        canonical_url=row["canonical_url"], host=row["host"],
        owner=args.owner, epoch=int(args.epoch), priority=row["priority"],
        expires_at=row["lease_expires_at"] or 0.0, attempt=row["attempt"],
    )


def cmd_respond(args) -> dict:
    con = store.connect(args.db)
    try:
        lease = _lease(con, args)
        data = json.loads(args.data) if args.data else {}
        return state.respond(
            con, lease, now=_now(args.now), outcome=args.outcome,
            status_code=args.status, content_hash=args.content_hash,
            body_ref=args.body_ref, data=data,
        )
    finally:
        con.close()


def cmd_confirm(args) -> dict:
    con = store.connect(args.db)
    try:
        lease = _lease(con, args)
        return state.confirm(con, lease, now=_now(args.now), outcome=args.outcome)
    finally:
        con.close()


def cmd_fail(args) -> dict:
    con = store.connect(args.db)
    try:
        lease = _lease(con, args)
        return state.fail(
            con, lease, now=_now(args.now), retryable=not args.permanent, reason=args.reason
        )
    finally:
        con.close()


def cmd_recover(args) -> dict:
    con = store.connect(args.db)
    try:
        report = recovery.recover(
            con, now=_now(args.now), batch=args.batch, categories=args.category or None
        )
        report["open_uncertainty"] = recovery.open_uncertainty(con)
        return report
    finally:
        con.close()


def cmd_status(args) -> dict:
    con = store.connect(args.db)
    try:
        return {
            "task": state.status(con, args.task_id),
            "events": state.events(con, args.task_id),
        }
    finally:
        con.close()


def cmd_stats(args) -> dict:
    con = store.connect(args.db)
    try:
        return {"depth": frontier.depth(con), "rows": store.count(con)}
    finally:
        con.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="swarmfetch", description="swarmfetch 生命周期工具")
    p.add_argument("--db", default=str(_config.DB_PATH), help="SQLite 路径")
    p.add_argument("--now", default=None, help="覆盖当前时间（便于回放/测试）")
    # 子命令也接受 --db/--now（放在子命令之后亦可），因此用共享父解析器。
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    common.add_argument("--now", default=argparse.SUPPRESS, help=argparse.SUPPRESS)
    sub = p.add_subparsers(dest="command")

    s = sub.add_parser("run", help="跑一遍 fixtures 摄取（默认）", parents=[common])
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("ingest", help="摄取原始 URL（保留原始输入）", parents=[common])
    s.add_argument("urls", nargs="+")
    s.add_argument("--priority", type=int, default=0)
    s.add_argument("--max-attempts", type=int, default=5)
    s.set_defaults(func=cmd_ingest)

    s = sub.add_parser("claim", help="以可识别代次的租约领取任务", parents=[common])
    s.add_argument("--owner", required=True)
    s.add_argument("--limit", type=int, default=1)
    s.add_argument("--ttl", type=float, default=None)
    s.add_argument("--host", action="append")
    s.set_defaults(func=cmd_claim)

    for name, fn, help_text in (
        ("respond", cmd_respond, "写入抓取结果（进入 responded）"),
        ("confirm", cmd_confirm, "确认终态（由结果推导）"),
        ("fail", cmd_fail, "上报失败（可重试/永久）"),
    ):
        s = sub.add_parser(name, help=help_text, parents=[common])
        s.add_argument("task_id")
        s.add_argument("--owner", required=True)
        s.add_argument("--epoch", type=int, required=True)
        if name == "respond":
            s.add_argument("--outcome", required=True,
                           choices=["success", "retryable", "permanent"])
            s.add_argument("--status", type=int, default=None)
            s.add_argument("--content-hash", default=None)
            s.add_argument("--body-ref", default=None)
            s.add_argument("--data", default=None)
        elif name == "confirm":
            s.add_argument("--outcome", default=None,
                           choices=["success", "retryable", "permanent"])
        else:
            s.add_argument("--permanent", action="store_true")
            s.add_argument("--reason", default="worker_failure")
        s.set_defaults(func=fn)

    s = sub.add_parser("recover", help="分类恢复（有界分批）", parents=[common])
    s.add_argument("--batch", type=int, default=None)
    s.add_argument("--category", action="append")
    s.set_defaults(func=cmd_recover)

    s = sub.add_parser("status", help="查询任务状态、依据与结果", parents=[common])
    s.add_argument("task_id")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("stats", help="队列深度与已存行数", parents=[common])
    s.set_defaults(func=cmd_stats)
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        # 保持基线行为：无子命令 = run
        print(json.dumps(pipeline.run(), ensure_ascii=False))
        return 0
    result = args.func(args)
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
