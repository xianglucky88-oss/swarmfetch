"""薄编排：loader → 阶段链 → keyer → 幂等入账；URL 记录额外登记身份并入队。

编排层只按顺序调用阶段与生命周期入口，不含业务判断。
``run()`` 保持既有返回结构 ``{"fed", "rows"}``，并额外回报生命周期计数。
"""
from __future__ import annotations

import importlib
import time as _time

from . import canonicalize as _canonicalize
from . import frontier as _frontier
from . import loader as _loader
from . import store as _store
from .keyer import key_of
from .models import Record

CHAIN_NAMES = ['canonicalize', 'ratelimit', 'state', 'recovery']

# 含这些字段的记录会被视为“URL 输入”，登记 URL 身份并（可选）入队。
URL_FIELDS = ("url", "original_url")


def _chain():
    return [importlib.import_module("." + n, __package__).process for n in CHAIN_NAMES]


def ingest(con, records, *, now: float | None = None, enqueue: bool = True,
           priority: int = 0, max_attempts: int = 5) -> dict:
    """把一批原始记录幂等入账；URL 记录登记身份并按需入队。"""
    now = _time.time() if now is None else now
    chain = _chain()
    fed = 0
    url_records = 0
    enqueued = 0
    invalid_urls = 0

    for raw in records:
        rec = raw
        for fn in chain:
            rec = fn(rec)

        url_source = next((rec[f] for f in URL_FIELDS if rec.get(f)), None)
        if url_source is not None:
            canon = _canonicalize.canonicalize_url(url_source)
            if canon.is_valid:
                _canonicalize.register(con, canon, now=now)
                rec["canonical_url"] = canon.canonical_url
                rec["url_key"] = canon.url_key
                rec["host"] = canon.host
                rec["url_rules"] = list(canon.rules)
                rec["url_ruleset_version"] = canon.ruleset_version
                url_records += 1
                if enqueue:
                    _frontier.enqueue(
                        con, canon, now=now, priority=priority, max_attempts=max_attempts
                    )
                    enqueued += 1
            else:
                invalid_urls += 1
                rec["url_valid"] = False
                rec["url_invalid_reason"] = canon.invalid_reason

        _store.upsert(con, Record(key=key_of(rec), data=rec))
        fed += 1

    _store.commit(con)
    return {
        "fed": fed,
        "urls": url_records,
        "enqueued": enqueued,
        "invalid_urls": invalid_urls,
    }


def run(con=None, *, now: float | None = None, use_fixture: bool = True) -> dict:
    """离线流程入口：读 fixtures，幂等入账并接续生命周期。"""
    own = con is None
    con = con or _store.connect()
    try:
        sum0 = store_summary(con)
        result = ingest(con, _loader.load() if use_fixture else [], now=now)
        result["rows"] = _store.count(con)
        result["tasks"] = _frontier.depth(con)
        result["before"] = sum0
        return result
    finally:
        if own:
            con.close()


def ingest_urls(con, urls, *, now: float | None = None, priority: int = 0,
                max_attempts: int = 5) -> dict:
    """便捷入口：直接摄取一批原始 URL（保留原始输入）。"""
    records = [{"url": u} for u in urls]
    return ingest(con, records, now=now, priority=priority, max_attempts=max_attempts)


def store_summary(con) -> dict:
    try:
        return {"rows": _store.count(con)}
    except Exception:  # pragma: no cover - 表尚未建立时
        return {"rows": 0}
