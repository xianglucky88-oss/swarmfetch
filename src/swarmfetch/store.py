"""单一职责：本地 sqlite3 持久化。

只提供连接、模式（schema）、事务与通用读写原语；业务判断在业务模块中。
保留原有 ``records`` 幂等 upsert / 计数 / 全量读取 API 以兼容既有调用方。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Iterator

from .config import DB_PATH

SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 旧版摄取结果（保持不变）
CREATE TABLE IF NOT EXISTS records (
    key  TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

-- URL 身份登记：每个 canonical 资源一行
CREATE TABLE IF NOT EXISTS urls (
    url_key         TEXT PRIMARY KEY,
    canonical_url   TEXT NOT NULL,
    host            TEXT NOT NULL,
    scheme          TEXT NOT NULL,
    path            TEXT NOT NULL,
    params          TEXT NOT NULL,
    dropped_params  TEXT NOT NULL,
    fragment        TEXT NOT NULL DEFAULT '',
    ruleset_version TEXT NOT NULL,
    rules           TEXT NOT NULL,
    first_seen      REAL NOT NULL,
    last_seen       REAL NOT NULL
);

-- 原始 URL 输入的保留（同一 url_key 可对应多个原始写法）
CREATE TABLE IF NOT EXISTS url_inputs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    url_key      TEXT NOT NULL,
    original_url TEXT NOT NULL,
    first_seen   REAL NOT NULL,
    last_seen    REAL NOT NULL,
    UNIQUE(url_key, original_url)
);

-- 身份判定与冲突审计：按规则版本留痕
CREATE TABLE IF NOT EXISTS identity_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url_key_a       TEXT NOT NULL,
    url_key_b       TEXT NOT NULL,
    verdict         TEXT NOT NULL,
    reason          TEXT NOT NULL,
    evidence        TEXT NOT NULL,
    ruleset_version TEXT NOT NULL,
    created_at      REAL NOT NULL,
    UNIQUE(url_key_a, url_key_b, verdict, ruleset_version, evidence)
);

-- 抓取任务：每个 canonical URL 一个确定性任务
CREATE TABLE IF NOT EXISTS tasks (
    task_id          TEXT PRIMARY KEY,
    url_key          TEXT NOT NULL,
    canonical_url    TEXT NOT NULL,
    host             TEXT NOT NULL,
    state            TEXT NOT NULL,
    priority         INTEGER NOT NULL DEFAULT 0,
    enqueued_at      REAL NOT NULL,
    available_at     REAL NOT NULL,
    attempt          INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 5,
    owner            TEXT,
    epoch            INTEGER NOT NULL DEFAULT 0,
    lease_expires_at REAL,
    result_ref       INTEGER,
    reason           TEXT,
    ruleset_version  TEXT,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_claim_idx
    ON tasks(state, available_at, priority, enqueued_at);
CREATE INDEX IF NOT EXISTS tasks_host_idx ON tasks(host, state);
CREATE INDEX IF NOT EXISTS tasks_lease_idx ON tasks(state, lease_expires_at);
CREATE INDEX IF NOT EXISTS tasks_url_idx ON tasks(url_key);

-- 抓取结果：同一 (task_id, epoch) 唯一，重复写入不产生额外效果
CREATE TABLE IF NOT EXISTS results (
    result_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id      TEXT NOT NULL,
    epoch        INTEGER NOT NULL,
    url_key      TEXT NOT NULL,
    outcome      TEXT NOT NULL,
    status_code  INTEGER,
    content_hash TEXT,
    body_ref     TEXT,
    data         TEXT NOT NULL,
    created_at   REAL NOT NULL,
    UNIQUE(task_id, epoch)
);

-- 审计事件流：状态、依据、结果均可追溯
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT,
    url_key    TEXT,
    epoch      INTEGER,
    kind       TEXT NOT NULL,
    detail     TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS events_task_idx ON events(task_id, id);

-- 令牌桶（全局 / 主机）：持久化，重启或时钟回拨不额外获得额度
CREATE TABLE IF NOT EXISTS rate_buckets (
    scope       TEXT PRIMARY KEY,
    capacity    REAL NOT NULL,
    tokens      REAL NOT NULL,
    rate        REAL NOT NULL,
    last_refill REAL NOT NULL
);

-- 状态损坏时标出的不确定范围（有界，不从头扫描全队列）
CREATE TABLE IF NOT EXISTS uncertainty (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    scope      TEXT NOT NULL,
    start_task TEXT,
    end_task   TEXT,
    reason     TEXT NOT NULL,
    created_at REAL NOT NULL,
    resolved   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS uncertainty_open_idx ON uncertainty(resolved);
"""


def connect(path=None, *, timeout: float = 30.0) -> sqlite3.Connection:
    """打开连接并确保模式存在。默认使用配置文件中的 DB_PATH。"""
    target = str(DB_PATH) if path is None else str(path)
    con = sqlite3.connect(target, timeout=timeout, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 30000")
    con.execute("PRAGMA foreign_keys = ON")
    if target != ":memory:":
        con.execute("PRAGMA journal_mode = WAL")
        con.execute("PRAGMA synchronous = NORMAL")
    init_schema(con)
    return con


def init_schema(con: sqlite3.Connection) -> None:
    con.executescript(_SCHEMA)
    con.execute(
        "INSERT INTO schema_meta(key,value) VALUES('version',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SCHEMA_VERSION,),
    )


@contextmanager
def transaction(con: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """串行化写事务。嵌套调用退化为保存点。"""
    if con.in_transaction:
        con.execute("SAVEPOINT swarmfetch_sp")
        try:
            yield con
        except BaseException:
            con.execute("ROLLBACK TO swarmfetch_sp")
            con.execute("RELEASE swarmfetch_sp")
            raise
        else:
            con.execute("RELEASE swarmfetch_sp")
        return
    con.execute("BEGIN IMMEDIATE")
    try:
        yield con
    except BaseException:
        con.execute("ROLLBACK")
        raise
    else:
        con.execute("COMMIT")


def dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def loads(value: str):
    return json.loads(value)


# --- 旧版 records API（兼容）---------------------------------------------
def upsert(con, record) -> None:
    con.execute(
        "INSERT INTO records(key,data) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET data=excluded.data",
        (record.key, json.dumps(record.data, ensure_ascii=False)),
    )


def commit(con) -> None:
    if con.in_transaction:
        con.execute("COMMIT")


def count(con) -> int:
    return con.execute("SELECT COUNT(*) FROM records").fetchone()[0]


def all_records(con):
    return [(k, json.loads(d)) for k, d in con.execute("SELECT key,data FROM records")]
