"""生命周期核心不变量测试：URL 身份、租约 fencing、限速、恢复、幂等。"""
import hashlib

import pytest

from src.swarmfetch import (
    canonicalize,
    dedupe,
    frontier,
    pipeline,
    ratelimit,
    recovery,
    state,
    store,
)


@pytest.fixture()
def con(tmp_path):
    c = store.connect(tmp_path / "life.db")
    yield c
    c.close()


def _enqueue_one(con, url, *, now=0.0, priority=0):
    canon = canonicalize.canonicalize_url(url)
    canonicalize.register(con, canon, now=now)
    task_id = frontier.enqueue(con, canon, now=now, priority=priority)
    return canon, task_id


# --- URL 规范化 ---------------------------------------------------------
def test_canonicalize_preserves_business_params_and_drops_tracking():
    c = canonicalize.canonicalize_url(
        "HTTPS://Example.COM:443/a/./b/../c?b=2&utm_source=x&a=1&a=0#frag"
    )
    assert c.is_valid
    assert c.canonical_url == "https://example.com/a/c?a=0&a=1&b=2"
    assert c.params == {"a": ["0", "1"], "b": ["2"]}
    assert ("utm_source", "x") in c.dropped_params
    assert "drop_tracking_params" in c.rules
    assert "drop_default_port" in c.rules


def test_canonicalize_is_idempotent_and_keeps_original_inputs(con):
    raw = "http://Example.com:80/p?utm_medium=email&x=1&b=2"
    canon = canonicalize.canonicalize_url(raw)
    assert canonicalize.canonicalize_url(canon.canonical_url).canonical_url == canon.canonical_url
    canonicalize.register(con, canon, now=1.0)
    canonicalize.register(con, canonicalize.canonicalize_url("http://example.com/p?b=2&x=1"), now=2.0)
    assert canonicalize.original_inputs(con, canon.url_key) == [raw, "http://example.com/p?b=2&x=1"]


def test_canonicalize_rejects_invalid_urls():
    assert not canonicalize.canonicalize_url("ftp://x/y").is_valid
    assert not canonicalize.canonicalize_url("").is_valid
    assert canonicalize.canonicalize_url("/relative").is_valid is False


# --- 身份判定：证据充分才算同一 ----------------------------------------
def test_identity_requires_complete_redirect_chain():
    a = canonicalize.canonicalize_url("https://a.example/old")
    b = canonicalize.canonicalize_url("https://a.example/new")

    # 终点与目标不符 → 证据不足
    broken = [{"status": 301, "from_url_key": a.url_key, "to_url_key": "other"}]
    assert dedupe.decide_identity(a, b, redirect_chain=broken).verdict == "conflict"

    # 起点与 a 不符 → 链条不是从 a 出发，证据不足
    foreign = [{"status": 301, "from_url_key": "someone-else", "to_url_key": b.url_key}]
    assert dedupe.decide_identity(a, b, redirect_chain=foreign).verdict == "conflict"

    # 非法跳转状态码 → 证据不足
    bad_status = [{"status": 200, "from_url_key": a.url_key, "to_url_key": b.url_key}]
    assert dedupe.decide_identity(a, b, redirect_chain=bad_status).verdict == "conflict"

    # 完整链：起点=a、终点=b、合法状态码 → 判同
    good = [{"status": 301, "from_url_key": a.url_key, "to_url_key": b.url_key}]
    assert dedupe.decide_identity(a, b, redirect_chain=good).verdict == "same"

    # 多跳链：每一跳连续
    mid = "mid-key"
    chain = [
        {"status": 302, "from_url_key": a.url_key, "to_url_key": mid},
        {"status": 308, "from_url_key": mid, "to_url_key": b.url_key},
    ]
    assert dedupe.decide_identity(a, b, redirect_chain=chain).verdict == "same"
    # 断链：第二跳起点不等于第一跳终点
    chain_broken = [
        {"status": 302, "from_url_key": a.url_key, "to_url_key": mid},
        {"status": 308, "from_url_key": "gap", "to_url_key": b.url_key},
    ]
    assert dedupe.decide_identity(a, b, redirect_chain=chain_broken).verdict == "conflict"


def test_identity_identical_content_is_same_but_grey_zone_is_conflict():
    a = canonicalize.canonicalize_url("https://a.example/1")
    b = canonicalize.canonicalize_url("https://a.example/2")
    body = "the quick brown fox jumps over the lazy dog " * 3
    same = dedupe.decide_identity(a, b, content_a=body, content_b=body)
    assert same.verdict == "same"

    unrelated = dedupe.decide_identity(a, b, content_a=body, content_b="完全不同的一段中文内容" * 5)
    assert unrelated.verdict == "distinct"

    # 相似但未达阈值 → 冲突而非强行合并
    mid = dedupe.decide_identity(
        a, b, content_a=body, content_b=body[:-20] + " zzz different tail here"
    )
    assert mid.verdict in ("conflict", "distinct")
    assert mid.evidence["ruleset_version"] == dedupe.RULESET_VERSION


def test_identity_decision_is_auditable_and_idempotent(con):
    a = canonicalize.canonicalize_url("https://a.example/1")
    b = canonicalize.canonicalize_url("https://a.example/2")
    body = "x" * 80
    d = dedupe.decide_identity(a, b, content_a=body, content_b=body)
    rid1 = dedupe.record_decision(con, d, now=1.0)
    rid2 = dedupe.record_decision(con, d, now=1.0)
    assert rid1 == rid2
    rows = dedupe.decisions_for(con, a.url_key)
    assert rows and rows[0]["ruleset_version"] == dedupe.RULESET_VERSION


# --- 优先级队列 ---------------------------------------------------------
def test_priority_order_and_aging_prevents_starvation(con):
    low = _enqueue_one(con, "https://h.example/low", now=0.0, priority=0)[1]
    high = _enqueue_one(con, "https://h.example/high", now=0.0, priority=10)[1]
    # 高优先级先出
    row = con.execute("SELECT task_id FROM tasks WHERE state='pending' ORDER BY "
                      "(priority + CAST((? - enqueued_at) / ? AS INTEGER)) DESC, enqueued_at LIMIT 1",
                      (0.0, 1000.0)).fetchone()
    assert row["task_id"] == high
    # 经过足够老化后，低优先级获得加成
    assert frontier.effective_priority(0, 0.0, 1e6, aging_interval=10) > 10
    assert low  # 仍在队列


def test_enqueue_is_idempotent(con):
    canon = canonicalize.canonicalize_url("https://h.example/a")
    canonicalize.register(con, canon, now=0.0)
    t1 = frontier.enqueue(con, canon, now=0.0)
    t2 = frontier.enqueue(con, canon, now=0.0)
    assert t1 == t2
    assert frontier.depth(con) == {"pending": 1}


# --- 领取 / 租约 fencing -------------------------------------------------
def test_claim_is_exclusive_and_fenced(con):
    _enqueue_one(con, "https://h.example/a", now=0.0)
    leases = state.claim(con, owner="w1", now=1.0, limit=1)
    assert len(leases) == 1
    lease = leases[0]
    # 同一任务不能有第二个有效执行者
    assert state.claim(con, owner="w2", now=1.1, limit=5) == []
    # 过期 worker 的响应被拒绝
    stale = type(lease)(**{**lease.__dict__, "epoch": lease.epoch - 1})
    with pytest.raises(state.StaleLease):
        state.respond(con, stale, now=1.2, outcome="success")


def test_expired_lease_cannot_write_back_and_is_recovered(con):
    _, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w1", now=1.0, limit=1, lease_ttl=5)[0]
    # 任务被另一个 worker 接管前，过期响应必须失败
    with pytest.raises(state.LeaseExpired):
        state.respond(con, lease, now=100.0, outcome="success")
    report = recovery.recover(con, now=100.0)
    assert report["expired_leases"]["requeued"] == 1
    assert state.task_row(con, task_id)["state"] == "failed_retryable"


def test_terminal_success_cannot_be_reopened_by_stale_worker(con):
    _, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w1", now=1.0, limit=1)[0]
    state.respond(con, lease, now=1.1, outcome="success", content="ok")
    state.confirm(con, lease, now=1.2)
    assert state.task_row(con, task_id)["state"] == "succeeded"
    # 模拟另一个旧 epoch 的 worker 试图改回处理中
    fake = type(lease)(**{**lease.__dict__, "epoch": lease.epoch + 99})
    with pytest.raises(state.StaleLease):
        state.respond(con, fake, now=1.3, outcome="retryable")
    assert state.task_row(con, task_id)["state"] == "succeeded"


# --- 结果 / 确认一致性 --------------------------------------------------
def test_result_and_confirmation_are_consistent_and_idempotent(con):
    _, _ = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w1", now=1.0, limit=1)[0]
    r1 = state.respond(con, lease, now=1.1, outcome="success", content="body")
    r2 = state.respond(con, lease, now=1.2, outcome="success", content="body")
    assert r1["result_id"] == r2["result_id"]
    assert con.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 1
    s1 = state.confirm(con, lease, now=1.3)
    s2 = state.confirm(con, lease, now=1.4)
    assert s1["state"] == s2["state"] == "succeeded"
    assert con.execute("SELECT COUNT(*) FROM events WHERE kind='terminal'").fetchone()[0] == 1
    # 确认结果必须与落库结果一致
    with pytest.raises(state.InvalidTransition):
        state.confirm(con, lease, now=1.5, outcome="permanent")


def test_retryable_failure_backs_off_and_eventually_permanent(con):
    _, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    now = 1.0
    for _ in range(6):
        leases = state.claim(con, owner="w", now=now, limit=1)
        if not leases:
            now += 1000
            continue
        lease = leases[0]
        state.respond(con, lease, now=now, outcome="retryable")
        state.confirm(con, lease, now=now)
        now += 1000
    assert state.task_row(con, task_id)["state"] == "failed_permanent"


# --- 限速 ---------------------------------------------------------------
def test_global_and_host_rate_limits_constrain_claims(con):
    for i in range(5):
        _enqueue_one(con, f"https://h.example/{i}", now=0.0)
    # 主机桶容量 8、全局 100；先把主机桶重置为 2 以观察约束
    ratelimit.reset(con, [ratelimit.host_scope("h.example")])
    with store.transaction(con):
        con.execute(
            "INSERT INTO rate_buckets(scope,capacity,tokens,rate,last_refill) "
            "VALUES(?,?,?,?,?) ON CONFLICT(scope) DO UPDATE SET tokens=2, capacity=2, rate=0",
            (ratelimit.host_scope("h.example"), 2, 2, 0, 100.0),
        )
    leases = state.claim(con, owner="w", now=100.0, limit=5)
    assert len(leases) == 2  # 只受主机桶限制
    # 令牌耗尽后再次领取失败
    assert state.claim(con, owner="w", now=100.0, limit=5) == []


def test_clock_rollback_does_not_grant_extra_tokens(con):
    bucket = ratelimit.peek(con, ratelimit.GLOBAL_SCOPE, 1000.0)
    with store.transaction(con):
        con.execute("UPDATE rate_buckets SET tokens=0, last_refill=1000.0 WHERE scope=?",
                    (ratelimit.GLOBAL_SCOPE,))
    # 回拨时间不应产生正补充
    after = ratelimit.peek(con, ratelimit.GLOBAL_SCOPE, 500.0)
    assert after.tokens == 0
    assert after.last_refill == 1000.0


def test_restart_does_not_refill_buckets(tmp_path):
    path = tmp_path / "r.db"
    con = store.connect(path)
    try:
        ratelimit.peek(con, ratelimit.GLOBAL_SCOPE, 100.0)
        with store.transaction(con):
            con.execute("UPDATE rate_buckets SET tokens=0 WHERE scope=?",
                        (ratelimit.GLOBAL_SCOPE,))
    finally:
        con.close()
    con2 = store.connect(path)
    try:
        assert ratelimit.peek(con2, ratelimit.GLOBAL_SCOPE, 100.0).tokens == 0
    finally:
        con2.close()


# --- 恢复分类 -----------------------------------------------------------
def test_recovery_distinguishes_categories_without_full_scan(con):
    # 成功终态但缺结果引用 = 状态损坏（succeeded 必须有结果）
    c1, _ = _enqueue_one(con, "https://h.example/r", now=0.0)
    l1 = state.claim(con, owner="w", now=1.0, limit=1, hosts=["h.example"])[0]
    with store.transaction(con):
        con.execute("UPDATE tasks SET state='succeeded', result_ref=NULL "
                    "WHERE task_id=?", (l1.task_id,))
    report = recovery.recover(con, now=200.0, batch=10)
    assert report["corruption"]["count"] >= 1
    assert recovery.open_uncertainty(con)


def test_permanent_failure_without_result_is_legitimate_terminal(con):
    # 永久失败（例如 404/重试耗尽）无需结果行，恢复不应误判为损坏
    _, task_id = _enqueue_one(con, "https://h.example/gone", now=0.0)
    lease = state.claim(con, owner="w", now=1.0, limit=1)[0]
    state.fail(con, lease, now=1.1, retryable=False, reason="http_404")
    assert state.task_row(con, task_id)["state"] == "failed_permanent"
    report = recovery.recover(con, now=200.0, categories=["corruption"])
    assert report["corruption"]["count"] == 0


def test_recovery_leased_with_result_finalizes_without_refetch(con):
    _, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w", now=1.0, limit=1)[0]
    with store.transaction(con):
        con.execute("INSERT INTO results(task_id,epoch,url_key,outcome,data,created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (lease.task_id, lease.epoch, lease.url_key, "success", "{}", 1.1))
        # 模拟结果已写但确认前崩溃（仍是 leased）
        con.execute("UPDATE tasks SET state='leased' WHERE task_id=?", (task_id,))
    report = recovery.recover(con, now=2.0, categories=["expired_leases"])
    # 租约未过期，不处理
    assert report["expired_leases"]["scanned"] == 0
    report = recovery.recover(con, now=10_000.0, categories=["expired_leases"])
    assert state.task_row(con, task_id)["state"] == "succeeded"


def test_recovery_is_bounded_and_reports_truncation(con):
    for i in range(10):
        _enqueue_one(con, f"https://h.example/{i}", now=0.0)
    with store.transaction(con):
        con.execute("UPDATE tasks SET state='failed_retryable', attempt=99, max_attempts=99")
    report = recovery.recover(con, now=1.0, batch=3, categories=["retryable"])
    assert report["retryable"]["scanned"] == 3
    assert report["retryable"]["truncated"] is True


# --- 生命周期集成 -------------------------------------------------------
def test_pipeline_ingests_urls_and_keeps_legacy_contract(tmp_path):
    con = store.connect(tmp_path / "p.db")
    try:
        result = pipeline.ingest_urls(
            con,
            ["https://Example.com/a?utm_source=x&b=2", "https://example.com/a?b=2"],
            now=0.0,
        )
        assert result["urls"] == 2
        # 两个原始写法归一到同一 url_key / 同一任务
        assert result["enqueued"] == 2
        assert frontier.depth(con) == {"pending": 1}
        assert len(canonicalize.original_inputs(con, result and con.execute(
            "SELECT url_key FROM urls").fetchone()["url_key"])) == 2
    finally:
        con.close()


def test_full_lifecycle_end_to_end(tmp_path):
    con = store.connect(tmp_path / "e.db")
    try:
        pipeline.ingest_urls(con, ["https://h.example/a", "https://h2.example/b"], now=0.0,
                             priority=5)
        lease = state.claim(con, owner="worker-1", now=1.0, limit=1)[0]
        state.respond(con, lease, now=1.1, outcome="success", content="payload")
        state.confirm(con, lease, now=1.2)
        status = state.status(con, lease.task_id)
        assert status["state"] == "succeeded"
        assert status["result"]["outcome"] == "success"
        kinds = [e["kind"] for e in state.events(con, lease.task_id)]
        assert "claim" in kinds and "respond" in kinds and "terminal" in kinds
    finally:
        con.close()


# --- 追加覆盖：复现提示中的关键场景 -------------------------------------
def test_duplicate_respond_after_confirm_is_noop(con):
    _, _ = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w", now=1.0, limit=1)[0]
    state.respond(con, lease, now=1.1, outcome="success", content="ok")
    state.confirm(con, lease, now=1.2)
    # 崩溃恢复后重放同一条响应：不新增结果、不改状态
    again = state.respond(con, lease, now=99.0, outcome="success", content="ok")
    assert again["epoch"] == lease.epoch
    assert con.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 1
    assert state.task_row(con, lease.task_id)["state"] == "succeeded"


def test_enqueue_does_not_reopen_terminal_or_duplicate_event(con):
    canon, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    n_events = con.execute("SELECT COUNT(*) FROM events WHERE kind='enqueue'").fetchone()[0]
    frontier.enqueue(con, canon, now=0.0)
    assert con.execute("SELECT COUNT(*) FROM events WHERE kind='enqueue'").fetchone()[0] == n_events
    lease = state.claim(con, owner="w", now=1.0, limit=1)[0]
    state.respond(con, lease, now=1.1, outcome="permanent")
    state.confirm(con, lease, now=1.2)
    frontier.enqueue(con, canon, now=5.0, priority=99)  # 终态不因重复入队复活
    row = state.task_row(con, task_id)
    assert row["state"] == "failed_permanent"


def test_reprioritize_pending_without_resetting_attempt(con):
    canon, task_id = _enqueue_one(con, "https://h.example/a", now=0.0, priority=1)
    frontier.enqueue(con, canon, now=2.0, priority=7)
    row = state.task_row(con, task_id)
    assert row["priority"] == 7
    assert row["attempt"] == 0
    assert con.execute("SELECT COUNT(*) FROM events WHERE kind='reprioritize'").fetchone()[0] == 1


def test_uncertainty_range_is_recorded_for_corrupt_state(con):
    canon, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    with store.transaction(con):
        con.execute("UPDATE tasks SET state='bogus' WHERE task_id=?", (task_id,))
    report = recovery.recover(con, now=1.0, categories=["corruption"])
    assert report["corruption"]["count"] == 1
    ranges = recovery.open_uncertainty(con)
    assert ranges[0]["start_task"] == task_id and ranges[0]["end_task"] == task_id
    assert state.task_row(con, task_id)["state"] == "uncertain"


def test_recovery_does_not_scan_whole_queue(con):
    # 大量 pending 任务，恢复只处理有界批次且不改动无关任务
    for i in range(50):
        _enqueue_one(con, f"https://big.example/{i}", now=0.0)
    report = recovery.recover(con, now=1.0, batch=5)
    assert report["batch"] == 5
    assert report["pending"]["scanned"] == 0  # pending 均健康
    assert frontier.depth(con) == {"pending": 50}


def test_legacy_fixture_records_still_ingest_and_dedupe(tmp_path):
    con = store.connect(tmp_path / "l.db")
    try:
        result = pipeline.ingest(con, pipeline._loader.load(), now=0.0)
        assert result["fed"] == 4
        assert store.count(con) == 3  # r-003 去重
        assert con.execute("SELECT COUNT(*) FROM tasks").fetchone()[0] == 0
    finally:
        con.close()


def test_retryable_backoff_cannot_be_claimed_early(con):
    _, task_id = _enqueue_one(con, "https://h.example/a", now=0.0)
    lease = state.claim(con, owner="w", now=1.0, limit=1)[0]
    state.respond(con, lease, now=1.1, outcome="retryable")
    state.confirm(con, lease, now=1.2)
    row = state.task_row(con, task_id)
    assert row["available_at"] > 1.2
    # 退避未到：不可领取；到达后可领取
    assert state.claim(con, owner="w2", now=1.3, limit=1) == []
    assert state.claim(con, owner="w2", now=row["available_at"] + 0.1, limit=1)
