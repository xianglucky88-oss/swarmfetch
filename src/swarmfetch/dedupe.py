"""单一职责：URL/资源身份归并与近似去重判定。

原则：**只有证据充分才认作同一资源**。
- 重定向：需要完整的重定向链证据（每一跳的状态码、来源与目标），且终点规范 URL 一致。
- 内容相似：需要达到高相似阈值；处于灰区时记为 ``conflict`` 而非强行合并。
所有判定都带 ``ruleset_version`` 与证据快照，冲突可追溯、可复查。
"""
from __future__ import annotations

import hashlib
import re

from . import store
from .models import CanonicalURL, IdentityDecision

RULESET_VERSION = "identity-v1"

# 内容相似阈值：>= SAME 判同；<= DISTINCT 判异；中间为冲突（需人工/更多证据）。
SIMILARITY_SAME_THRESHOLD = 0.92
SIMILARITY_DISTINCT_THRESHOLD = 0.55
SHINGLE_SIZE = 5
MIN_SHINGLE_CHARS = 40
# simhash 汉明距离上限（64 位），用于快速预筛；不单独作为判同依据。
SIMHASH_MAX_DISTANCE = 3

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
VERDICT_SAME = "same"
VERDICT_DISTINCT = "distinct"
VERDICT_CONFLICT = "conflict"

_WS = re.compile(r"\s+")


def _text_of(content) -> str:
    if content is None:
        return ""
    if isinstance(content, bytes):
        return content.decode("utf-8", "replace")
    return str(content)


def _shingles(text: str) -> set[str]:
    normalized = _WS.sub(" ", text.casefold()).strip()
    if len(normalized) < MIN_SHINGLE_CHARS:
        return {hashlib.sha256(normalized.encode()).hexdigest()} if normalized else set()
    grams = set()
    for i in range(len(normalized) - SHINGLE_SIZE + 1):
        gram = normalized[i : i + SHINGLE_SIZE]
        grams.add(hashlib.blake2b(gram.encode(), digest_size=8).hexdigest())
    return grams


def content_fingerprint(content) -> dict:
    """内容指纹：归一化文本的 sha256 + simhash + 分片数（可持久化、可比较）。"""
    text = _text_of(content)
    normalized = _WS.sub(" ", text.casefold()).strip()
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    # 64 位 simhash
    vector = [0] * 64
    for gram in _shingles(text):
        h = int(gram, 16)
        for bit in range(64):
            vector[bit] += 1 if (h >> bit) & 1 else -1
    sim = 0
    for bit in range(64):
        if vector[bit] > 0:
            sim |= 1 << bit
    return {
        "sha256": digest,
        "simhash": f"{sim:016x}",
        "shingles": len(_shingles(text)),
        "length": len(normalized),
        "ruleset_version": RULESET_VERSION,
    }


def content_similarity(a, b) -> float:
    """基于分片 Jaccard 的内容相似度（0..1）。"""
    sa, sb = _shingles(_text_of(a)), _shingles(_text_of(b))
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _simhash_distance(fp_a: dict, fp_b: dict) -> int:
    return bin(int(fp_a["simhash"], 16) ^ int(fp_b["simhash"], 16)).count("1")


def _redirect_evidence_ok(redirect_chain, target_url_key: str,
                          source_url_key: str | None = None) -> tuple[bool, dict]:
    """重定向证据是否“充分”：链完整、每跳合法、起点与终点都与两端一致。"""
    if not redirect_chain:
        return False, {"chain_length": 0, "reason": "no_redirect_evidence"}
    hops = []
    previous_url_key = None
    for index, hop in enumerate(redirect_chain):
        status = hop.get("status")
        source_key = hop.get("from_url_key")
        target_key = hop.get("to_url_key")
        hops.append(
            {
                "status": status,
                "from_url_key": source_key,
                "to_url_key": target_key,
                "cross_host": bool(hop.get("cross_host", False)),
            }
        )
        if status not in REDIRECT_STATUSES:
            return False, {
                "chain_length": len(redirect_chain),
                "reason": f"invalid_redirect_status:{status}",
                "hops": hops,
            }
        if index == 0 and source_url_key is not None and source_key != source_url_key:
            return False, {
                "chain_length": len(redirect_chain),
                "reason": "redirect_source_mismatch",
                "source": source_key,
                "hops": hops,
            }
        if previous_url_key is not None and source_key != previous_url_key:
            return False, {
                "chain_length": len(redirect_chain),
                "reason": "broken_redirect_chain",
                "hops": hops,
            }
        previous_url_key = target_key
    final_key = redirect_chain[-1].get("to_url_key")
    if target_url_key and final_key != target_url_key:
        return False, {
            "chain_length": len(redirect_chain),
            "reason": "redirect_terminal_mismatch",
            "terminal": final_key,
            "hops": hops,
        }
    return True, {"chain_length": len(redirect_chain), "hops": hops,
                  "reason": "redirect_chain_complete"}


def decide_identity(
    a: CanonicalURL,
    b: CanonicalURL,
    *,
    content_a=None,
    content_b=None,
    redirect_chain=None,
    ruleset_version: str = RULESET_VERSION,
) -> IdentityDecision:
    """判定两个资源是否同一；证据不足时返回 conflict（不合并）。"""
    evidence: dict = {
        "a": {"url_key": a.url_key, "canonical_url": a.canonical_url, "host": a.host},
        "b": {"url_key": b.url_key, "canonical_url": b.canonical_url, "host": b.host},
        "ruleset_version": ruleset_version,
    }

    if not a.is_valid or not b.is_valid:
        evidence["invalid"] = {
            "a": a.invalid_reason,
            "b": b.invalid_reason,
        }
        return IdentityDecision(
            verdict=VERDICT_CONFLICT,
            reason="invalid_url",
            evidence=evidence,
            ruleset_version=ruleset_version,
            url_key_a=a.url_key,
            url_key_b=b.url_key,
        )

    if a.url_key == b.url_key:
        evidence["basis"] = "identical_canonical_url"
        return IdentityDecision(
            VERDICT_SAME, "identical_canonical_url", evidence, ruleset_version,
            a.url_key, b.url_key,
        )

    # 重定向证据：只有链完整、起点=a 且终点=b 才足以判同。
    ok, redirect_meta = _redirect_evidence_ok(redirect_chain, b.url_key, a.url_key)
    evidence["redirect"] = redirect_meta
    if redirect_chain:
        if ok:
            return IdentityDecision(
                VERDICT_SAME, "complete_redirect_chain", evidence, ruleset_version,
                a.url_key, b.url_key,
            )
        return IdentityDecision(
            VERDICT_CONFLICT,
            f"insufficient_redirect_evidence:{redirect_meta['reason']}",
            evidence,
            ruleset_version,
            a.url_key,
            b.url_key,
        )

    if content_a is None and content_b is None:
        return IdentityDecision(
            VERDICT_DISTINCT, "no_content_or_redirect_evidence", evidence,
            ruleset_version, a.url_key, b.url_key,
        )

    fp_a = content_fingerprint(content_a)
    fp_b = content_fingerprint(content_b)
    score = content_similarity(content_a, content_b)
    distance = _simhash_distance(fp_a, fp_b)
    evidence.update(
        {
            "content": {
                "similarity": round(score, 6),
                "simhash_distance": distance,
                "fingerprint_a": fp_a,
                "fingerprint_b": fp_b,
                "same_sha256": fp_a["sha256"] == fp_b["sha256"],
            },
            "thresholds": {
                "same": SIMILARITY_SAME_THRESHOLD,
                "distinct": SIMILARITY_DISTINCT_THRESHOLD,
                "max_simhash_distance": SIMHASH_MAX_DISTANCE,
            },
        }
    )

    if fp_a["sha256"] == fp_b["sha256"] and fp_a["length"] >= MIN_SHINGLE_CHARS:
        evidence["basis"] = "identical_content_digest"
        return IdentityDecision(
            VERDICT_SAME, "identical_content_digest", evidence, ruleset_version,
            a.url_key, b.url_key,
        )

    # simhash 接近但 Jaccard 未达阈值：证据相互矛盾 → conflict，不合并。
    if distance <= SIMHASH_MAX_DISTANCE and score < SIMILARITY_SAME_THRESHOLD:
        return IdentityDecision(
            VERDICT_CONFLICT, "simhash_near_but_shingles_differ", evidence,
            ruleset_version, a.url_key, b.url_key,
        )

    if score >= SIMILARITY_SAME_THRESHOLD:
        return IdentityDecision(
            VERDICT_SAME, "high_content_similarity", evidence, ruleset_version,
            a.url_key, b.url_key,
        )

    if score <= SIMILARITY_DISTINCT_THRESHOLD:
        return IdentityDecision(
            VERDICT_DISTINCT, "low_content_similarity", evidence, ruleset_version,
            a.url_key, b.url_key,
        )

    return IdentityDecision(
        VERDICT_CONFLICT, "similarity_in_grey_zone", evidence, ruleset_version,
        a.url_key, b.url_key,
    )


def _ordered_pair(key_a: str, key_b: str) -> tuple[str, str]:
    return (key_a, key_b) if key_a <= key_b else (key_b, key_a)


def record_decision(con, decision: IdentityDecision, *, now: float) -> int:
    """持久化判定（幂等：同证据同版本不重复），返回审计行 id。"""
    key_a, key_b = _ordered_pair(decision.url_key_a, decision.url_key_b)
    payload = store.dumps(decision.evidence)
    with store.transaction(con):
        cur = con.execute(
            """
            INSERT INTO identity_decisions
                (url_key_a, url_key_b, verdict, reason, evidence, ruleset_version, created_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(url_key_a, url_key_b, verdict, ruleset_version, evidence)
            DO NOTHING
            """,
            (key_a, key_b, decision.verdict, decision.reason, payload,
             decision.ruleset_version, now),
        )
        if cur.lastrowid:
            rowid = cur.lastrowid
        else:
            row = con.execute(
                """
                SELECT id FROM identity_decisions
                WHERE url_key_a=? AND url_key_b=? AND verdict=? AND ruleset_version=? AND evidence=?
                """,
                (key_a, key_b, decision.verdict, decision.ruleset_version, payload),
            ).fetchone()
            rowid = row["id"] if row else 0
        con.execute(
            "INSERT INTO events(task_id,url_key,epoch,kind,detail,created_at) "
            "VALUES(NULL,?,NULL,'identity_decision',?,?)",
            (key_a, payload, now),
        )
    return int(rowid)


def decisions_for(con, url_key: str) -> list[dict]:
    rows = con.execute(
        "SELECT * FROM identity_decisions WHERE url_key_a=? OR url_key_b=? ORDER BY id",
        (url_key, url_key),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "url_key_a": r["url_key_a"],
            "url_key_b": r["url_key_b"],
            "verdict": r["verdict"],
            "reason": r["reason"],
            "evidence": store.loads(r["evidence"]),
            "ruleset_version": r["ruleset_version"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def process(record):
    """流水线阶段入口：为含 URL 的记录补充内容指纹（不判定、不合并）。"""
    rec = dict(record)
    if rec.get("canonical_url"):
        rec["content_fingerprint"] = content_fingerprint(rec.get("content", ""))
        rec.setdefault("_stage_dedupe", "fingerprint")
    else:
        rec.setdefault("_stage_dedupe", "skipped")
    return rec
