"""单一职责：URL 规范化（保留业务参数、丢弃跟踪参数、记录规则版本）。

规范化是幂等的：对已是规范形的 URL 再规范一次不变。
原始输入与每条已应用规则都会被记录，便于追溯与规则换版比较。
"""
from __future__ import annotations

import hashlib
from urllib.parse import parse_qsl, quote, urlsplit

from . import store
from .models import CanonicalURL

# 规则换版时递增；判定与冲突都带版本留痕。
RULESET_VERSION = "url-canon-v1"

# 明确无业务含义的跟踪参数（前缀 / 全名）。未知参数一律保留，避免误删业务语义。
TRACKING_PREFIXES = ("utm_", "pk_", "mtm_", "hsa_", "_hs", "vero_", "oly_")
TRACKING_NAMES = frozenset(
    {
        "gclid",
        "dclid",
        "fbclid",
        "msclkid",
        "yclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "mkt_tok",
        "_ga",
        "_gl",
    }
)

_UNRESERVED = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
)
DEFAULT_PORTS = {"http": 80, "https": 443}


def _normalize_percent(text: str) -> str:
    """解码未保留字符的百分号编码，其余百分号统一大写，非 ASCII 统一按 UTF-8 编码。"""
    out: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "%" and i + 2 < n:
            hexpart = text[i + 1 : i + 3]
            try:
                code = int(hexpart, 16)
            except ValueError:
                out.append("%25")
                i += 1
                continue
            decoded = chr(code)
            if decoded in _UNRESERVED:
                out.append(decoded)
            else:
                out.append("%" + hexpart.upper())
            i += 3
            continue
        if ord(ch) > 127:
            # 原始非 ASCII → 统一 UTF-8 百分号编码，与已编码形式归一到同一写法。
            out.extend(f"%{b:02X}" for b in ch.encode("utf-8"))
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def _remove_dot_segments(path: str) -> str:
    absolute = path.startswith("/")
    trailing = path.endswith("/")
    stack: list[str] = []
    for part in path.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            if stack:
                stack.pop()
            continue
        stack.append(part)
    result = "/".join(stack)
    if absolute:
        result = "/" + result
    if not result:
        return "/"
    if trailing and result != "/":
        result += "/"
    return result


def _is_tracking(name: str) -> bool:
    lowered = name.lower()
    return lowered in TRACKING_NAMES or lowered.startswith(TRACKING_PREFIXES)


def _invalid(original: str, reason: str, ruleset_version: str) -> CanonicalURL:
    return CanonicalURL(
        original_url=original,
        canonical_url="",
        url_key="",
        host="",
        scheme="",
        path="",
        ruleset_version=ruleset_version,
        is_valid=False,
        invalid_reason=reason,
    )


def canonicalize_url(raw: object, *, ruleset_version: str = RULESET_VERSION) -> CanonicalURL:
    """把一个原始 URL 规范化为稳定的 ``CanonicalURL``（不联网、不改写业务参数值）。"""
    original = "" if raw is None else str(raw).strip()
    if not original:
        return _invalid(original, "empty_url", ruleset_version)

    rules: list[str] = []
    try:
        parts = urlsplit(original)
    except ValueError as exc:  # 例如非法的 IPv6 字面量
        return _invalid(original, f"unparsable_url:{exc}", ruleset_version)

    scheme = parts.scheme.lower()
    if scheme not in DEFAULT_PORTS:
        return _invalid(original, "unsupported_scheme", ruleset_version)
    rules.append("lowercase_scheme")

    host = parts.hostname or ""
    if not host:
        return _invalid(original, "missing_host", ruleset_version)
    host = host.rstrip(".").lower()
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return _invalid(original, "invalid_host_idna", ruleset_version)
    rules.append("lowercase_host")
    rules.append("idna_host")

    try:
        port = parts.port
    except ValueError:
        return _invalid(original, "invalid_port", ruleset_version)

    if parts.username is not None or parts.password is not None:
        rules.append("drop_userinfo")

    netloc = f"[{host}]" if ":" in host else host
    if port is not None and port != DEFAULT_PORTS[scheme]:
        netloc = f"{netloc}:{port}"
    elif port is not None:
        rules.append("drop_default_port")

    raw_path = parts.path or "/"
    path = _remove_dot_segments(_normalize_percent(raw_path))
    rules.append("normalize_path")
    rules.append("normalize_percent_encoding")

    dropped: list[tuple[str, str]] = []
    retained: list[tuple[str, str]] = []
    if parts.query:
        for name, value in parse_qsl(
            parts.query, keep_blank_values=True, strict_parsing=False
        ):
            norm_name = _normalize_percent(name)
            norm_value = _normalize_percent(value)
            if _is_tracking(norm_name):
                dropped.append((norm_name, norm_value))
            else:
                retained.append((norm_name, norm_value))
        # 完全重复的 (name,value) 折叠一次，保留业务上有意义的重复键与顺序无关性。
        if len(retained) != len(set(retained)):
            retained = list(dict.fromkeys(retained))
            rules.append("collapse_duplicate_params")
        retained.sort(key=lambda pair: (pair[0], pair[1]))
        if dropped:
            rules.append("drop_tracking_params")
        if len(retained) > 1:
            rules.append("sort_query")

    # 值已由 _normalize_percent 完成百分号规范化，故 quote 时保留合法 "%XX"。
    query = "&".join(
        f"{quote(name, safe='%')}={quote(value, safe='%')}" for name, value in retained
    )
    fragment = _normalize_percent(parts.fragment)
    if fragment:
        rules.append("drop_fragment")

    canonical_url = f"{scheme}://{netloc}{path}"
    if query:
        canonical_url += "?" + query

    url_key = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()[:32]

    grouped: dict[str, list[str]] = {}
    for name, value in retained:
        grouped.setdefault(name, []).append(value)

    return CanonicalURL(
        original_url=original,
        canonical_url=canonical_url,
        url_key=url_key,
        host=host,
        scheme=scheme,
        path=path,
        params=grouped,
        dropped_params=tuple(dropped),
        ruleset_version=ruleset_version,
        rules=tuple(rules),
        fragment=fragment,
        is_valid=True,
    )


# 便捷别名
canonicalize = canonicalize_url
normalize_url = canonicalize_url


def register(con, canon: CanonicalURL, *, now: float) -> str:
    """登记 URL 身份与原始输入（同一 url_key 可保留多条原始写法）。"""
    if not canon.is_valid:
        raise ValueError(f"cannot register invalid url: {canon.invalid_reason}")
    with store.transaction(con):
        con.execute(
            """
            INSERT INTO urls(url_key, canonical_url, host, scheme, path, params,
                             dropped_params, fragment, ruleset_version, rules,
                             first_seen, last_seen)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(url_key) DO UPDATE SET
                last_seen=excluded.last_seen,
                ruleset_version=excluded.ruleset_version,
                rules=excluded.rules
            """,
            (
                canon.url_key,
                canon.canonical_url,
                canon.host,
                canon.scheme,
                canon.path,
                store.dumps(canon.params),
                store.dumps([list(p) for p in canon.dropped_params]),
                canon.fragment,
                canon.ruleset_version,
                store.dumps(list(canon.rules)),
                now,
                now,
            ),
        )
        con.execute(
            """
            INSERT INTO url_inputs(url_key, original_url, first_seen, last_seen)
            VALUES(?,?,?,?)
            ON CONFLICT(url_key, original_url) DO UPDATE SET last_seen=excluded.last_seen
            """,
            (canon.url_key, canon.original_url, now, now),
        )
    return canon.url_key


def original_inputs(con, url_key: str) -> list[str]:
    """返回该规范化资源保留的全部原始 URL 输入。"""
    rows = con.execute(
        "SELECT original_url FROM url_inputs WHERE url_key=? ORDER BY first_seen, id",
        (url_key,),
    ).fetchall()
    return [r["original_url"] for r in rows]


def registered(con, url_key: str) -> CanonicalURL | None:
    row = con.execute("SELECT * FROM urls WHERE url_key=?", (url_key,)).fetchone()
    if row is None:
        return None
    return CanonicalURL(
        original_url="",
        canonical_url=row["canonical_url"],
        url_key=row["url_key"],
        host=row["host"],
        scheme=row["scheme"],
        path=row["path"],
        params=store.loads(row["params"]),
        dropped_params=tuple(tuple(p) for p in store.loads(row["dropped_params"])),
        ruleset_version=row["ruleset_version"],
        rules=tuple(store.loads(row["rules"])),
        fragment=row["fragment"],
    )


def process(record):
    """流水线阶段入口。

    含 URL 字段时输出规范化 URL 信息；否则保持原有记录规范化行为（向后兼容）。
    """
    if isinstance(record, dict) and ("url" in record or "original_url" in record):
        rec = dict(record)
        source = rec.get("url", rec.get("original_url"))
        canon = canonicalize_url(source)
        rec["original_url"] = canon.original_url
        rec["canonical_url"] = canon.canonical_url
        rec["url_key"] = canon.url_key
        rec["host"] = canon.host
        rec["url_rules"] = list(canon.rules)
        rec["url_ruleset_version"] = canon.ruleset_version
        rec["url_valid"] = canon.is_valid
        if not canon.is_valid:
            rec["url_invalid_reason"] = canon.invalid_reason
        rec.setdefault("_stage_canonicalize", "url")
        return rec

    rec = dict(record)
    rec["src"] = str(rec.get("src", "")).strip().lower()
    rec["name"] = str(rec.get("name", "")).strip()
    rec["value"] = int(rec.get("value", 0))
    return rec
