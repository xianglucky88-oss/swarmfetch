"""集中配置：路径与生命周期默认参数（均可通过环境变量或显式参数覆盖）。

注意：这里只放“数值/路径”配置，不放业务规则；业务规则在各业务模块中以
显式的 ``*_RULESET_VERSION`` 常量声明，便于审计规则换版。
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "fixtures" / "input.json"
DB_PATH = Path(os.environ.get("SWARMFETCH_DB", str(ROOT / "swarmfetch.db")))

# --- 租约 / 队列 ---------------------------------------------------------
# 租约 TTL（秒）：领取后若未在 TTL 内确认，视为执行者失联。
DEFAULT_LEASE_TTL = float(os.environ.get("SWARMFETCH_LEASE_TTL", "60"))
# 优先级老化间隔（秒）：每等待一个间隔，有效优先级 +1，防止低优先级饥饿。
AGING_INTERVAL = float(os.environ.get("SWARMFETCH_AGING_INTERVAL", "60"))
# 单主机同时在租任务上限，避免单一主机占满有限 worker。
MAX_INFLIGHT_PER_HOST = int(os.environ.get("SWARMFETCH_MAX_INFLIGHT_PER_HOST", "8"))
# 一次领取最多考察的候选行数（候选池 = limit * factor，有上界）。
CLAIM_POOL_FACTOR = 8
CLAIM_POOL_MAX = 2048

# --- 重试退避 -----------------------------------------------------------
RETRY_BASE = float(os.environ.get("SWARMFETCH_RETRY_BASE", "1"))
RETRY_CAP = float(os.environ.get("SWARMFETCH_RETRY_CAP", "300"))

# --- 限速（令牌桶；仅在“创建桶”时生效，之后以持久化值为准）----------------
GLOBAL_RATE_CAPACITY = float(os.environ.get("SWARMFETCH_GLOBAL_CAPACITY", "100"))
GLOBAL_RATE_REFILL = float(os.environ.get("SWARMFETCH_GLOBAL_REFILL", "20"))
HOST_RATE_CAPACITY = float(os.environ.get("SWARMFETCH_HOST_CAPACITY", "8"))
HOST_RATE_REFILL = float(os.environ.get("SWARMFETCH_HOST_REFILL", "1"))

# --- 恢复 ---------------------------------------------------------------
RECOVERY_BATCH = int(os.environ.get("SWARMFETCH_RECOVERY_BATCH", "200"))


def backoff_delay(attempt: int, base: float | None = None, cap: float | None = None) -> float:
    """指数退避（attempt 从 1 开始）。结果有上界，重启后按可用时间恢复。"""
    base = RETRY_BASE if base is None else base
    cap = RETRY_CAP if cap is None else cap
    if attempt <= 0:
        return 0.0
    # 限制指数规模，避免 attempt 很大时溢出。
    exponent = min(attempt - 1, 30)
    return min(cap, base * (2 ** exponent))
