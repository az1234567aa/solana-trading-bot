"""Serialize Helius API calls — free tier 429s when polling 5 wallets in parallel."""
from __future__ import annotations

import asyncio
import os
import time

_lock = asyncio.Lock()
_last_call = 0.0
MIN_INTERVAL_SEC = float(os.getenv("HELIUS_MIN_INTERVAL_SEC", "4.0"))


async def throttle() -> None:
    global _last_call
    async with _lock:
        elapsed = time.time() - _last_call
        if elapsed < MIN_INTERVAL_SEC:
            await asyncio.sleep(MIN_INTERVAL_SEC - elapsed)
        _last_call = time.time()
