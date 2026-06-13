"""Throttle DexScreener — free tier 429s when scanner hammers token lookups."""
from __future__ import annotations

import asyncio
import os
import time

_lock = asyncio.Lock()
_last_call = 0.0
_backoff_until = 0.0
MIN_INTERVAL_SEC = float(os.getenv("DEXSCREENER_MIN_INTERVAL_SEC", "1.5"))
BACKOFF_SEC = float(os.getenv("DEXSCREENER_429_BACKOFF_SEC", "90"))


async def throttle() -> None:
    global _last_call
    async with _lock:
        now = time.time()
        if now < _backoff_until:
            wait = _backoff_until - now
            await asyncio.sleep(wait)
        elapsed = time.time() - _last_call
        if elapsed < MIN_INTERVAL_SEC:
            await asyncio.sleep(MIN_INTERVAL_SEC - elapsed)
        _last_call = time.time()


def mark_rate_limited() -> None:
    global _backoff_until
    _backoff_until = time.time() + BACKOFF_SEC
