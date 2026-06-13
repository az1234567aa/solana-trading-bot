"""Persist cooldowns, daily counters, and trade history across restarts."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger("solana-bot.state")

DATABASE_URL = os.getenv("DATABASE_URL", "")
STATE_FILE = Path("bot_state.json")


def _default_state() -> dict[str, Any]:
    return {
        "loss_cooldowns": {},       # mint → ISO expiry
        "traded_mints": [],         # recent closed trades (newest first)
        "trade_stats": {},          # lifetime PnL counters (survives redeploy)
        "daily": {
            "date": "",
            "buys_today": 0,
            "daily_pnl_usd": 0.0,
            "halted_today": False,
            "halt_reason": "",
        },
    }


class BotStateStore:
    """JSON file + optional PostgreSQL — survives Railway redeploys when DB linked."""

    def __init__(self) -> None:
        self._pool = None

    async def initialize(self) -> None:
        if not DATABASE_URL:
            return
        try:
            import asyncpg

            self._pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bot_state (
                        id   TEXT PRIMARY KEY DEFAULT 'main',
                        data JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            logger.info("Bot state store ready (PostgreSQL)")
        except Exception as exc:
            logger.warning("Bot state DB init failed — using file: %s", exc)
            self._pool = None

    async def load(self) -> dict[str, Any]:
        if self._pool is not None:
            try:
                async with self._pool.acquire() as conn:
                    row = await conn.fetchrow(
                        "SELECT data FROM bot_state WHERE id = 'main'"
                    )
                if row:
                    data = json.loads(row["data"])
                    logger.info(
                        "Loaded bot state — %d cooldown(s), %d trade(s) in history",
                        len(data.get("loss_cooldowns", {})),
                        len(data.get("traded_mints", [])),
                    )
                    return data
            except Exception as exc:
                logger.warning("Bot state DB load failed: %s", exc)

        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                logger.info(
                    "Loaded bot state from file — %d cooldown(s), %d trade(s)",
                    len(data.get("loss_cooldowns", {})),
                    len(data.get("traded_mints", [])),
                )
                return data
            except Exception as exc:
                logger.warning("Bot state file load failed: %s", exc)
        return _default_state()

    async def save(self, state: dict[str, Any]) -> None:
        state = {**_default_state(), **state}
        payload = json.dumps(state, default=str)

        if self._pool is not None:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute("""
                        INSERT INTO bot_state (id, data, updated_at)
                        VALUES ('main', $1::jsonb, NOW())
                        ON CONFLICT (id)
                        DO UPDATE SET data = EXCLUDED.data, updated_at = NOW()
                    """, payload)
                return
            except Exception as exc:
                logger.warning("Bot state DB save failed: %s", exc)

        try:
            STATE_FILE.write_text(json.dumps(state, indent=2, default=str))
        except Exception as exc:
            logger.warning("Bot state file save failed: %s", exc)

    @staticmethod
    def parse_cooldowns(raw: dict[str, str]) -> dict[str, datetime]:
        out: dict[str, datetime] = {}
        for mint, iso in (raw or {}).items():
            try:
                out[mint] = datetime.fromisoformat(iso)
                if out[mint].tzinfo is None:
                    out[mint] = out[mint].replace(tzinfo=timezone.utc)
            except Exception:
                continue
        return out

    @staticmethod
    def serialize_cooldowns(cooldowns: dict[str, datetime]) -> dict[str, str]:
        return {
            mint: dt.astimezone(timezone.utc).isoformat()
            for mint, dt in cooldowns.items()
            if dt > datetime.now(timezone.utc)
        }

    @staticmethod
    def append_trade(
        history: list[dict[str, Any]],
        *,
        mint: str,
        symbol: str,
        pnl_usd: float,
        exit_reason: str,
        max_entries: int = 200,
    ) -> list[dict[str, Any]]:
        row = {
            "mint": mint,
            "symbol": symbol,
            "pnl_usd": round(pnl_usd, 4),
            "exit_reason": exit_reason,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        history = [row] + [h for h in history if h.get("mint") != mint]
        return history[:max_entries]
