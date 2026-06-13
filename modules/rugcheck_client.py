"""Shared RugCheck parsing — used by scanner, copy trader, and council."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import aiohttp

from config import RUGCHECK_URL
from modules.utils import fetch_json


@dataclass
class RugReport:
    ok: bool
    score: float
    top_holder_pct: float
    top5_holder_pct: float
    mint_renounced: bool
    freeze_safe: bool
    raw: dict[str, Any]


def _holder_pcts(data: dict[str, Any]) -> tuple[float, float]:
    holders = data.get("topHolders") or data.get("top_holders") or []
    pcts: list[float] = []
    for h in holders:
        if not isinstance(h, dict):
            continue
        pct = h.get("pct") or h.get("percentage") or h.get("uiAmount")
        try:
            val = float(pct)
        except (TypeError, ValueError):
            continue
        # RugCheck sometimes returns 0-1 fraction
        if 0 < val <= 1:
            val *= 100.0
        pcts.append(val)
    pcts.sort(reverse=True)
    top1 = pcts[0] if pcts else 0.0
    top5 = sum(pcts[:5]) if pcts else 0.0
    return top1, top5


def parse_rug_report(data: dict[str, Any]) -> RugReport:
    score = float(data.get("score", 0) or 0)
    risks = data.get("risks", []) or []
    is_honeypot = any(
        "honeypot" in str(r.get("name", "")).lower()
        or "cannot sell" in str(r.get("description", "")).lower()
        for r in risks
    )
    rugged = bool(data.get("rugged", False))
    too_risky = score > 500

    top1, top5 = _holder_pcts(data)
    mint_auth = data.get("mintAuthority") or data.get("mint_authority")
    mint_renounced = mint_auth in (None, "", "null") or str(mint_auth).lower() in ("none", "renounced")
    freeze_auth = data.get("freezeAuthority") or data.get("freeze_authority")
    freeze_safe = freeze_auth in (None, "", "null") or str(freeze_auth).lower() in ("none", "disabled")

    ok = not (is_honeypot or rugged or too_risky)
    return RugReport(
        ok=ok,
        score=score,
        top_holder_pct=top1,
        top5_holder_pct=top5,
        mint_renounced=mint_renounced,
        freeze_safe=freeze_safe,
        raw=data,
    )


async def fetch_rug_report(session: aiohttp.ClientSession, mint: str) -> RugReport:
    try:
        url = RUGCHECK_URL.format(mint=mint)
        data = await fetch_json(session, "GET", url, label=f"RugCheck {mint[:8]}")
        if not isinstance(data, dict):
            return RugReport(False, 999.0, 0, 0, False, False, {})
        return parse_rug_report(data)
    except Exception:
        return RugReport(False, 999.0, 0, 0, False, False, {})
