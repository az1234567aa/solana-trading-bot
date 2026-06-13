"""Build council candidates + shared gate for scanner, copy, and Twitter paths."""
from __future__ import annotations

from typing import Any

import aiohttp

import config
from modules.meme_council import CouncilResult, TokenCandidate, evaluate
from modules.rugcheck_client import RugReport, fetch_rug_report


async def council_gate(
    session: aiohttp.ClientSession,
    *,
    mint: str,
    symbol: str,
    pair: dict[str, Any],
    source: str,
    sell_route_ok: bool,
    score: float = 0.0,
    breakdown: dict[str, Any] | None = None,
    birdeye: dict[str, Any] | None = None,
    twitter_mentions: int = 0,
    rug: RugReport | None = None,
    on_loss_cooldown: bool = False,
    prior_losses: int = 0,
    daily_budget_ok: bool = True,
    trader_name: str | None = None,
) -> tuple[bool, CouncilResult | None, RugReport]:
    if rug is None:
        rug = await fetch_rug_report(session, mint)

    candidate = TokenCandidate(
        mint=mint,
        symbol=symbol,
        pair=pair or {},
        rugcheck_ok=rug.ok,
        rugcheck_score=rug.score,
        birdeye=birdeye or {},
        twitter_mentions=twitter_mentions,
        score=score,
        breakdown=breakdown or {},
        source=source,
        sell_route_ok=sell_route_ok,
        on_loss_cooldown=on_loss_cooldown,
        prior_losses=prior_losses,
        top_holder_pct=rug.top_holder_pct,
        top5_holder_pct=rug.top5_holder_pct,
        mint_renounced=rug.mint_renounced,
        freeze_safe=rug.freeze_safe,
        daily_budget_ok=daily_budget_ok,
        trader_name=trader_name,
    )

    if not config.USE_MEME_COUNCIL:
        return True, None, rug

    min_approve = config.COPY_COUNCIL_MIN if source == "copy" else config.MEME_COUNCIL_MIN
    result = evaluate(candidate, min_approve)
    return result.approved, result, rug
