"""
HERMES Meme Council — 7 rule-based agents (zero LLM credits).

Mirrors the futures HERMES engine: multiple agents vote, then HEPHAESTUS
validates execution. Default: 5/7 must approve, zero rejects.

Agents:
  GUARD    — RugCheck, loss cooldown, repeat losers
  DEPTH    — liquidity, mcap, graduated pool
  FLOW     — buy pressure / volume
  SOCIAL   — Twitter + DexScreener socials
  WHALE    — holder concentration (top1 / top5)
  HYDRA    — scanner score + daily trade budget
  ROUTE    — Jupiter sell route (honeypot guard)
  HEPHAESTUS — execution preflight (not a voter)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from config import (
    COPY_REBUY_COOLDOWN_HOURS,
    MEME_COUNCIL_MIN,
    SCAN_MIN_MCAP_USD,
    SCAN_MIN_SCORE,
    SCAN_MAX_MCAP_USD,
    SCAN_MIN_LIQUIDITY_USD,
    SCAN_MIN_BUY_PRESSURE,
    SCAN_MIN_VOLUME_24H,
    SCAN_PUMPFUN_ALLOW_BONDING,
    SCAN_PUMPFUN_BONDING_MIN_PCT,
    SCAN_PUMPFUN_MIN_USD_MCAP,
    WHALE_MAX_TOP1_PCT,
    WHALE_MAX_TOP5_PCT,
)

logger = logging.getLogger("solana-bot.council")

GRADUATED_DEXES = {"raydium", "orca", "meteora", "pumpswap"}
COUNCIL_SIZE = 7


class Vote(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"
    ABSTAIN = "abstain"


@dataclass
class AgentVote:
    name: str
    code: str
    vote: Vote
    reason: str


@dataclass
class CouncilResult:
    approved: bool
    score: str
    votes: list[AgentVote] = field(default_factory=list)

    def summary_lines(self) -> list[str]:
        icons = {Vote.APPROVE: "✅", Vote.REJECT: "❌", Vote.ABSTAIN: "⏭"}
        lines = [f"<b>🛡️ HERMES Council {self.score}</b>"]
        for v in self.votes:
            lines.append(f"{icons[v.vote]} <b>{v.name}</b> — {v.reason}")
        return lines


@dataclass
class TokenCandidate:
    mint: str
    symbol: str
    pair: dict[str, Any]
    rugcheck_ok: bool
    rugcheck_score: float
    birdeye: dict[str, Any]
    twitter_mentions: int
    score: float
    breakdown: dict[str, Any]
    source: str = "scanner"
    sell_route_ok: bool = True
    on_loss_cooldown: bool = False
    prior_losses: int = 0
    top_holder_pct: float = 0.0
    top5_holder_pct: float = 0.0
    mint_renounced: bool = True
    freeze_safe: bool = True
    daily_budget_ok: bool = True
    trader_name: str | None = None


def _guard(candidate: TokenCandidate) -> AgentVote:
    if candidate.on_loss_cooldown:
        return AgentVote(
            "GUARD", "GRD", Vote.REJECT,
            f"loss cooldown ({COPY_REBUY_COOLDOWN_HOURS}h)",
        )
    if candidate.prior_losses >= 2:
        return AgentVote(
            "GUARD", "GRD", Vote.REJECT,
            f"burned {candidate.prior_losses}x on this mint",
        )
    if not candidate.rugcheck_ok:
        return AgentVote(
            "GUARD", "GRD", Vote.REJECT,
            f"RugCheck fail (score {candidate.rugcheck_score:.0f})",
        )
    if not candidate.mint_renounced:
        return AgentVote("GUARD", "GRD", Vote.REJECT, "mint authority not renounced")
    if not candidate.freeze_safe:
        return AgentVote("GUARD", "GRD", Vote.REJECT, "freeze authority active")
    return AgentVote(
        "GUARD", "GRD", Vote.APPROVE,
        f"RugCheck ok ({candidate.rugcheck_score:.0f}) · mint safe",
    )


def _depth(candidate: TokenCandidate) -> AgentVote:
    liq = float(candidate.pair.get("liquidity", {}).get("usd", 0) or 0)
    mcap = float(candidate.pair.get("marketCap") or candidate.pair.get("fdv") or 0)
    dex = (candidate.pair.get("dexId") or "?").lower()
    pump = candidate.pair.get("pumpfun") or {}
    progress = float(pump.get("bonding_progress", 0) or 0)
    on_bonding = dex == "pump.fun" or (not pump.get("complete") and progress > 0)

    if on_bonding and SCAN_PUMPFUN_ALLOW_BONDING:
        if progress >= SCAN_PUMPFUN_BONDING_MIN_PCT and mcap >= SCAN_PUMPFUN_MIN_USD_MCAP * 0.5:
            return AgentVote(
                "DEPTH", "DEP", Vote.APPROVE,
                f"pump graduating {progress:.0f}% | mcap ${mcap:,.0f}",
            )
        return AgentVote(
            "DEPTH", "DEP", Vote.REJECT,
            f"pump too early ({progress:.0f}% / need {SCAN_PUMPFUN_BONDING_MIN_PCT:.0f}%)",
        )

    if liq < SCAN_MIN_LIQUIDITY_USD:
        return AgentVote("DEPTH", "DEP", Vote.REJECT, f"liq ${liq:,.0f} too thin")
    if mcap < SCAN_MIN_MCAP_USD or mcap > SCAN_MAX_MCAP_USD:
        return AgentVote("DEPTH", "DEP", Vote.REJECT, f"mcap ${mcap:,.0f} out of range")
    if dex not in GRADUATED_DEXES:
        return AgentVote("DEPTH", "DEP", Vote.REJECT, f"not graduated (dex={dex})")
    return AgentVote(
        "DEPTH", "DEP", Vote.APPROVE,
        f"liq ${liq:,.0f} | mcap ${mcap:,.0f} | {dex}",
    )


def _flow(candidate: TokenCandidate) -> AgentVote:
    txns = candidate.pair.get("txns", {}) or {}
    h24 = txns.get("h24", {}) or {}
    buys = float(h24.get("buys", 0) or 0)
    sells = float(h24.get("sells", 0) or 0)
    total = buys + sells
    pressure = (buys / total * 100.0) if total > 0 else 50.0

    be = candidate.birdeye or {}
    be_buys = float(be.get("buy24h", 0) or 0)
    be_sells = float(be.get("sell24h", 0) or 0)
    be_total = be_buys + be_sells
    if be_total > 20:
        pressure = be_buys / be_total * 100.0

    vol = float(candidate.pair.get("volume", {}).get("h24", 0) or 0)

    if candidate.source == "copy":
        return AgentVote("FLOW", "FLW", Vote.APPROVE, "copy trade — flow skipped")

    if pressure < 45:
        return AgentVote(
            "FLOW", "FLW", Vote.REJECT,
            f"sell-heavy {pressure:.0f}% buy pressure",
        )
    if pressure < SCAN_MIN_BUY_PRESSURE:
        return AgentVote(
            "FLOW", "FLW", Vote.REJECT,
            f"buy pressure {pressure:.0f}% < {SCAN_MIN_BUY_PRESSURE:.0f}%",
        )
    if vol < SCAN_MIN_VOLUME_24H:
        return AgentVote(
            "FLOW", "FLW", Vote.REJECT,
            f"vol ${vol:,.0f} < ${SCAN_MIN_VOLUME_24H:,.0f}",
        )
    return AgentVote(
        "FLOW", "FLW", Vote.APPROVE,
        f"strong flow {pressure:.0f}% | vol ${vol:,.0f}",
    )


def _social(candidate: TokenCandidate) -> AgentVote:
    info = candidate.pair.get("info", {}) or {}
    websites = info.get("websites") or []
    socials = info.get("socials") or []
    has_social = bool(websites or socials)
    mentions = candidate.twitter_mentions

    if mentions >= 3:
        return AgentVote("SOCIAL", "SOC", Vote.APPROVE, f"{mentions} Twitter mentions")
    if candidate.source in ("copy", "twitter") and mentions >= 1:
        return AgentVote("SOCIAL", "SOC", Vote.APPROVE, "caller / copy signal")
    if "gmgn signals" in (candidate.source or "").lower():
        return AgentVote("SOCIAL", "SOC", Vote.APPROVE, "GMGN smart-money signal")
    if has_social and candidate.score >= SCAN_MIN_SCORE:
        return AgentVote("SOCIAL", "SOC", Vote.APPROVE, "DexScreener socials listed")
    src = (candidate.source or "").lower()
    if src in ("scanner",) or "gmgn trending" in src:
        return AgentVote("SOCIAL", "SOC", Vote.REJECT, "no social proof — scanner skip")
    if mentions == 0 and not has_social:
        return AgentVote("SOCIAL", "SOC", Vote.ABSTAIN, "no social signal")
    return AgentVote("SOCIAL", "SOC", Vote.APPROVE, "weak but present socials")


def _whale(candidate: TokenCandidate) -> AgentVote:
    top1 = candidate.top_holder_pct
    top5 = candidate.top5_holder_pct
    if top1 <= 0 and top5 <= 0:
        if candidate.source == "copy":
            return AgentVote("WHALE", "WHL", Vote.ABSTAIN, "holder data n/a — copy vetted")
        return AgentVote("WHALE", "WHL", Vote.ABSTAIN, "holder data unavailable")
    if top1 > WHALE_MAX_TOP1_PCT:
        return AgentVote(
            "WHALE", "WHL", Vote.REJECT,
            f"top holder {top1:.1f}% > {WHALE_MAX_TOP1_PCT:.0f}%",
        )
    if top5 > WHALE_MAX_TOP5_PCT:
        return AgentVote(
            "WHALE", "WHL", Vote.REJECT,
            f"top 5 hold {top5:.1f}% > {WHALE_MAX_TOP5_PCT:.0f}%",
        )
    return AgentVote(
        "WHALE", "WHL", Vote.APPROVE,
        f"top1 {top1:.1f}% · top5 {top5:.1f}%",
    )


def _hydra(candidate: TokenCandidate) -> AgentVote:
    if not candidate.daily_budget_ok:
        return AgentVote("HYDRA", "HYD", Vote.REJECT, "daily loss/profit limit or max buys hit")
    if candidate.source == "copy" and candidate.trader_name:
        return AgentVote(
            "HYDRA", "HYD", Vote.APPROVE,
            f"copy budget ok — {candidate.trader_name}",
        )
    if candidate.source == "twitter":
        return AgentVote("HYDRA", "HYD", Vote.APPROVE, "Twitter signal — budget ok")
    src = (candidate.source or "").lower()
    if "gmgn trending" in src:
        return AgentVote(
            "HYDRA", "HYD", Vote.REJECT,
            "GMGN trending alone — need copy/signals/score+flow",
        )
    if candidate.score >= SCAN_MIN_SCORE:
        return AgentVote(
            "HYDRA", "HYD", Vote.APPROVE,
            f"score {candidate.score:.0f} ≥ {SCAN_MIN_SCORE}",
        )
    return AgentVote(
        "HYDRA", "HYD", Vote.REJECT,
        f"score {candidate.score:.0f} < {SCAN_MIN_SCORE}",
    )


def _route(candidate: TokenCandidate) -> AgentVote:
    if not candidate.sell_route_ok:
        return AgentVote("ROUTE", "RTE", Vote.REJECT, "no Jupiter sell route (honeypot risk)")
    return AgentVote("ROUTE", "RTE", Vote.APPROVE, "sell route verified")


def _hephaestus(candidate: TokenCandidate) -> AgentVote:
    """Execution preflight — must pass after council consensus."""
    if not candidate.sell_route_ok:
        return AgentVote("HEPHAESTUS", "HPH", Vote.REJECT, "no sell route for execution")
    if not candidate.daily_budget_ok:
        return AgentVote("HEPHAESTUS", "HPH", Vote.REJECT, "risk manager blocked new buys")
    return AgentVote("HEPHAESTUS", "HPH", Vote.APPROVE, "execution route clear")


def evaluate(candidate: TokenCandidate, min_approve: int | None = None) -> CouncilResult:
    needed = min_approve if min_approve is not None else MEME_COUNCIL_MIN
    votes = [
        _guard(candidate),
        _depth(candidate),
        _flow(candidate),
        _social(candidate),
        _whale(candidate),
        _hydra(candidate),
        _route(candidate),
    ]
    approve = sum(1 for v in votes if v.vote == Vote.APPROVE)
    reject = sum(1 for v in votes if v.vote == Vote.REJECT)

    approved = approve >= needed and reject == 0
    if approved:
        preflight = _hephaestus(candidate)
        votes.append(preflight)
        if preflight.vote == Vote.REJECT:
            approved = False

    score = f"{approve}/{COUNCIL_SIZE}"
    icons = " ".join(
        "✅" if v.vote == Vote.APPROVE else "❌" if v.vote == Vote.REJECT else "⏭"
        for v in votes[:COUNCIL_SIZE]
    )
    logger.info(
        "HERMES %s %s — %s (%s) %s",
        score, "FIRE" if approved else "SKIP",
        candidate.symbol, candidate.source, icons,
    )
    return CouncilResult(approved=approved, score=score, votes=votes)
