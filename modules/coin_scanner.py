from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import aiohttp

from config import (
    AXIOM_AUTH_TOKEN,
    AUTO_BUY,
    DEXSCREENER_BOOSTS_URL,
    DEXSCREENER_PROFILES_URL,
    DEXSCREENER_TOKEN_URL,
    DEXSCREENER_TOP_BOOSTS_URL,
    DEXTOOLS_API_KEY,
    GMGN_SIGNAL_SCORE_BOOST,
    MEME_COUNCIL_MIN,
    RUGCHECK_URL,
    SCAN_GMGN_SIGNALS_BUY,
    SCAN_GMGN_TRENDING_BUY,
    SCAN_GRADUATED_ONLY,
    SCAN_INTERVAL_SECONDS,
    SCAN_MAX_CANDIDATES,
    SCAN_MAX_PUMP_EVAL,
    SCAN_MAX_MCAP_USD,
    SCAN_MIN_AGE_HOURS,
    SCAN_MIN_BUY_PRESSURE,
    SCAN_MIN_LIQUIDITY_USD,
    SCAN_MIN_MCAP_USD,
    SCAN_MIN_SCORE,
    SCAN_MIN_VOLUME_24H,
    SCAN_PUMPFUN_BONDING_MIN_PCT,
    SCAN_PUMPFUN_ALLOW_BONDING,
    SCAN_PUMPFUN_ENABLED,
    SCAN_PUMPFUN_GRADUATED,
    SCAN_PUMPFUN_GRADUATING,
    SCAN_PUMPFUN_LIVE,
    SCAN_PUMPFUN_MAX_AGE_HOURS,
    SCAN_PUMPFUN_MIN_USD_MCAP,
    SCAN_REQUIRE_SELL_TEST,
    SCANNER_BUY_SOL,
    SCORE_LOW_RETRY_HOURS,
    SELL_SLIPPAGE_BPS,
    SOL_MINT,
    TWITTER_BEARER_TOKEN,
    TWITTER_SEARCH_URL,
    TWITTER_TRACKER_ENABLED,
    USE_MEME_COUNCIL,
)
from modules.council_gate import council_gate
from modules.dexscreener_limiter import mark_rate_limited, throttle as dex_throttle
from modules.rugcheck_client import fetch_rug_report
from modules import pumpfun
from modules.utils import clamp, fetch_json, sol_to_lamports

GMGN_TRENDING_URL = "https://gmgn.ai/defi/quotation/v1/rank/sol/swaps/1h"
GMGN_SIGNALS_URL  = "https://gmgn.ai/defi/quotation/v1/signals/sol"
GMGN_TOKEN_URL    = "https://gmgn.ai/defi/quotation/v1/tokens/sol/{mint}"

DEXTOOLS_HOT_URL = "https://api.dextools.io/v2/ranking/sol/hotpools"
AXIOM_TRENDING_URL = "https://api3.axiom.trade/new-trending-v2"

GMGN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://gmgn.ai/",
    "Origin": "https://gmgn.ai",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

if TYPE_CHECKING:
    from modules.executor import Executor

logger = logging.getLogger("solana-bot.coin_scanner")

GRADUATED_DEXES = {"raydium", "orca", "meteora", "pumpswap"}


class CoinScanner:
    def __init__(self, session: aiohttp.ClientSession, executor: "Executor") -> None:
        self.session = session
        self.executor = executor
        # Permanent for this session — filter failures (rug, liq, mcap, etc.)
        self._filter_rejected: set[str] = set()
        # Scored below threshold — re-try after cooldown (markets move)
        self._scored_low: dict[str, float] = {}
        self._pair_cache: dict[str, tuple[dict[str, Any], float]] = {}
        self._running = False
        self._gmgn_signals_warn_at = 0.0

    async def _dex_fetch(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        label: str = "DexScreener",
    ) -> Any | None:
        await dex_throttle()
        try:
            async with self.session.request(
                method,
                url,
                params=params,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 429:
                    mark_rate_limited()
                    logger.warning("%s rate limited — backing off", label)
                    return None
                resp.raise_for_status()
                return await resp.json()
        except Exception as exc:
            if "429" in str(exc):
                mark_rate_limited()
            logger.warning("%s failed: %s", label, exc)
            return None

    def _already_processed(self, mint: str) -> bool:
        if mint in self._filter_rejected:
            return True
        scored_at = self._scored_low.get(mint)
        if scored_at is None:
            return False
        if (time.time() - scored_at) < SCORE_LOW_RETRY_HOURS * 3600:
            return True
        del self._scored_low[mint]
        return False

    def _mark_scored_low(self, mint: str) -> None:
        self._scored_low[mint] = time.time()

    async def _fetch_latest_profiles(self) -> list[str]:
        data = await self._dex_fetch(
            "GET", DEXSCREENER_PROFILES_URL, label="DexScreener profiles",
        )
        if not isinstance(data, list):
            return []
        return [
            p["tokenAddress"] for p in data
            if p.get("chainId") == "solana" and p.get("tokenAddress")
        ]

    async def _fetch_dexscreener_boosts(self) -> list[str]:
        mints: list[str] = []
        for url, label in [
            (DEXSCREENER_BOOSTS_URL, "DexScreener boosts"),
            (DEXSCREENER_TOP_BOOSTS_URL, "DexScreener top boosts"),
        ]:
            data = await self._dex_fetch("GET", url, label=label)
            if not isinstance(data, list):
                continue
            for item in data:
                if item.get("chainId") == "solana" and item.get("tokenAddress"):
                    mints.append(item["tokenAddress"])
        return mints

    @staticmethod
    def _flow_from_pair(pair: dict[str, Any]) -> dict[str, Any]:
        """Buy/sell txn counts from DexScreener — free, no Birdeye key needed."""
        txns = (pair.get("txns") or {}).get("h24") or {}
        buys = float(txns.get("buys", 0) or 0)
        sells = float(txns.get("sells", 0) or 0)
        return {"buy24h": buys, "sell24h": sells, "source": "dexscreener"}

    def _is_graduated(self, pair: dict[str, Any]) -> bool:
        pump = pair.get("pumpfun") or {}
        if pump.get("complete"):
            return True
        dex = (pair.get("dexId") or "").lower()
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        return dex in GRADUATED_DEXES and liq >= SCAN_MIN_LIQUIDITY_USD

    def _pumpfun_bonding_ok(self, pair: dict[str, Any]) -> bool:
        if not SCAN_PUMPFUN_ALLOW_BONDING:
            return False
        pump = pair.get("pumpfun") or {}
        if pump.get("complete"):
            return False
        progress = float(pump.get("bonding_progress", 0) or 0)
        mcap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        return (
            progress >= SCAN_PUMPFUN_BONDING_MIN_PCT
            and mcap >= SCAN_PUMPFUN_MIN_USD_MCAP * 0.5
        )

    async def _verify_sell_route(self, mint: str, buy_quote: dict[str, Any]) -> bool:
        """Confirm Jupiter can sell this token — prevents honeypot/no-route bags."""
        if not SCAN_REQUIRE_SELL_TEST:
            return True
        try:
            out_amount = int(buy_quote.get("outAmount", 0))
            if out_amount <= 0:
                return False
            # Test-sell 10% of expected tokens
            test_amount = max(out_amount // 10, 1)
            quote, _ = await self.executor._get_exit_quote(
                mint, test_amount, SELL_SLIPPAGE_BPS,
            )
            return bool(quote and int(quote.get("outAmount", 0)) > 0)
        except Exception:
            return False

    async def _fetch_pair_data(self, mint: str) -> dict[str, Any] | None:
        cached = self._pair_cache.get(mint)
        if cached and (time.time() - cached[1]) < 90:
            return cached[0]

        url = DEXSCREENER_TOKEN_URL.format(mint=mint)
        data = await self._dex_fetch(
            "GET", url, label=f"DexScreener token {mint[:8]}",
        )
        if not data:
            return cached[0] if cached else None
        pairs = data.get("pairs") or []
        if not pairs:
            return None
        pair = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        self._pair_cache[mint] = (pair, time.time())
        return pair

    async def _rugcheck_score(self, mint: str) -> tuple[bool, float]:
        """RugCheck: 0 = safe, higher = riskier. Reject above 500."""
        rug = await fetch_rug_report(self.session, mint)
        return rug.ok, rug.score

    async def _twitter_mentions(self, symbol: str) -> int:
        if not TWITTER_TRACKER_ENABLED:
            return 0
        if not TWITTER_BEARER_TOKEN or TWITTER_BEARER_TOKEN.startswith("your_"):
            return 0
        if not symbol or symbol == "UNKNOWN":
            return 0
        try:
            headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
            params = {
                "query": f"${symbol} OR #{symbol} -is:retweet lang:en",
                "max_results": 10,
                "tweet.fields": "created_at",
            }
            data = await fetch_json(
                self.session,
                "GET",
                TWITTER_SEARCH_URL,
                params=params,
                headers=headers,
                label=f"Twitter search {symbol}",
            )
            return data.get("meta", {}).get("result_count", 0)
        except Exception:
            return 0

    def _score_token(
        self,
        pair: dict[str, Any],
        rugcheck_ok: bool,
        rugcheck_score: float,
        flow: dict[str, Any],
        twitter_mentions: int,
    ) -> tuple[float, dict[str, Any]]:
        liquidity_usd = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        market_cap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        volume_24h = float(pair.get("volume", {}).get("h24", 0) or 0)
        price_change_5m = float(pair.get("priceChange", {}).get("m5", 0) or 0)
        price_change_1h = float(pair.get("priceChange", {}).get("h1", 0) or 0)
        pair_created = pair.get("pairCreatedAt")
        age_hours = 999.0
        if pair_created:
            created_dt = datetime.fromtimestamp(pair_created / 1000, tz=timezone.utc)
            age_hours = (datetime.now(timezone.utc) - created_dt).total_seconds() / 3600

        buy_pressure = max(price_change_5m, price_change_1h * 0.3, 0)

        # Liquidity (0-20)
        if liquidity_usd >= 100_000:
            liquidity_score = 20
        elif liquidity_usd >= 50_000:
            liquidity_score = 16
        elif liquidity_usd >= 20_000:
            liquidity_score = 12
        elif liquidity_usd >= 10_000:
            liquidity_score = 8
        elif liquidity_usd >= 5_000:
            liquidity_score = 4
        else:
            liquidity_score = 0

        # Market cap (0-15) — sweet spot for memecoins
        if 50_000 <= market_cap <= 500_000:
            mcap_score = 15
        elif 20_000 <= market_cap < 50_000:
            mcap_score = 12
        elif 500_000 < market_cap <= 1_000_000:
            mcap_score = 10
        elif 1_000_000 < market_cap <= 2_000_000:
            mcap_score = 6
        else:
            mcap_score = 3

        # Age (0-15) — newer tokens score higher
        if age_hours <= 1:
            age_score = 15
        elif age_hours <= 6:
            age_score = 12
        elif age_hours <= 24:
            age_score = 8
        elif age_hours <= 72:
            age_score = 4
        else:
            age_score = 1

        # Buy pressure (0-20)
        buy_pressure_score = clamp(buy_pressure * 2, 0, 20)

        # Volume (0-15)
        if volume_24h >= 500_000:
            volume_score = 15
        elif volume_24h >= 200_000:
            volume_score = 12
        elif volume_24h >= 100_000:
            volume_score = 9
        elif volume_24h >= 50_000:
            volume_score = 6
        elif volume_24h >= 10_000:
            volume_score = 3
        else:
            volume_score = 0

        # Twitter mentions (0-15)
        if twitter_mentions >= 50:
            twitter_score = 15
        elif twitter_mentions >= 20:
            twitter_score = 12
        elif twitter_mentions >= 10:
            twitter_score = 9
        elif twitter_mentions >= 5:
            twitter_score = 6
        elif twitter_mentions >= 1:
            twitter_score = 3
        else:
            twitter_score = 0

        # RugCheck safety score (0-10) — lower risk score = safer = more points
        if not rugcheck_ok:
            safety_score = 0
        elif rugcheck_score <= 100:
            safety_score = 10   # very safe
        elif rugcheck_score <= 200:
            safety_score = 7
        elif rugcheck_score <= 350:
            safety_score = 4
        else:
            safety_score = 1   # borderline — passes 500 filter but still risky

        # DexScreener buy/sell txn ratio bonus (0-5) — free flow data
        buy_24h = float(flow.get("buy24h", 0) or 0)
        sell_24h = float(flow.get("sell24h", 0) or 0)
        if buy_24h + sell_24h > 0:
            buy_ratio = buy_24h / (buy_24h + sell_24h)
            flow_score = clamp(buy_ratio * 5, 0, 5)
        else:
            flow_score = 0

        total = (
            liquidity_score
            + mcap_score
            + age_score
            + buy_pressure_score
            + volume_score
            + twitter_score
            + safety_score
            + flow_score
        )
        total = clamp(total, 0, 100)

        breakdown = {
            "liquidity": f"{liquidity_score}/20 (${liquidity_usd:,.0f})",
            "market_cap": f"{mcap_score}/15 (${market_cap:,.0f})",
            "age": f"{age_score}/15 ({age_hours:.1f}h)",
            "buy_pressure": f"{buy_pressure_score:.0f}/20 ({buy_pressure:.1f}%)",
            "volume": f"{volume_score}/15 (${volume_24h:,.0f})",
            "twitter": f"{twitter_score}/15 ({twitter_mentions} mentions)",
            "rugcheck": f"{safety_score:.0f}/10 (score {rugcheck_score:.0f})",
            "flow_ratio": f"{flow_score:.1f}/5 (DexScreener {buy_24h:.0f}b/{sell_24h:.0f}s)",
            "total": f"{total:.0f}/100",
        }
        return total, breakdown

    async def _evaluate_token(self, mint: str) -> None:
        if self._already_processed(mint):
            return

        pair = await self._fetch_pair_data(mint)
        if not pair:
            self._filter_rejected.add(mint)
            return

        symbol = pair.get("baseToken", {}).get("symbol", "UNKNOWN")
        liquidity_usd = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        market_cap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        volume_24h = float(pair.get("volume", {}).get("h24", 0) or 0)
        dex = pair.get("dexId", "?")

        if liquidity_usd < SCAN_MIN_LIQUIDITY_USD:
            logger.info("Scanner skip %s — liquidity $%.0f < $%.0f",
                        symbol, liquidity_usd, SCAN_MIN_LIQUIDITY_USD)
            self._filter_rejected.add(mint)
            return

        if market_cap < SCAN_MIN_MCAP_USD:
            logger.info("Scanner skip %s — mcap $%.0f < $%.0f",
                        symbol, market_cap, SCAN_MIN_MCAP_USD)
            self._filter_rejected.add(mint)
            return

        if market_cap > SCAN_MAX_MCAP_USD:
            logger.info("Scanner skip %s — mcap $%.0f > $%.0f (too big)",
                        symbol, market_cap, SCAN_MAX_MCAP_USD)
            self._filter_rejected.add(mint)
            return

        if SCAN_GRADUATED_ONLY and not self._is_graduated(pair) and not self._pumpfun_bonding_ok(pair):
            logger.info("Scanner skip %s — not graduated (dex=%s, need Raydium/Orca/PumpSwap or pump graduating)",
                        symbol, dex)
            self._filter_rejected.add(mint)
            return

        pair_created = pair.get("pairCreatedAt")
        age_hours = 999.0
        if pair_created:
            age_hours = (datetime.now(timezone.utc) - datetime.fromtimestamp(
                pair_created / 1000, tz=timezone.utc)).total_seconds() / 3600
            if age_hours < SCAN_MIN_AGE_HOURS:
                logger.info("Scanner skip %s — only %.1fh old (min %.1fh)",
                            symbol, age_hours, SCAN_MIN_AGE_HOURS)
                self._filter_rejected.add(mint)
                return

        rugcheck_ok, rugcheck_score = await self._rugcheck_score(mint)
        if not rugcheck_ok:
            logger.info("Scanner skip %s — RugCheck flagged (score %.0f)", symbol, rugcheck_score)
            self._filter_rejected.add(mint)
            return

        flow = self._flow_from_pair(pair)
        twitter_mentions = await self._twitter_mentions(symbol)
        score, breakdown = self._score_token(pair, rugcheck_ok, rugcheck_score, flow, twitter_mentions)

        logger.info(
            "Scanned %s (%s) | score %.0f | mcap $%s | liq $%s | vol $%s | dex %s | age %.1fh",
            symbol, mint[:8], score,
            f"{market_cap:,.0f}", f"{liquidity_usd:,.0f}",
            f"{volume_24h:,.0f}", dex, age_hours,
        )

        if score < SCAN_MIN_SCORE:
            self._mark_scored_low(mint)
            return

        await self._attempt_buy(
            mint=mint,
            symbol=symbol,
            pair=pair,
            score=score,
            breakdown=breakdown,
            rugcheck_ok=rugcheck_ok,
            rugcheck_score=rugcheck_score,
            birdeye=flow,
            twitter_mentions=twitter_mentions,
            source="scanner",
            reason_extra=f"mcap ${market_cap:,.0f} | liq ${liquidity_usd:,.0f} | {dex}",
        )

    @staticmethod
    def _pair_buy_pressure(pair: dict[str, Any]) -> float:
        txns = pair.get("txns", {}) or {}
        h24 = txns.get("h24", {}) or {}
        buys = float(h24.get("buys", 0) or 0)
        sells = float(h24.get("sells", 0) or 0)
        total = buys + sells
        return (buys / total * 100.0) if total > 0 else 0.0

    @staticmethod
    def _pair_volume_24h(pair: dict[str, Any]) -> float:
        return float(pair.get("volume", {}).get("h24", 0) or 0)

    def _scanner_catalyst_ok(
        self,
        *,
        source: str,
        raw_score: float,
        pair: dict[str, Any],
        twitter_mentions: int,
    ) -> tuple[bool, str]:
        """Scanner buys need a real catalyst — not random trending noise."""
        src = source.lower()
        pressure = self._pair_buy_pressure(pair)
        vol = self._pair_volume_24h(pair)

        if "gmgn signals" in src:
            return True, f"GMGN smart-money buy signal | flow {pressure:.0f}%"

        if "gmgn trending" in src:
            return False, "GMGN trending disabled — volume list alone is not a reason"

        if src == "scanner":
            info = pair.get("info", {}) or {}
            has_social = bool(info.get("websites") or info.get("socials"))
            strong = (
                raw_score >= SCAN_MIN_SCORE + 2
                and pressure >= SCAN_MIN_BUY_PRESSURE
                and vol >= SCAN_MIN_VOLUME_24H
            )
            if strong:
                return True, (
                    f"DexScreener feed | score {raw_score:.0f} | "
                    f"flow {pressure:.0f}% | vol ${vol:,.0f}"
                )
            if raw_score >= SCAN_MIN_SCORE and pressure >= SCAN_MIN_BUY_PRESSURE and vol >= SCAN_MIN_VOLUME_24H:
                if twitter_mentions >= 1 or has_social:
                    return True, (
                        f"DexScreener feed + social | score {raw_score:.0f} | "
                        f"flow {pressure:.0f}% | vol ${vol:,.0f}"
                    )
                return False, f"score ok but no social proof ({twitter_mentions} tweets)"
            return False, (
                f"weak setup — score {raw_score:.0f}, flow {pressure:.0f}%, vol ${vol:,.0f}"
            )

        if src.startswith("pump"):
            pump_floor = max(SCAN_MIN_SCORE - 5, 70)
            if raw_score >= pump_floor and pressure >= max(SCAN_MIN_BUY_PRESSURE - 5, 45):
                return True, f"Pump.fun {source} | score {raw_score:.0f} | flow {pressure:.0f}%"
            return False, "pump candidate below score/flow floor"

        return True, source

    async def _attempt_buy(
        self,
        *,
        mint: str,
        symbol: str,
        pair: dict[str, Any],
        score: float,
        breakdown: dict[str, Any],
        rugcheck_ok: bool,
        rugcheck_score: float,
        birdeye: dict[str, Any],
        twitter_mentions: int,
        source: str,
        reason_extra: str,
        score_threshold: float | None = None,
        raw_score: float | None = None,
    ) -> None:
        raw = raw_score if raw_score is not None else score
        catalyst_ok, catalyst_reason = self._scanner_catalyst_ok(
            source=source,
            raw_score=raw,
            pair=pair,
            twitter_mentions=twitter_mentions,
        )
        if not catalyst_ok:
            logger.info("%s skip %s (score %.0f) — no catalyst: %s", source, symbol, score, catalyst_reason)
            return

        can_buy, skip = await self.executor.can_trade(mint)
        if not can_buy:
            logger.info("%s skip %s (score %.0f) — %s", source, symbol, score, skip)
            return

        buy_quote = await self.executor.get_quote(
            SOL_MINT, mint, sol_to_lamports(SCANNER_BUY_SOL),
        )
        sell_ok = await self._verify_sell_route(mint, buy_quote)
        if not sell_ok:
            logger.info(
                "%s skip %s (score %.0f) — cannot sell on Jupiter (honeypot/no route)",
                source, symbol, score,
            )
            return

        rm = self.executor.risk_manager
        daily_ok = True
        if rm:
            ok_budget, _ = await rm.can_open_new_trade()
            daily_ok = ok_budget

        approved, council, _rug = await council_gate(
            self.session,
            mint=mint,
            symbol=symbol,
            pair=pair,
            source=source,
            sell_route_ok=sell_ok,
            score=score,
            breakdown=breakdown,
            birdeye=birdeye,
            twitter_mentions=twitter_mentions,
            on_loss_cooldown=rm.on_cooldown(mint) if rm else False,
            prior_losses=rm.prior_loss_count(mint) if rm else 0,
            daily_budget_ok=daily_ok,
        )
        if not approved:
            label = council.score if council else "bypass"
            logger.info("%s skip %s — HERMES Council %s rejected", source, symbol, label)
            return
        if council:
            breakdown = {**breakdown, "council": council.score}
            breakdown["council_detail"] = " | ".join(
                f"{v.code}:{v.vote.value}" for v in council.votes[:7]
            )

        reason = f"{catalyst_reason} | {reason_extra}"
        mcap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)

        if not AUTO_BUY:
            logger.info("HERMES signal (alerts-only) — %s scored %.0f (%s)", symbol, score, source)
            rm = self.executor.risk_manager
            if rm and rm.alerter:
                await rm.alerter.send_hermes_signal(
                    symbol=symbol,
                    mint=mint,
                    source=source,
                    score=score,
                    council_result=council,
                    reason=reason,
                    mcap_usd=mcap,
                    liq_usd=liq,
                )
            return

        logger.info("BUY signal — %s scored %.0f (%s)", symbol, score, source)
        buy_sol = await self.executor.calc_buy_size_sol()
        await self.executor.buy_token(
            mint=mint,
            amount_sol=buy_sol,
            reason=reason,
            symbol=symbol,
            score_breakdown=breakdown,
        )

    async def _fetch_gmgn_trending(self) -> list[str]:
        """Fetch trending tokens from GMGN ranked by 1h swap volume."""
        try:
            params = {
                "limit": "20",
                "orderby": "swaps",
                "direction": "desc",
                "filters[]": ["renounced", "frozen"],
            }
            data = await fetch_json(
                self.session, "GET", GMGN_TRENDING_URL,
                params=params, headers=GMGN_HEADERS, label="GMGN trending",
            )
            tokens = data.get("data", {}).get("rank", []) or []
            mints = [t.get("address", "") for t in tokens if t.get("address")]
            if mints:
                logger.info("GMGN trending: %d tokens", len(mints))
            return mints
        except Exception as exc:
            logger.warning("GMGN trending fetch failed: %s", exc)
            return []

    async def _fetch_gmgn_signals(self) -> list[str]:
        """GMGN removed this endpoint (404) — use copy trading instead."""
        if not SCAN_GMGN_SIGNALS_BUY:
            return []
        now = time.time()
        if now - self._gmgn_signals_warn_at > 3600:
            self._gmgn_signals_warn_at = now
            logger.warning(
                "GMGN signals API is dead (404) — set SCAN_GMGN_SIGNALS_BUY=false on Railway"
            )
        return []

    async def _evaluate_gmgn_token(self, mint: str, source: str) -> None:
        """GMGN signals = smart money. GMGN trending = disabled by default (too noisy)."""
        if source == "trending" and not SCAN_GMGN_TRENDING_BUY:
            return
        if source == "signals" and not SCAN_GMGN_SIGNALS_BUY:
            return
        if self._already_processed(mint):
            return

        try:
            pair = await self._fetch_pair_data(mint)
            if not pair:
                return

            symbol = pair.get("baseToken", {}).get("symbol", "UNKNOWN")
            rugcheck_ok, rugcheck_score = await self._rugcheck_score(mint)
            if not rugcheck_ok:
                logger.info("GMGN skip %s — RugCheck flagged", symbol)
                self._filter_rejected.add(mint)
                return

            flow = self._flow_from_pair(pair)
            twitter_mentions = await self._twitter_mentions(symbol)
            raw_score, breakdown = self._score_token(
                pair, rugcheck_ok, rugcheck_score, flow, twitter_mentions,
            )

            boost = GMGN_SIGNAL_SCORE_BOOST if source == "signals" else 0
            display_score = min(raw_score + boost, 100)
            liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
            logger.info(
                "GMGN [%s] %s (%s) — raw %.0f → %.0f | liq $%s",
                source, symbol, mint[:8], raw_score, display_score, f"{liq:,.0f}",
            )

            if raw_score < SCAN_MIN_SCORE:
                self._mark_scored_low(mint)
                logger.info("GMGN skip %s — raw score %.0f < %d", symbol, raw_score, SCAN_MIN_SCORE)
                return

            jupiter_price = await self.executor.get_token_price_usd(mint)
            if not jupiter_price or jupiter_price <= 0:
                logger.info("GMGN skip %s — Jupiter has no price", symbol)
                return

            await self._attempt_buy(
                mint=mint,
                symbol=symbol,
                pair=pair,
                score=display_score,
                raw_score=raw_score,
                breakdown=breakdown,
                rugcheck_ok=rugcheck_ok,
                rugcheck_score=rugcheck_score,
                birdeye=flow,
                twitter_mentions=twitter_mentions,
                source=f"GMGN {source}",
                reason_extra=f"score {display_score:.0f}/100 | liq ${liq:,.0f}",
            )
        except Exception as exc:
            logger.error("Error evaluating GMGN token %s: %s", mint[:8], exc)

    async def _evaluate_pumpfun_coin(self, coin: dict[str, Any], source: str) -> None:
        mint = coin.get("mint", "")
        if not mint or self._already_processed(mint):
            return

        try:
            symbol = coin.get("symbol", "UNKNOWN")
            complete = bool(coin.get("complete"))
            progress = pumpfun.bonding_progress(coin)
            mcap = pumpfun.usd_market_cap(coin)
            age = pumpfun.coin_age_hours(coin)

            if age > SCAN_PUMPFUN_MAX_AGE_HOURS:
                return
            if mcap < SCAN_PUMPFUN_MIN_USD_MCAP * 0.5:
                return
            if not complete and progress < SCAN_PUMPFUN_BONDING_MIN_PCT:
                return

            pair = await self._fetch_pair_data(mint)
            if pair:
                pair.setdefault("pumpfun", {})
                pair["pumpfun"].update({
                    "complete": complete,
                    "bonding_progress": progress,
                    "source": source,
                })
            else:
                pair = pumpfun.synthetic_pair_from_coin(coin)

            if coin.get("nsfw"):
                self._filter_rejected.add(mint)
                return

            rugcheck_ok, rugcheck_score = await self._rugcheck_score(mint)
            if not rugcheck_ok:
                logger.info("Pump.fun skip %s — RugCheck flagged", symbol)
                self._filter_rejected.add(mint)
                return

            flow = self._flow_from_pair(pair)
            twitter_mentions = await self._twitter_mentions(symbol)
            score, breakdown = self._score_token(
                pair, rugcheck_ok, rugcheck_score, flow, twitter_mentions,
            )

            # Boost graduating pump tokens (Axiom Pulse "final stretch" style)
            if not complete and progress >= SCAN_PUMPFUN_BONDING_MIN_PCT:
                score = min(100.0, score + 8.0)
                breakdown["pumpfun"] = f"+8 graduating ({progress:.0f}% curve)"
            elif complete:
                score = min(100.0, score + 5.0)
                breakdown["pumpfun"] = "+5 freshly graduated"

            logger.info(
                "Pump.fun [%s] %s (%s) — score %.0f | %s | mcap $%s | curve %.0f%%",
                source, symbol, mint[:8], score,
                "graduated" if complete else "graduating",
                f"{mcap:,.0f}", progress,
            )

            threshold = SCAN_MIN_SCORE - (3 if complete else 5)
            if score < threshold:
                self._mark_scored_low(mint)
                return

            await self._attempt_buy(
                mint=mint,
                symbol=symbol,
                pair=pair,
                score=score,
                breakdown=breakdown,
                rugcheck_ok=rugcheck_ok,
                rugcheck_score=rugcheck_score,
                birdeye=flow,
                twitter_mentions=twitter_mentions,
                source=f"Pump.fun {source}",
                reason_extra=(
                    f"{'graduated' if complete else f'graduating {progress:.0f}%'} | "
                    f"mcap ${mcap:,.0f}"
                ),
            )
        except Exception as exc:
            logger.error("Error evaluating Pump.fun %s: %s", mint[:8], exc)

    async def _run_pumpfun_cycle(self) -> None:
        if not SCAN_PUMPFUN_ENABLED:
            return

        tasks: list[tuple[str, list[dict[str, Any]]]] = []
        if SCAN_PUMPFUN_LIVE:
            live = await pumpfun.fetch_live(self.session)
            tasks.append(("live", live))
        if SCAN_PUMPFUN_GRADUATING:
            graduating = await pumpfun.fetch_graduating(self.session)
            tasks.append(("graduating", graduating))
        if SCAN_PUMPFUN_GRADUATED:
            graduated = await pumpfun.fetch_graduated_recent(self.session)
            tasks.append(("graduated", graduated))

        for label, coins in tasks:
            for coin in coins[:SCAN_MAX_PUMP_EVAL]:
                try:
                    await self._evaluate_pumpfun_coin(coin, label)
                    await asyncio.sleep(0.6)
                except Exception as exc:
                    mint = coin.get("mint", "")[:8]
                    logger.error("Pump.fun cycle error %s: %s", mint, exc)

    async def _fetch_dextools_hot(self) -> list[str]:
        if not DEXTOOLS_API_KEY or DEXTOOLS_API_KEY.startswith("your_"):
            return []
        try:
            headers = {"X-API-KEY": DEXTOOLS_API_KEY, "Accept": "application/json"}
            data = await fetch_json(
                self.session, "GET", DEXTOOLS_HOT_URL,
                headers=headers, label="DexTools hot",
            )
            results = data.get("data", {}).get("results", []) or data.get("results", []) or []
            mints = []
            for item in results[:20]:
                token = item.get("token") or item.get("mainToken") or {}
                addr = token.get("address") or item.get("address") or ""
                if addr:
                    mints.append(addr)
            if mints:
                logger.info("DexTools hot pools: %d token(s)", len(mints))
            return mints
        except Exception as exc:
            logger.warning("DexTools fetch failed: %s", exc)
            return []

    async def _fetch_axiom_trending(self) -> list[str]:
        if not AXIOM_AUTH_TOKEN or AXIOM_AUTH_TOKEN.startswith("your_"):
            return []
        try:
            headers = {
                "Accept": "application/json",
                "Cookie": f"auth-token={AXIOM_AUTH_TOKEN}",
                "User-Agent": GMGN_HEADERS["User-Agent"],
                "Referer": "https://axiom.trade/",
            }
            data = await fetch_json(
                self.session, "GET", AXIOM_TRENDING_URL,
                params={"timePeriod": "1h"},
                headers=headers, label="Axiom trending",
            )
            tokens = data if isinstance(data, list) else data.get("tokens", []) or data.get("data", []) or []
            mints = []
            for t in tokens[:20]:
                addr = t.get("tokenAddress") or t.get("mint") or t.get("address") or ""
                if addr:
                    mints.append(addr)
            if mints:
                logger.info("Axiom trending: %d token(s)", len(mints))
            return mints
        except Exception as exc:
            logger.warning("Axiom trending fetch failed: %s", exc)
            return []

    async def _scan_cycle(self) -> None:
        candidates: list[str] = []

        profiles = await self._fetch_latest_profiles()
        boosts = await self._fetch_dexscreener_boosts()
        gmgn_trending = await self._fetch_gmgn_trending()
        gmgn_signals: list[str] = []
        if SCAN_GMGN_SIGNALS_BUY:
            gmgn_signals = await self._fetch_gmgn_signals()
        dextools = await self._fetch_dextools_hot()
        axiom = await self._fetch_axiom_trending()

        dex_count = len(profiles) + len(boosts)
        for source_mints in (profiles, boosts, dextools, axiom):
            for mint in source_mints:
                if mint and not self._already_processed(mint) and mint not in candidates:
                    candidates.append(mint)

        if candidates or gmgn_trending or gmgn_signals:
            logger.info(
                "Scanner cycle — %d candidates (dex=%d dextools=%d axiom=%d) | "
                "GMGN trending=%d signals=%d",
                len(candidates), dex_count,
                len(dextools), len(axiom), len(gmgn_trending), len(gmgn_signals),
            )

        for mint in candidates[:SCAN_MAX_CANDIDATES]:
            try:
                await self._evaluate_token(mint)
            except Exception as exc:
                logger.error("Error evaluating %s: %s", mint[:8], exc)

        if SCAN_GMGN_SIGNALS_BUY:
            for mint in gmgn_signals[:5]:
                try:
                    await self._evaluate_gmgn_token(mint, "signals")
                except Exception as exc:
                    logger.error("Error evaluating GMGN signal %s: %s", mint[:8], exc)

        if SCAN_GMGN_TRENDING_BUY:
            for mint in gmgn_trending[:10]:
                try:
                    await self._evaluate_gmgn_token(mint, "trending")
                except Exception as exc:
                    logger.error("Error evaluating GMGN trending %s: %s", mint[:8], exc)

        await self._run_pumpfun_cycle()

    async def run(self) -> None:
        self._running = True
        logger.info(
            "Market scanner started — DexScreener (free) + GMGN"
            + (" + Pump.fun" if SCAN_PUMPFUN_ENABLED else "")
            + (" + DexTools" if DEXTOOLS_API_KEY else "")
            + (" + Axiom" if AXIOM_AUTH_TOKEN else "")
            + f" every {SCAN_INTERVAL_SECONDS}s | council {'ON' if USE_MEME_COUNCIL else 'OFF'}"
            f" ({MEME_COUNCIL_MIN}/7 HERMES) | min score {SCAN_MIN_SCORE}"
            f" | GMGN trending={'ON' if SCAN_GMGN_TRENDING_BUY else 'OFF'}"
            f" | GMGN signals={'ON' if SCAN_GMGN_SIGNALS_BUY else 'OFF'}",
        )

        while self._running:
            try:
                await self._scan_cycle()
            except Exception as exc:
                logger.error("Scanner cycle error: %s", exc)
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._running = False
