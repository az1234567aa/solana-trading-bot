"""
Twitter / X caller tracker — scans influencer timelines for meme coin calls.

Parses $tickers, pump.fun links, and Solana mints; enriches with Pump.fun +
DexScreener + RugCheck; sends Telegram alerts and tracks caller hit rates.
"""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import aiohttp

from config import (
    AUTO_BUY,
    COPY_BUY_SLIPPAGE_BPS,
    DEXSCREENER_TOKEN_URL,
    DEXSCREENER_SEARCH_URL,
    SELL_SLIPPAGE_BPS,
    SOL_MINT,
    TWITTER_AUTO_BUY,
    TWITTER_BEARER_TOKEN,
    TWITTER_CALLERS,
    TWITTER_KEYWORD_SEARCH,
    TWITTER_POLL_SECONDS,
    TWITTER_STATS_DAYS,
    TWITTER_STATS_INTERVAL_HOURS,
    TWITTER_TRACKER_ENABLED,
    TWITTER_USE_COUNCIL,
    TWITTER_USER_LOOKUP_URL,
    TWITTER_USER_TWEETS_URL,
)
from modules.council_gate import council_gate
from modules.rugcheck_client import fetch_rug_report
from modules.utils import sol_to_lamports
from modules import pumpfun
from modules.twitter_calls import CallLedger, CallRecord
from modules.utils import fetch_json

if TYPE_CHECKING:
    from modules.alerter import Alerter
    from modules.executor import Executor

logger = logging.getLogger("solana-bot.twitter_tracker")

# Solana base58 mint (incl. pump.fun ...pump suffix)
MINT_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")
PUMP_URL_RE = re.compile(
    r"pump\.fun/(?:coin/)?([1-9A-HJ-NP-Za-km-z]{32,44})",
    re.IGNORECASE,
)
DEX_URL_RE = re.compile(
    r"dexscreener\.com/solana/([1-9A-HJ-NP-Za-km-z]{32,44})",
    re.IGNORECASE,
)
CASHTAG_RE = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,14})\b")
CALL_KEYWORDS = (
    "call", "launch", "ape", "entry", "buy", "ca:", "contract",
    "pump", "moon", "alpha", "stealth", "fair launch", "dev",
)

SEARCH_QUERIES = (
    'pump.fun lang:en -is:retweet',
    '(memecoin OR "meme coin") (sol OR solana) lang:en -is:retweet',
)


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}


def _looks_like_call(text: str) -> bool:
    lower = text.lower()
    if PUMP_URL_RE.search(text) or DEX_URL_RE.search(text):
        return True
    if CASHTAG_RE.search(text) and any(k in lower for k in CALL_KEYWORDS):
        return True
    if MINT_RE.search(text) and any(k in lower for k in CALL_KEYWORDS):
        return True
    return False


def parse_tweet(text: str) -> tuple[list[str], list[str]]:
    """Return (mints, cashtags) extracted from tweet body."""
    mints: list[str] = []
    for pat in (PUMP_URL_RE, DEX_URL_RE, MINT_RE):
        for m in pat.findall(text):
            if m not in mints and len(m) >= 32:
                mints.append(m)

    symbols: list[str] = []
    for sym in CASHTAG_RE.findall(text):
        if sym.upper() not in {"SOL", "BTC", "ETH", "USDC", "USD"}:
            if sym not in symbols:
                symbols.append(sym)
    return mints, symbols


class TwitterTracker:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        alerter: "Alerter",
        executor: "Executor | None" = None,
    ) -> None:
        self.session = session
        self.alerter = alerter
        self.executor = executor
        self.ledger = CallLedger()
        self._running = False
        self._user_ids: dict[str, str] = {}
        self._seen_search_ids: set[str] = set()
        self._last_stats_at: datetime | None = None

    @property
    def enabled(self) -> bool:
        return (
            TWITTER_TRACKER_ENABLED
            and bool(TWITTER_BEARER_TOKEN)
            and not TWITTER_BEARER_TOKEN.startswith("your_")
        )

    async def _resolve_user_id(self, username: str) -> str | None:
        username = username.lstrip("@").lower()
        if username in self._user_ids:
            return self._user_ids[username]
        try:
            url = TWITTER_USER_LOOKUP_URL.format(username=username)
            data = await fetch_json(
                self.session, "GET", url,
                headers=_auth_headers(),
                params={"user.fields": "public_metrics,description"},
                label=f"Twitter user @{username}",
            )
            uid = data.get("data", {}).get("id")
            if uid:
                self._user_ids[username] = uid
            return uid
        except Exception as exc:
            logger.warning("Twitter user lookup @%s failed: %s", username, exc)
            return None

    async def _fetch_user_tweets(self, username: str) -> list[dict[str, Any]]:
        uid = await self._resolve_user_id(username)
        if not uid:
            return []
        try:
            url = TWITTER_USER_TWEETS_URL.format(user_id=uid)
            data = await fetch_json(
                self.session, "GET", url,
                headers=_auth_headers(),
                params={
                    "max_results": "10",
                    "tweet.fields": "created_at,author_id,entities,public_metrics",
                    "exclude": "retweets,replies",
                },
                label=f"Twitter timeline @{username}",
            )
            return data.get("data", []) or []
        except Exception as exc:
            logger.warning("Twitter timeline @%s failed: %s", username, exc)
            return []

    async def _search_recent(self, query: str) -> list[dict[str, Any]]:
        if not TWITTER_KEYWORD_SEARCH:
            return []
        try:
            from config import TWITTER_SEARCH_URL
            data = await fetch_json(
                self.session, "GET", TWITTER_SEARCH_URL,
                headers=_auth_headers(),
                params={
                    "query": query,
                    "max_results": "10",
                    "tweet.fields": "created_at,author_id,public_metrics",
                    "expansions": "author_id",
                    "user.fields": "username",
                },
                label="Twitter search",
            )
            users = {
                u["id"]: u.get("username", "?")
                for u in data.get("includes", {}).get("users", [])
            }
            tweets = data.get("data", []) or []
            for t in tweets:
                t["_username"] = users.get(t.get("author_id", ""), "?")
            return tweets
        except Exception as exc:
            logger.warning("Twitter search failed: %s", exc)
            return []

    async def _dexscreener_search(self, query: str) -> dict[str, Any] | None:
        try:
            data = await fetch_json(
                self.session, "GET", DEXSCREENER_SEARCH_URL,
                params={"q": query},
                label=f"DexScreener search {query[:12]}",
            )
            pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
            if not pairs:
                return None
            return max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        except Exception:
            return None

    async def _dexscreener_pair(self, mint: str) -> dict[str, Any] | None:
        try:
            data = await fetch_json(
                self.session, "GET", DEXSCREENER_TOKEN_URL.format(mint=mint),
                label=f"DexScreener {mint[:8]}",
            )
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            return max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        except Exception:
            return None

    async def _resolve_mint(self, mints: list[str], symbols: list[str]) -> tuple[str, str, dict[str, Any]]:
        """Pick best mint + symbol with market data."""
        for mint in mints:
            pair = await self._dexscreener_pair(mint)
            sym = (pair or {}).get("baseToken", {}).get("symbol") or symbols[0] if symbols else mint[:6]
            coin = await pumpfun.fetch_coin(self.session, mint)
            if coin:
                pair = pair or pumpfun.synthetic_pair_from_coin(coin)
                sym = coin.get("symbol", sym)
            if pair:
                return mint, sym, pair

        for sym in symbols:
            pair = await self._dexscreener_search(f"${sym}")
            if not pair:
                pair = await self._dexscreener_search(sym)
            if pair:
                mint = pair.get("baseToken", {}).get("address", "")
                if mint:
                    return mint, sym, pair

        if mints:
            coin = await pumpfun.fetch_coin(self.session, mints[0])
            if coin:
                return mints[0], coin.get("symbol", symbols[0] if symbols else "?"), pumpfun.synthetic_pair_from_coin(coin)
        raise ValueError("no mint resolved")

    async def _enrich_and_alert(
        self,
        *,
        caller: str,
        tweet_id: str,
        text: str,
        mints: list[str],
        symbols: list[str],
        created_at: str | None,
    ) -> None:
        if self.ledger.seen_tweet(tweet_id):
            return

        try:
            mint, symbol, pair = await self._resolve_mint(mints, symbols)
        except ValueError:
            logger.info("Twitter skip — no token data for @%s tweet %s", caller, tweet_id[:8])
            return

        rug = await fetch_rug_report(self.session, mint)
        rug_ok, rug_score = rug.ok, rug.score
        mcap = float(pair.get("marketCap") or pair.get("fdv") or 0)
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        pump_meta = pair.get("pumpfun") or {}
        progress = float(pump_meta.get("bonding_progress", 0) or 0)
        if not progress and not pair.get("pumpfun"):
            coin = await pumpfun.fetch_coin(self.session, mint)
            if coin:
                progress = pumpfun.bonding_progress(coin)
                mcap = mcap or pumpfun.usd_market_cap(coin)

        entry_price = 0.0
        if self.executor:
            entry_price = await self.executor.get_token_price_usd(mint) or 0.0

        record = CallRecord(
            tweet_id=tweet_id,
            caller=caller,
            symbol=symbol,
            mint=mint,
            called_at=created_at or datetime.now(timezone.utc).isoformat(),
            tweet_text=text[:280],
            entry_mcap_usd=mcap,
            entry_price_usd=entry_price,
            bonding_progress=progress,
            rugcheck_ok=rug_ok,
        )
        self.ledger.add(record)

        await self.alerter.send_twitter_call_alert(
            caller=caller,
            symbol=symbol,
            mint=mint,
            mcap_usd=mcap,
            liq_usd=liq,
            bonding_progress=progress,
            rugcheck_ok=rug_ok,
            rugcheck_score=rug_score,
            tweet_text=text,
            tweet_id=tweet_id,
        )

        logger.info(
            "Twitter CALL @%s — %s (%s) mcap $%.0f curve %.0f%% rug %s",
            caller, symbol, mint[:8], mcap, progress, "ok" if rug_ok else "FAIL",
        )

        if TWITTER_AUTO_BUY and self.executor and rug_ok:
            can, skip = await self.executor.can_trade(mint)
            if not can:
                return
            sell_ok = False
            try:
                buy_sol = await self.executor.calc_buy_size_sol()
                buy_quote = await self.executor.get_quote(
                    SOL_MINT, mint, sol_to_lamports(buy_sol),
                )
                test_amount = max(int(buy_quote.get("outAmount", 0)) // 10, 1)
                sell_quote, _ = await self.executor._get_exit_quote(mint, test_amount, 1000)
                sell_ok = bool(sell_quote and int(sell_quote.get("outAmount", 0)) > 0)
            except Exception:
                sell_ok = False
            if not sell_ok:
                logger.info("Twitter auto-buy skip %s — no sell route", symbol)
                return
            rm = self.executor.risk_manager
            daily_ok = True
            if rm:
                daily_ok, _ = await rm.can_open_new_trade()
            if TWITTER_USE_COUNCIL:
                approved, council, _ = await council_gate(
                    self.session,
                    mint=mint,
                    symbol=symbol,
                    pair=pair or {},
                    source="twitter",
                    sell_route_ok=sell_ok,
                    score=75.0,
                    twitter_mentions=1,
                    rug=await fetch_rug_report(self.session, mint),
                    daily_budget_ok=daily_ok,
                    trader_name=f"@{caller}",
                )
                if not approved:
                    logger.info(
                        "Twitter auto-buy skip %s — HERMES %s",
                        symbol, council.score if council else "?",
                    )
                    return
            buy_sol = await self.executor.calc_buy_size_sol()
            await self.executor.buy_token(
                mint=mint,
                amount_sol=buy_sol,
                reason=f"Twitter call @{caller}",
                symbol=symbol,
                slippage_bps=COPY_BUY_SLIPPAGE_BPS,
            )

    async def _process_tweet(self, tweet: dict[str, Any], caller: str) -> None:
        tweet_id = tweet.get("id", "")
        text = tweet.get("text", "")
        if not tweet_id or not text or self.ledger.seen_tweet(tweet_id):
            return
        if not _looks_like_call(text):
            return
        mints, symbols = parse_tweet(text)
        if not mints and not symbols:
            return
        await self._enrich_and_alert(
            caller=caller,
            tweet_id=tweet_id,
            text=text,
            mints=mints,
            symbols=symbols,
            created_at=tweet.get("created_at"),
        )

    async def _poll_callers(self) -> None:
        if not TWITTER_CALLERS:
            return
        for raw in TWITTER_CALLERS:
            username = raw.strip().lstrip("@")
            if not username:
                continue
            tweets = await self._fetch_user_tweets(username)
            for tweet in reversed(tweets):
                await self._process_tweet(tweet, username)
            await asyncio.sleep(1.2)

    async def _poll_search(self) -> None:
        for query in SEARCH_QUERIES:
            tweets = await self._search_recent(query)
            for tweet in tweets:
                tid = tweet.get("id", "")
                if tid in self._seen_search_ids:
                    continue
                self._seen_search_ids.add(tid)
                if len(self._seen_search_ids) > 1000:
                    self._seen_search_ids = set(list(self._seen_search_ids)[-500:])
                caller = tweet.get("_username", "search")
                await self._process_tweet(tweet, caller)
            await asyncio.sleep(1.5)

    async def _update_call_prices(self) -> None:
        if not self.executor:
            return
        recent = self.ledger.recent_calls(TWITTER_STATS_DAYS)
        for call in recent[-30:]:
            if call.entry_price_usd <= 0:
                price = await self.executor.get_token_price_usd(call.mint)
                if price and price > 0:
                    call.entry_price_usd = price
                    continue
            current = await self.executor.get_token_price_usd(call.mint)
            if current and call.entry_price_usd > 0:
                mult = current / call.entry_price_usd
                self.ledger.update_price(call.mint, mult)

    async def _maybe_send_stats(self) -> None:
        now = datetime.now(timezone.utc)
        if self._last_stats_at:
            elapsed = (now - self._last_stats_at).total_seconds() / 3600
            if elapsed < TWITTER_STATS_INTERVAL_HOURS:
                return
        summary = self.ledger.summary(TWITTER_STATS_DAYS)
        if summary["calls"] < 3:
            return
        leaders = self.ledger.caller_leaderboard(TWITTER_STATS_DAYS)[:3]
        top = self.ledger.top_calls(TWITTER_STATS_DAYS, 10)
        await self.alerter.send_caller_stats(summary, leaders, top, TWITTER_STATS_DAYS)
        self._last_stats_at = now

    async def run(self) -> None:
        if not self.enabled:
            logger.info(
                "Twitter tracker OFF — set TWITTER_BEARER_TOKEN + ENABLE_TWITTER_TRACKER=true"
            )
            return

        self._running = True
        mode = []
        if TWITTER_CALLERS:
            mode.append(f"{len(TWITTER_CALLERS)} account(s)")
        if TWITTER_KEYWORD_SEARCH:
            mode.append("keyword search")
        logger.info(
            "Twitter tracker started — %s, poll every %ds",
            " + ".join(mode) or "no sources (add TWITTER_CALLERS on Railway)",
            TWITTER_POLL_SECONDS,
        )

        while self._running:
            try:
                await self._poll_callers()
                await self._poll_search()
                await self._update_call_prices()
                await self._maybe_send_stats()
            except Exception as exc:
                logger.error("Twitter tracker cycle error: %s", exc)
            await asyncio.sleep(TWITTER_POLL_SECONDS)

    def stop(self) -> None:
        self._running = False
