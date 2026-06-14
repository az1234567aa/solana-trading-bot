from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import aiohttp

from config import (
    ALERTS_ONLY,
    AUTO_BUY,
    BOT_PAUSED,
    DAILY_LOSS_LIMIT_USD,
    DAILY_PROFIT_TARGET_USD,
    COPY_COUNCIL_MIN,
    MEME_COUNCIL_MIN,
    MAX_OPEN_POSITIONS,
    PAPER_TRADE,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_SEND_URL,
    USE_MEME_COUNCIL,
    SCAN_PUMPFUN_ENABLED,
    TWITTER_CALLERS,
    TWITTER_TRACKER_ENABLED,
)
from modules.onchain import is_on_chain_tx, solscan_account_link, solscan_tx_link
from modules.utils import format_duration, format_usd

logger = logging.getLogger("solana-bot.alerter")


@dataclass
class TradeAlert:
    token_mint: str
    token_symbol: str
    reason: str
    entry_price: float
    exit_price: float
    exit_reason: str
    entry_time: datetime
    exit_time: datetime
    pnl_sol: float
    pnl_usd: float
    spent_usd: float
    received_usd: float
    peak_multiplier: float
    daily_pnl_usd: float = 0.0
    lifetime_pnl_usd: float = 0.0
    buys_today: int = 0
    score_breakdown: dict[str, Any] | None = None
    sol_price_usd: float = 0.0
    entry_tx: str = ""
    exit_tx: str = ""


class Alerter:
    def __init__(self, session: aiohttp.ClientSession) -> None:
        self.session = session
        self.enabled = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)

    async def send_message(self, text: str) -> None:
        if not self.enabled:
            logger.info("[ALERT] Telegram disabled — %s", text[:200])
            return

        url = TELEGRAM_SEND_URL.format(token=TELEGRAM_BOT_TOKEN)
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("Telegram send failed (%s): %s", resp.status, body)
        except Exception as exc:
            logger.error("Telegram send error: %s", exc)

    async def send_buy_alert(
        self,
        symbol: str,
        mint: str,
        amount_sol: float,
        cost_usd: float,
        reason: str,
        tx_sig: str | None,
        buys_today: int = 0,
        score_breakdown: dict[str, Any] | None = None,
    ) -> None:
        mode = "📝 PAPER — " if PAPER_TRADE else ""
        lines = [
            f"<b>{mode}🟢 BOUGHT — {symbol}</b>",
            f"<b>Cost:</b> {amount_sol:.4f} SOL ({format_usd(cost_usd)})",
            f"<b>Why:</b> {reason}",
            f"<b>Buy #</b>{buys_today} today",
        ]
        if score_breakdown:
            for key, value in score_breakdown.items():
                lines.append(f"<b>{key.replace('_', ' ').title()}:</b> {value}")
        lines.append(f"<b>Mint:</b> <code>{mint}</code>")
        if tx_sig and is_on_chain_tx(tx_sig):
            lines.append(f'<a href="{solscan_tx_link(tx_sig)}">✅ Verify buy on Solscan</a>')
        elif tx_sig:
            lines.append(f"<b>Tx:</b> <code>{tx_sig}</code> (simulated)")
        await self.send_message("\n".join(lines))

    async def send_partial_sell_alert(
        self,
        symbol: str,
        sell_pct: float,
        received_usd: float,
        exit_reason: str,
        remaining_pct: float,
    ) -> None:
        mode = "📝 PAPER — " if PAPER_TRADE else ""
        await self.send_message(
            f"<b>{mode}📤 PARTIAL SELL — {symbol}</b>\n"
            f"<b>Sold:</b> {sell_pct:.0f}% ({exit_reason})\n"
            f"<b>USDC received:</b> {format_usd(received_usd)}\n"
            f"<b>Still holding:</b> {remaining_pct:.0f}%"
        )

    async def send_trade_alert(self, alert: TradeAlert) -> None:
        hold_seconds = (alert.exit_time - alert.entry_time).total_seconds()
        pnl_sign = "+" if alert.pnl_usd >= 0 else ""
        emoji = "✅" if alert.pnl_usd >= 0 else "❌"
        pct = (alert.pnl_usd / alert.spent_usd * 100.0) if alert.spent_usd > 0 else 0.0
        day_sign = "+" if alert.daily_pnl_usd >= 0 else ""
        life_sign = "+" if alert.lifetime_pnl_usd >= 0 else ""

        mode = "📝 PAPER — " if PAPER_TRADE else ""
        lines = [
            f"<b>{mode}{emoji} CLOSED → USDC — {alert.token_symbol}</b>",
            "",
            f"<b>This trade</b>",
            f"  Spent: {format_usd(alert.spent_usd)}",
            f"  Got back: {format_usd(alert.received_usd)} USDC",
            f"  PnL: {pnl_sign}{format_usd(alert.pnl_usd)} ({pnl_sign}{pct:.1f}%)",
            "",
            f"<b>Running totals</b>",
            f"  Today: {day_sign}{format_usd(alert.daily_pnl_usd)}",
            f"  All-time (tracked): {life_sign}{format_usd(alert.lifetime_pnl_usd)}",
            "",
            f"<b>Why bought:</b> {alert.reason}",
            f"<b>Exit:</b> {alert.exit_reason}",
            f"<b>Hold:</b> {format_duration(hold_seconds)} | <b>Peak:</b> {alert.peak_multiplier:.2f}x",
            f"<b>Mint:</b> <code>{alert.token_mint}</code>",
        ]
        if is_on_chain_tx(alert.entry_tx):
            lines.append(f'<a href="{solscan_tx_link(alert.entry_tx)}">Buy tx on Solscan</a>')
        if is_on_chain_tx(alert.exit_tx):
            lines.append(f'<a href="{solscan_tx_link(alert.exit_tx)}">Sell tx on Solscan</a>')
        await self.send_message("\n".join(lines))

    async def send_startup_message(
        self,
        lifetime_pnl: float = 0.0,
        lifetime_trades: int = 0,
        *,
        open_positions: list[tuple[str, str]] | None = None,
        persistence: str = "file",
        wallet_address: str = "",
    ) -> None:
        lines = [
            "<b>Solana Bot started</b>",
        ]
        if BOT_PAUSED:
            lines.append("• Mode: <b>⏸ PAUSED</b> — scanning only, no buys")
        if not PAPER_TRADE and AUTO_BUY and not ALERTS_ONLY and not BOT_PAUSED:
            lines.append("• Mode: <b>🔴 LIVE AUTO-BUY</b>")
        if ALERTS_ONLY or not AUTO_BUY:
            lines.append("• Mode: <b>🔔 ALERTS ONLY</b> — HERMES signals, no auto-buy")
        if PAPER_TRADE:
            lines.append("• Mode: <b>📝 PAPER TRADE</b> (simulated — no real txs)")
            lines.append("• Paper PnL = tracked price only (not Jupiter quotes)")
        lines.extend([
            f"• Stops new buys at <b>-${DAILY_LOSS_LIMIT_USD:.0f}/day</b> loss"
            f" | locks profit at <b>+${DAILY_PROFIT_TARGET_USD:.0f}/day</b>",
            f"• Max <b>{MAX_OPEN_POSITIONS} open positions</b> at once",
            f"• Memory: <b>{persistence}</b> — open coins + cooldowns survive restarts",
            f"• Pump.fun scanner: <b>{'ON' if SCAN_PUMPFUN_ENABLED else 'OFF'}</b> (live + graduating + graduated)",
            f"• Twitter caller tracker: <b>{'ON' if TWITTER_TRACKER_ENABLED else 'OFF'}</b>"
            + (
                f" ({len(TWITTER_CALLERS)} accounts + keyword search)"
                if TWITTER_CALLERS
                else " (keyword search — add TWITTER_CALLERS to watch specific accounts)"
                if TWITTER_TRACKER_ENABLED
                else ""
            ),
            f"• HERMES Council: <b>{'ON' if USE_MEME_COUNCIL else 'OFF'}</b>"
            + (f" ({MEME_COUNCIL_MIN}/7 HERMES · copy {COPY_COUNCIL_MIN}/7)" if USE_MEME_COUNCIL else ""),
            "• Every buy + sell → Telegram + trade log",
            "• Sells → <b>USDC</b> with running PnL totals",
            "• Live trades include <b>Solscan links</b> — verify every tx on-chain",
        ])
        if wallet_address and wallet_address != "PAPER_WALLET":
            lines.append(
                f'• Wallet: <a href="{solscan_account_link(wallet_address)}">{wallet_address[:8]}…</a>'
            )
        if open_positions:
            names = ", ".join(sym for sym, _ in open_positions)
            lines.append(
                f"• <b>Resumed monitoring {len(open_positions)} open position(s):</b> {names}"
            )
        if lifetime_trades > 0:
            sign = "+" if lifetime_pnl >= 0 else ""
            lines.append(
                f"• Tracked history: {lifetime_trades} trades, "
                f"{sign}{format_usd(lifetime_pnl)} all-time"
            )
        await self.send_message("\n".join(lines))

    async def send_hermes_signal(
        self,
        *,
        symbol: str,
        mint: str,
        source: str,
        score: float,
        council_result,
        reason: str,
        mcap_usd: float = 0,
        liq_usd: float = 0,
    ) -> None:
        """HERMES council passed — alert user without buying (alerts-only mode)."""
        lines = [
            f"<b>🛡️ HERMES SIGNAL — {symbol}</b>",
            f"<b>Source:</b> {source}",
            f"<b>Score:</b> {score:.0f}/100",
            f"<b>Why:</b> {reason}",
        ]
        if mcap_usd:
            lines.append(f"<b>Mcap:</b> ${mcap_usd:,.0f}")
        if liq_usd:
            lines.append(f"<b>Liq:</b> ${liq_usd:,.0f}")
        if council_result:
            lines.extend(council_result.summary_lines())
        lines.append(f"<b>Mint:</b> <code>{mint}</code>")
        lines.append("<i>Alerts-only — buy manually in Phantom if you like the setup.</i>")
        await self.send_message("\n".join(lines))

    async def send_twitter_call_alert(
        self,
        *,
        caller: str,
        symbol: str,
        mint: str,
        mcap_usd: float,
        liq_usd: float,
        bonding_progress: float,
        rugcheck_ok: bool,
        rugcheck_score: float,
        tweet_text: str,
        tweet_id: str,
    ) -> None:
        rug_icon = "✅" if rugcheck_ok else "❌"
        curve = (
            f"Graduated ✅"
            if bonding_progress >= 100
            else f"Bonding curve {bonding_progress:.0f}%"
        )
        snippet = tweet_text.replace("\n", " ")[:160]
        lines = [
            f"<b>🐦 NEW CALL — @{caller}</b>",
            f"<b>${symbol}</b> | mcap {format_usd(mcap_usd)} | liq {format_usd(liq_usd)}",
            f"{curve} | RugCheck {rug_icon} (score {rugcheck_score:.0f})",
            "",
            f"<i>{snippet}</i>",
            "",
            f"<b>Mint:</b> <code>{mint}</code>",
            f"<a href=\"https://pump.fun/coin/{mint}\">Pump.fun</a> · "
            f"<a href=\"https://dexscreener.com/solana/{mint}\">DexScreener</a> · "
            f"<a href=\"https://twitter.com/i/web/status/{tweet_id}\">Tweet</a>",
        ]
        await self.send_message("\n".join(lines))

    async def send_caller_stats(
        self,
        summary: dict,
        leaders: list,
        top_calls: list,
        days: int,
    ) -> None:
        lines = [
            f"<b>📊 Caller stats — last {days} days</b>",
            f"Calls: <b>{summary['calls']}</b> | "
            f"Hit rate: <b>{summary['hit_rate_pct']:.0f}%</b> (2x+) | "
            f"Median: <b>{summary['median_return_x']:.2f}x</b> | "
            f"Avg: <b>{summary['avg_return_x']:.2f}x</b>",
            "",
            "<b>Top callers</b>",
        ]
        for i, s in enumerate(leaders[:3], 1):
            lines.append(
                f"{i}. @{s.caller} — {s.calls} calls | "
                f"{s.hit_rate_pct:.0f}% hit | med {s.median_return_x:.2f}x | "
                f"best ${s.best_symbol} {s.best_return_x:.2f}x"
            )
        if top_calls:
            lines.append("")
            lines.append("<b>Top 10 calls</b>")
            for i, c in enumerate(top_calls[:10], 1):
                lines.append(
                    f"{i}. <b>${c.symbol}</b> @{c.caller} — "
                    f"<b>{c.peak_multiplier:.2f}x</b> peak"
                )
        await self.send_message("\n".join(lines))

    async def send_error(self, context: str, error: str) -> None:
        await self.send_message(f"<b>Error</b> in {context}:\n<code>{error}</code>")
