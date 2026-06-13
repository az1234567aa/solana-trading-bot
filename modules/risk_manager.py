from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from config import (
    COPY_REBUY_COOLDOWN_HOURS,
    DAILY_LOSS_LIMIT_USD,
    DAILY_PROFIT_TARGET_USD,
    DUST_BALANCE_USD,
    MAX_HOLD_MINUTES,
    MAX_OPEN_POSITIONS,
    PAPER_TRADE,
    RISK_POLL_INTERVAL_SECONDS,
    SELL_TO_STABLE,
    STOP_LOSS_PCT,
    TIME_STOP_MINUTES,
    TIME_STOP_MIN_MULTIPLIER,
    TP1_MULTIPLIER,
    TP1_SELL_PCT,
    TP2_MULTIPLIER,
    TP2_SELL_PCT,
    TP3_MULTIPLIER,
    TP3_SELL_PCT,
    TRAILING_ACTIVATION_MULTIPLIER,
    TRAILING_STOP_PCT,
)
from modules.alerter import Alerter, TradeAlert
from modules.bot_state import BotStateStore
from modules.onchain import is_on_chain_tx, solscan_account_link
from modules.trade_journal import TradeStats, log_event
from modules.utils import format_duration

if TYPE_CHECKING:
    from modules.executor import BuyResult, Executor

logger = logging.getLogger("solana-bot.risk_manager")

DATABASE_URL   = os.getenv("DATABASE_URL", "")
POSITIONS_FILE = "positions.json"   # fallback when no DB


# ── Helpers ──────────────────────────────────────────────────────────────────

def _pos_to_dict(p: "Position") -> dict:
    return {
        "position_id": p.position_id,
        "mint": p.mint,
        "symbol": p.symbol,
        "entry_price_usd": p.entry_price_usd,
        "entry_time": p.entry_time.isoformat(),
        "initial_tokens": p.initial_tokens,
        "remaining_tokens": p.remaining_tokens,
        "initial_sol": p.initial_sol,
        "entry_cost_usd": p.entry_cost_usd,
        "reason": p.reason,
        "score_breakdown": p.score_breakdown,
        "decimals": p.decimals,
        "peak_multiplier": p.peak_multiplier,
        "last_multiplier": p.last_multiplier,
        "trailing_active": p.trailing_active,
        "trailing_peak_multiplier": p.trailing_peak_multiplier,
        "tp1_hit": p.tp1_hit,
        "tp2_hit": p.tp2_hit,
        "tp3_hit": p.tp3_hit,
        "total_sol_received": p.total_sol_received,
        "price_miss_count": p.price_miss_count,
        "sell_fail_count": p.sell_fail_count,
        "entry_tx_signature": p.entry_tx_signature,
        "partial_exits": [
            {k: str(v) if isinstance(v, datetime) else v for k, v in e.items()}
            for e in p.partial_exits
        ],
    }


def _dict_to_pos(d: dict) -> "Position":
    return Position(
        position_id=d["position_id"],
        mint=d["mint"],
        symbol=d["symbol"],
        entry_price_usd=d["entry_price_usd"],
        entry_time=datetime.fromisoformat(d["entry_time"]),
        initial_tokens=d["initial_tokens"],
        remaining_tokens=d["remaining_tokens"],
        initial_sol=d["initial_sol"],
        entry_cost_usd=d.get("entry_cost_usd", 0.0),
        reason=d["reason"],
        score_breakdown=d.get("score_breakdown"),
        decimals=d.get("decimals", 6),
        peak_multiplier=d.get("peak_multiplier", 1.0),
        last_multiplier=d.get("last_multiplier", 1.0),
        trailing_active=d.get("trailing_active", False),
        trailing_peak_multiplier=d.get("trailing_peak_multiplier", 1.0),
        tp1_hit=d.get("tp1_hit", False),
        tp2_hit=d.get("tp2_hit", False),
        tp3_hit=d.get("tp3_hit", False),
        total_sol_received=d.get("total_sol_received", 0.0),
        partial_exits=d.get("partial_exits", []),
        price_miss_count=d.get("price_miss_count", 0),
        sell_fail_count=d.get("sell_fail_count", 0),
        entry_tx_signature=d.get("entry_tx_signature", ""),
    )


# ── File-based fallback ───────────────────────────────────────────────────────

def _file_save(positions: dict) -> None:
    try:
        data = {pid: _pos_to_dict(p) for pid, p in positions.items() if not p.closed}
        with open(POSITIONS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as exc:
        logger.warning("File save failed: %s", exc)


def _file_load() -> dict:
    if not os.path.exists(POSITIONS_FILE):
        return {}
    try:
        with open(POSITIONS_FILE) as f:
            data = json.load(f)
        positions = {pid: _dict_to_pos(d) for pid, d in data.items()}
        logger.info("Loaded %d position(s) from file", len(positions))
        return positions
    except Exception as exc:
        logger.warning("File load failed: %s", exc)
        return {}


# ── PostgreSQL store ──────────────────────────────────────────────────────────

class PositionStore:
    """
    Persists positions in PostgreSQL when DATABASE_URL is set,
    otherwise falls back to a local JSON file.
    Positions survive Railway restarts and redeploys either way.
    """

    def __init__(self) -> None:
        self._pool = None

    async def initialize(self) -> None:
        if not DATABASE_URL:
            logger.warning("DATABASE_URL not set — using file fallback (positions lost on redeploy)")
            return
        try:
            import asyncpg
            self._pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS positions (
                        position_id TEXT PRIMARY KEY,
                        data        JSONB    NOT NULL,
                        closed      BOOLEAN  NOT NULL DEFAULT FALSE,
                        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
            logger.info("PostgreSQL position store ready")
            await self._migrate_file_to_db()
        except Exception as exc:
            logger.error("PostgreSQL init failed, using file fallback: %s", exc)
            self._pool = None

    async def _migrate_file_to_db(self) -> None:
        """One-time: copy positions.json into Postgres so redeploys keep open trades."""
        if self._pool is None:
            return
        file_positions = _file_load()
        if not file_positions:
            return
        try:
            async with self._pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM positions WHERE closed = FALSE"
                )
            if count and int(count) > 0:
                return
            for position in file_positions.values():
                if not position.closed:
                    await self.save(position)
            logger.info(
                "Migrated %d open position(s) from positions.json → PostgreSQL",
                len(file_positions),
            )
        except Exception as exc:
            logger.warning("File → PostgreSQL migration failed: %s", exc)

    async def save(self, position: "Position") -> None:
        if self._pool is None:
            return  # file save handled by caller
        try:
            data = json.dumps(_pos_to_dict(position))
            async with self._pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO positions (position_id, data, closed, updated_at)
                    VALUES ($1, $2::jsonb, $3, NOW())
                    ON CONFLICT (position_id)
                    DO UPDATE SET data=EXCLUDED.data, closed=EXCLUDED.closed, updated_at=NOW()
                """, position.position_id, data, position.closed)
        except Exception as exc:
            logger.warning("DB save failed: %s", exc)

    async def load_all(self) -> dict:
        if self._pool is None:
            return _file_load()
        try:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT data FROM positions WHERE closed = FALSE"
                )
            positions = {}
            for row in rows:
                d = json.loads(row["data"])
                p = _dict_to_pos(d)
                positions[p.position_id] = p
            logger.info("Loaded %d open position(s) from PostgreSQL", len(positions))
            return positions
        except Exception as exc:
            logger.warning("DB load failed, trying file: %s", exc)
            return _file_load()


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class Position:
    position_id: str
    mint: str
    symbol: str
    entry_price_usd: float
    entry_time: datetime
    initial_tokens: float
    remaining_tokens: float
    initial_sol: float
    reason: str
    entry_cost_usd: float = 0.0   # SOL cost at buy time (for accurate PnL)
    score_breakdown: dict[str, Any] | None = None
    decimals: int = 6
    peak_multiplier: float = 1.0
    last_multiplier: float = 1.0   # last polled value — used for paper PnL
    trailing_active: bool = False
    trailing_peak_multiplier: float = 1.0
    tp1_hit: bool = False
    tp2_hit: bool = False
    tp3_hit: bool = False
    closed: bool = False
    total_sol_received: float = 0.0
    partial_exits: list[dict[str, Any]] = field(default_factory=list)
    price_miss_count: int = 0
    sell_fail_count: int = 0
    entry_tx_signature: str = ""   # required for live — proves on-chain buy


# ── Risk Manager ─────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self, executor: "Executor", alerter: Alerter) -> None:
        self.executor = executor
        self.alerter  = alerter
        self.store    = PositionStore()
        self.state_store = BotStateStore()
        self.positions: dict[str, Position] = {}
        self._running = False
        self._loss_cooldown: dict[str, datetime] = {}
        self._traded_mints: list[dict[str, Any]] = []
        self._trading_date: str = ""
        self._buys_today: int = 0
        self._daily_pnl_usd: float = 0.0
        self._halted_today: bool = False
        self._halt_reason: str = ""
        self.stats = TradeStats.load()

    def _reset_daily_if_needed(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self._trading_date != today:
            self._trading_date = today
            self._buys_today = 0
            self._daily_pnl_usd = 0.0
            self._halted_today = False
            self._halt_reason = ""

    def open_position_count(self) -> int:
        return sum(1 for p in self.positions.values() if not p.closed)

    async def can_open_new_trade(self) -> tuple[bool, str]:
        self._reset_daily_if_needed()
        if self._halted_today:
            return False, self._halt_reason or "trading paused for today"
        if self.open_position_count() >= MAX_OPEN_POSITIONS:
            return False, f"max {MAX_OPEN_POSITIONS} open positions"
        return True, ""

    def is_holding(self, mint: str) -> bool:
        return any(not p.closed and p.mint == mint for p in self.positions.values())

    def on_cooldown(self, mint: str) -> bool:
        until = self._loss_cooldown.get(mint)
        if not until:
            return False
        if datetime.now(timezone.utc) >= until:
            del self._loss_cooldown[mint]
            self._schedule_state_save()
            return False
        return True

    def prior_loss_count(self, mint: str) -> int:
        return sum(
            1 for t in self._traded_mints
            if t.get("mint") == mint and float(t.get("pnl_usd", 0)) < 0
        )

    async def _schedule_state_save(self) -> None:
        await self._save_bot_state()

    async def _save_bot_state(self) -> None:
        await self.state_store.save({
            "loss_cooldowns": BotStateStore.serialize_cooldowns(self._loss_cooldown),
            "traded_mints": self._traded_mints,
            "trade_stats": {
                "lifetime_pnl_usd": self.stats.lifetime_pnl_usd,
                "lifetime_trades": self.stats.lifetime_trades,
                "wins": self.stats.wins,
                "losses": self.stats.losses,
                "total_spent_usd": self.stats.total_spent_usd,
                "total_received_usd": self.stats.total_received_usd,
                "last_updated": self.stats.last_updated,
            },
            "daily": {
                "date": self._trading_date,
                "buys_today": self._buys_today,
                "daily_pnl_usd": self._daily_pnl_usd,
                "halted_today": self._halted_today,
                "halt_reason": self._halt_reason,
            },
        })

    async def _persist_stats(self) -> None:
        self.stats.save()
        await self._save_bot_state()

    async def initialize(self) -> None:
        await self.store.initialize()
        await self.state_store.initialize()
        self.positions = await self.store.load_all()

        raw = await self.state_store.load()
        self._loss_cooldown = BotStateStore.parse_cooldowns(raw.get("loss_cooldowns", {}))
        self._traded_mints = list(raw.get("traded_mints", []))

        ts = raw.get("trade_stats") or {}
        if ts:
            fields = TradeStats.__dataclass_fields__
            self.stats = TradeStats(**{k: ts[k] for k in fields if k in ts})
            logger.info(
                "Restored trade stats — %d trades, PnL $%.2f",
                self.stats.lifetime_trades,
                self.stats.lifetime_pnl_usd,
            )

        daily = raw.get("daily", {})
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if daily.get("date") == today:
            self._trading_date = today
            self._buys_today = int(daily.get("buys_today", 0))
            self._daily_pnl_usd = float(daily.get("daily_pnl_usd", 0))
            self._halted_today = bool(daily.get("halted_today", False))
            self._halt_reason = str(daily.get("halt_reason", ""))
            logger.info(
                "Restored daily state — buys %d today | PnL $%.2f | halted=%s",
                self._buys_today, self._daily_pnl_usd, self._halted_today,
            )
        else:
            self._reset_daily_if_needed()

        purged = await self._reconcile_loaded_positions()
        if purged:
            logger.info("Purged %d paper/ghost position(s) on live startup", purged)

        if self._loss_cooldown:
            logger.info("Restored %d loss cooldown(s)", len(self._loss_cooldown))
        if self.positions:
            symbols = ", ".join(p.symbol for p in self.positions.values() if not p.closed)
            logger.info("Still monitoring open position(s): %s", symbols)

    async def _reset_paper_contaminated_stats(self) -> None:
        """Live mode — drop PnL from paper trades that never hit the chain."""
        self.stats = TradeStats()
        self._daily_pnl_usd = 0.0
        self._buys_today = 0
        self._halted_today = False
        self._halt_reason = ""
        self._traded_mints = []
        self._loss_cooldown = {}
        self._reset_daily_if_needed()
        await self._persist_stats()

    async def _purge_ghost_position(self, position: Position, reason: str) -> None:
        """Remove a DB position that was never a real on-chain hold."""
        position.closed = True
        position.remaining_tokens = 0
        await self._persist(position)
        self.positions.pop(position.position_id, None)
        logger.warning(
            "Purged ghost position %s (%s) — %s",
            position.symbol, position.mint[:8], reason,
        )

    async def _reconcile_loaded_positions(self) -> int:
        """Live startup — drop paper positions; sync real ones with wallet."""
        if PAPER_TRADE:
            return 0

        purged: list[Position] = []
        for pos in list(self.positions.values()):
            if pos.closed:
                continue
            if not is_on_chain_tx(pos.entry_tx_signature):
                purged.append(pos)
                continue
            amount, decimals = await self.executor.get_token_balance(pos.mint)
            if amount > 0:
                pos.remaining_tokens = amount
                pos.decimals = decimals
                await self._persist(pos)
            elif pos.total_sol_received <= 0 and not pos.partial_exits:
                purged.append(pos)

        for pos in purged:
            await self._purge_ghost_position(pos, "no on-chain buy or empty wallet")

        if purged:
            await self._reset_paper_contaminated_stats()
            symbols = ", ".join(p.symbol for p in purged)
            await self.alerter.send_message(
                "<b>🧹 Cleared paper/fake positions</b>\n"
                f"Removed: {symbols}\n"
                "PnL counters reset — live mode only tracks verified on-chain trades.\n"
                f'<a href="{solscan_account_link(self.executor.public_key)}">View wallet on Solscan</a>'
            )
        return len(purged)

    async def _persist(self, position: Position) -> None:
        if self.store._pool is not None:
            await self.store.save(position)
        else:
            _file_save(self.positions)

    async def open_position(self, buy: "BuyResult") -> None:
        if not buy.success or buy.tokens_received <= 0:
            return

        if not PAPER_TRADE and not is_on_chain_tx(buy.tx_signature):
            logger.error(
                "Refusing to track %s — live buy has no on-chain tx (%s)",
                buy.symbol, buy.tx_signature,
            )
            return

        sol_price = await self.executor.get_sol_price_usd()
        entry_cost_usd = buy.amount_sol * sol_price

        position = Position(
            position_id=buy.position_id,
            mint=buy.mint,
            symbol=buy.symbol,
            entry_price_usd=buy.entry_price_usd,
            entry_time=datetime.now(timezone.utc),
            initial_tokens=buy.tokens_received,
            remaining_tokens=buy.tokens_received,
            initial_sol=buy.amount_sol,
            entry_cost_usd=entry_cost_usd,
            reason=buy.reason,
            score_breakdown=buy.score_breakdown,
            decimals=buy.decimals,
            entry_tx_signature=buy.tx_signature or "",
        )
        self.positions[buy.position_id] = position
        self._reset_daily_if_needed()
        self._buys_today += 1
        await self._persist(position)
        await self._save_bot_state()
        log_event(
            "BUY", symbol=buy.symbol, mint=buy.mint,
            cost_usd=round(entry_cost_usd, 2), sol=buy.amount_sol,
            reason=buy.reason, tx=buy.tx_signature,
        )
        logger.info(
            "Position opened — %s | %.4f tokens @ $%.8f | buy #%d today | cost $%.2f",
            buy.symbol, buy.tokens_received, buy.entry_price_usd,
            self._buys_today, entry_cost_usd,
        )

    def _current_multiplier(self, position: Position, current_price: float) -> float:
        if position.entry_price_usd <= 0:
            return 1.0
        return current_price / position.entry_price_usd

    def _cap_multiplier(self, mult: float) -> float:
        """Paper/live sanity — meme price feeds can report nonsense multipliers."""
        return max(0.0, min(mult, TP3_MULTIPLIER))

    def _paper_exit_multiplier(self, position: Position, exit_reason: str) -> float:
        """Realistic paper exit value from what monitoring tracked."""
        peak = self._cap_multiplier(position.peak_multiplier)
        last = self._cap_multiplier(
            position.last_multiplier if position.last_multiplier > 0 else 1.0,
        )
        mult = min(last, peak)

        if exit_reason == "SL":
            return min(mult, 1.0 + STOP_LOSS_PCT / 100.0)
        if exit_reason == "time_stop":
            return min(mult, TIME_STOP_MIN_MULTIPLIER)
        if exit_reason == "no_price_data":
            return 0.0
        return mult

    async def _paper_received_usd(
        self,
        position: Position,
        quote_received: float,
        tokens_to_sell: float,
        exit_reason: str = "",
    ) -> float:
        """Paper partial sell — never trust Jupiter sell quotes."""
        if position.initial_tokens <= 0:
            return 0.0
        fraction = min(1.0, tokens_to_sell / position.initial_tokens)
        spent = position.entry_cost_usd * fraction
        if spent <= 0:
            return 0.0

        exit_mult = self._paper_exit_multiplier(position, exit_reason)
        sanitized = round(spent * exit_mult, 4)

        if quote_received > sanitized + 0.10 and quote_received > spent * 1.5:
            logger.warning(
                "[PAPER] Ignoring bogus sell quote $%.2f → $%.2f for %s "
                "(tracked %.2fx, peak %.2fx, exit %s)",
                quote_received, sanitized, position.symbol,
                position.last_multiplier, position.peak_multiplier, exit_reason,
            )
        return sanitized

    async def _sync_wallet_balance(self, position: Position) -> None:
        """Keep tracked balance in sync with what's actually in the wallet."""
        amount, decimals = await self.executor.get_token_balance(position.mint)
        if amount > 0:
            position.remaining_tokens = amount
            position.decimals = decimals
            return
        if self.executor.paper_trade:
            return
        # Live: never zero remaining_tokens just because wallet reads empty —
        # that caused fake -100% closes on paper positions after redeploy.
        has_proceeds = position.total_sol_received > 0 or bool(position.partial_exits)
        if has_proceeds:
            position.remaining_tokens = 0

    async def _sell_partial(self, position: Position, sell_pct: float, exit_reason: str) -> bool:
        await self._sync_wallet_balance(position)
        if position.remaining_tokens <= 0:
            return True

        tokens_to_sell = position.remaining_tokens * (sell_pct / 100.0)
        if tokens_to_sell <= 0:
            return True

        sell_result = await self.executor.sell_token(
            mint=position.mint,
            amount_tokens=tokens_to_sell,
            decimals=position.decimals,
            symbol=position.symbol,
            sell_pct=sell_pct,
        )
        if sell_result.success:
            if sell_result.is_dust and not self.executor.paper_trade:
                logger.warning(
                    "Live sell rejected for %s — dust skip not allowed without on-chain tx",
                    position.symbol,
                )
                position.sell_fail_count += 1
                await self._persist(position)
                return False

            position.sell_fail_count = 0
            received = sell_result.sol_received
            if sell_result.is_dust:
                position.remaining_tokens = 0
            else:
                if self.executor.paper_trade:
                    quote_usd = received if SELL_TO_STABLE else (
                        received * await self.executor.get_sol_price_usd()
                    )
                    received_usd = await self._paper_received_usd(
                        position, quote_usd, tokens_to_sell, exit_reason,
                    )
                    received = received_usd if SELL_TO_STABLE else (
                        received_usd / await self.executor.get_sol_price_usd()
                    )
                    position.remaining_tokens = max(
                        0.0, position.remaining_tokens - tokens_to_sell,
                    )
                else:
                    position.remaining_tokens = max(
                        0.0, position.remaining_tokens - tokens_to_sell,
                    )
                await self._sync_wallet_balance(position)
                if position.remaining_tokens > 0:
                    raw = int(position.remaining_tokens * (10 ** position.decimals))
                    rem_usd = await self.executor.get_sell_quote_usd(position.mint, raw)
                    if rem_usd is not None and rem_usd < DUST_BALANCE_USD:
                        logger.info("Dust cleared — %s ($%.2f left, stopping retries)",
                                      position.symbol, rem_usd)
                        position.remaining_tokens = 0
                position.total_sol_received += received
            position.partial_exits.append({
                "reason": exit_reason,
                "tokens": tokens_to_sell,
                "sol": received,
                "tx": sell_result.tx_signature or "",
                "time": datetime.now(timezone.utc),
            })
            await self._persist(position)
            unit = sell_result.exit_label or ("stable" if SELL_TO_STABLE else "SOL")
            logger.info("Partial sell — %s | %s | %.2f%% | %.4f %s",
                        position.symbol, exit_reason, sell_pct, received, unit)
            if sell_pct < 99.0 and not sell_result.is_dust:
                received_usd = received if SELL_TO_STABLE else (
                    received * await self.executor.get_sol_price_usd()
                )
                remaining_pct = (
                    (position.remaining_tokens / position.initial_tokens) * 100.0
                    if position.initial_tokens > 0 else 0.0
                )
                log_event(
                    "PARTIAL_SELL", symbol=position.symbol, mint=position.mint,
                    sell_pct=sell_pct, received_usd=round(received_usd, 2),
                    reason=exit_reason,
                )
                await self.alerter.send_partial_sell_alert(
                    position.symbol, sell_pct, received_usd, exit_reason, remaining_pct,
                )
            return True

        position.sell_fail_count += 1
        await self._persist(position)
        logger.warning("Sell failed for %s (%s) — attempt %d, will retry",
                         position.symbol, exit_reason, position.sell_fail_count)
        return False

    async def _close_position(self, position: Position, exit_reason: str, exit_price: float) -> None:
        if position.closed:
            return

        if not self.executor.paper_trade:
            if not is_on_chain_tx(position.entry_tx_signature):
                await self._purge_ghost_position(position, "no on-chain buy tx")
                return

        wallet_amt, wallet_dec = await self.executor.get_token_balance(position.mint)
        if wallet_amt > 0:
            position.remaining_tokens = wallet_amt
            position.decimals = wallet_dec

        if position.remaining_tokens > 0:
            sold = await self._sell_partial(position, 100.0, exit_reason)
            if not sold:
                return  # keep position open — sell failed, retry next poll

        await self._sync_wallet_balance(position)
        wallet_amt, _ = await self.executor.get_token_balance(position.mint)

        if not self.executor.paper_trade:
            if wallet_amt > 0.0001:
                return  # tokens still in wallet after sell attempt
            has_real_exit = (
                position.total_sol_received > 0
                or any(is_on_chain_tx(e.get("tx")) for e in position.partial_exits)
            )
            if not has_real_exit:
                logger.warning(
                    "Refusing fake close %s — no on-chain sell proceeds recorded",
                    position.symbol,
                )
                return
        elif position.remaining_tokens > 0.0001:
            return  # paper: tokens still held

        position.closed = True
        await self._persist(position)

        exit_time = datetime.now(timezone.utc)
        sol_price = await self.executor.get_sol_price_usd()
        spent_usd = position.entry_cost_usd or (position.initial_sol * sol_price)
        if self.executor.paper_trade:
            partial_usd = position.total_sol_received if SELL_TO_STABLE else (
                position.total_sol_received * sol_price
            )
            remaining_frac = (
                position.remaining_tokens / position.initial_tokens
                if position.initial_tokens > 0 else 0.0
            )
            remaining_cost = spent_usd * remaining_frac
            exit_mult = self._paper_exit_multiplier(position, exit_reason)
            received_usd = round(partial_usd + remaining_cost * exit_mult, 2)
        else:
            received_usd = position.total_sol_received if SELL_TO_STABLE else (
                position.total_sol_received * sol_price
            )
        pnl_usd = received_usd - spent_usd
        pnl_sol = pnl_usd / sol_price if sol_price > 0 else 0.0

        self._reset_daily_if_needed()
        self._daily_pnl_usd += pnl_usd
        self.stats.lifetime_pnl_usd += pnl_usd
        self.stats.lifetime_trades += 1
        self.stats.total_spent_usd += spent_usd
        self.stats.total_received_usd += received_usd
        if pnl_usd >= 0:
            self.stats.wins += 1
        else:
            self.stats.losses += 1
        await self._persist_stats()

        log_event(
            "CLOSE", symbol=position.symbol, mint=position.mint,
            spent_usd=round(spent_usd, 2), received_usd=round(received_usd, 2),
            pnl_usd=round(pnl_usd, 2), exit_reason=exit_reason,
            daily_pnl=round(self._daily_pnl_usd, 2),
            lifetime_pnl=round(self.stats.lifetime_pnl_usd, 2),
        )

        exit_tx = ""
        for partial in reversed(position.partial_exits):
            tx = partial.get("tx", "")
            if is_on_chain_tx(tx):
                exit_tx = tx
                break

        alert = TradeAlert(
            token_mint=position.mint,
            token_symbol=position.symbol,
            reason=position.reason,
            entry_price=position.entry_price_usd,
            exit_price=exit_price,
            exit_reason=exit_reason,
            entry_time=position.entry_time,
            exit_time=exit_time,
            pnl_sol=pnl_sol,
            pnl_usd=pnl_usd,
            spent_usd=spent_usd,
            received_usd=received_usd,
            peak_multiplier=position.peak_multiplier,
            daily_pnl_usd=self._daily_pnl_usd,
            lifetime_pnl_usd=self.stats.lifetime_pnl_usd,
            buys_today=self._buys_today,
            score_breakdown=position.score_breakdown,
            sol_price_usd=sol_price,
            entry_tx=position.entry_tx_signature,
            exit_tx=exit_tx,
        )
        await self.alerter.send_trade_alert(alert)
        if (
            DAILY_PROFIT_TARGET_USD > 0
            and self._daily_pnl_usd >= DAILY_PROFIT_TARGET_USD
            and not self._halted_today
        ):
            self._halted_today = True
            self._halt_reason = f"profit target hit (+${DAILY_PROFIT_TARGET_USD:.0f})"
            await self.alerter.send_message(
                f"<b>🏦 Profits locked — done for today</b>\n"
                f"Daily PnL: <b>+${self._daily_pnl_usd:.2f}</b>\n"
                f"Target: +${DAILY_PROFIT_TARGET_USD:.0f}\n"
                f"No new buys until tomorrow. USDC stays in your wallet."
            )
            await self._save_bot_state()
        elif self._daily_pnl_usd <= -DAILY_LOSS_LIMIT_USD and not self._halted_today:
            self._halted_today = True
            self._halt_reason = f"daily loss limit hit (-${DAILY_LOSS_LIMIT_USD:.0f})"
            await self.alerter.send_message(
                f"<b>⛔ Trading paused for today</b>\n"
                f"Daily PnL: ${self._daily_pnl_usd:.2f}\n"
                f"Limit: -${DAILY_LOSS_LIMIT_USD:.0f}\n"
                f"No new buys until tomorrow."
            )
            await self._save_bot_state()

        if pnl_usd < 0:
            self._loss_cooldown[position.mint] = exit_time + timedelta(hours=COPY_REBUY_COOLDOWN_HOURS)

        self._traded_mints = BotStateStore.append_trade(
            self._traded_mints,
            mint=position.mint,
            symbol=position.symbol,
            pnl_usd=pnl_usd,
            exit_reason=exit_reason,
        )
        await self._save_bot_state()

        hold_time = (exit_time - position.entry_time).total_seconds()
        logger.info("Position closed — %s | %s | hold %s | PnL %.4f SOL | peak %.2fx",
                    position.symbol, exit_reason, format_duration(hold_time), pnl_sol,
                    position.peak_multiplier)

    async def _evaluate_position(self, position: Position) -> None:
        if position.closed:
            return

        current_price = await self.executor.get_token_price_usd(position.mint)
        if not current_price or current_price <= 0:
            position.price_miss_count += 1
            # Force-sell after 10 consecutive price failures (~50s) — token is dead/illiquid
            if position.price_miss_count >= 6:
                logger.warning("Force-selling %s — no price data for %d checks",
                               position.symbol, position.price_miss_count)
                await self._close_position(position, "no_price_data", position.entry_price_usd)
            return
        position.price_miss_count = 0

        multiplier   = self._cap_multiplier(self._current_multiplier(position, current_price))
        position.peak_multiplier = self._cap_multiplier(
            max(position.peak_multiplier, multiplier),
        )
        pnl_pct      = (multiplier - 1.0) * 100.0
        hold_minutes = (datetime.now(timezone.utc) - position.entry_time).total_seconds() / 60.0

        # Also check real Jupiter sell quote — price feed can lie on meme coins
        await self._sync_wallet_balance(position)
        if position.remaining_tokens > 0:
            raw = int(position.remaining_tokens * (10 ** position.decimals))
            quote_sol = await self.executor.get_sell_quote_sol(position.mint, raw)
            if quote_sol and position.initial_sol > 0:
                quote_mult = quote_sol / position.initial_sol
                if not self.executor.paper_trade:
                    position.peak_multiplier = max(position.peak_multiplier, quote_mult)
                quote_pnl_pct = (quote_mult - 1.0) * 100.0
                if quote_pnl_pct > pnl_pct and not self.executor.paper_trade:
                    pnl_pct = quote_pnl_pct
                    multiplier = quote_mult

        position.last_multiplier = multiplier

        # Take profits FIRST — don't let a fast dump skip TP
        if multiplier >= TP1_MULTIPLIER and not position.tp1_hit:
            if await self._sell_partial(position, TP1_SELL_PCT, "TP1"):
                position.tp1_hit = True
                await self._persist(position)

        if multiplier >= TP2_MULTIPLIER and not position.tp2_hit:
            if await self._sell_partial(position, TP2_SELL_PCT, "TP2"):
                position.tp2_hit = True
                await self._persist(position)

        if multiplier >= TP3_MULTIPLIER and not position.tp3_hit:
            if await self._sell_partial(position, TP3_SELL_PCT, "TP3"):
                position.tp3_hit = True
                await self._persist(position)
                if position.remaining_tokens <= 0:
                    await self._close_position(position, "TP3", current_price)
                    return

        # Stop loss
        if pnl_pct <= STOP_LOSS_PCT:
            await self._close_position(position, "SL", current_price)
            return

        # Activate trailing stop after 3x
        if multiplier >= TRAILING_ACTIVATION_MULTIPLIER:
            position.trailing_active = True
            position.trailing_peak_multiplier = max(position.trailing_peak_multiplier, multiplier)

        # Trailing stop -18% from peak
        if position.trailing_active:
            drawdown = ((multiplier - position.trailing_peak_multiplier)
                        / position.trailing_peak_multiplier) * 100.0
            if drawdown <= TRAILING_STOP_PCT:
                await self._close_position(position, "trailing", current_price)
                return

        # Time stop — not moving up
        if hold_minutes >= TIME_STOP_MINUTES and multiplier < TIME_STOP_MIN_MULTIPLIER:
            await self._close_position(position, "time_stop", current_price)
            return

        # Hard max hold — never sit in a bag forever
        if hold_minutes >= MAX_HOLD_MINUTES:
            logger.info("Max hold %d min — force exit %s", MAX_HOLD_MINUTES, position.symbol)
            await self._close_position(position, "max_hold", current_price)
            return

    async def run(self) -> None:
        self._running = True
        logger.info("Risk manager started — polling every %ds", RISK_POLL_INTERVAL_SECONDS)

        while self._running:
            try:
                open_positions = [p for p in self.positions.values() if not p.closed]
                if open_positions:
                    logger.debug("Monitoring %d open position(s)", len(open_positions))
                    for position in open_positions:
                        await self._evaluate_position(position)
            except Exception as exc:
                logger.error("Risk manager loop error: %s", exc)
            await asyncio.sleep(RISK_POLL_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._running = False
