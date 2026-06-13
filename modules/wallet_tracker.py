from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import aiohttp

from config import (
    CASH_MINT,
    COPY_GRADUATED_ONLY,
    COPY_MAX_MARKET_CAP_USD,
    COPY_MAX_TRADER_SOL,
    COPY_MIN_GRADUATED_LIQUIDITY_USD,
    COPY_MIN_TRADER_SOL,
    COPY_SKIP_IF_HOLDING,
    COPY_USE_COUNCIL,
    COPY_BUY_SLIPPAGE_BPS,
    DEXSCREENER_TOKEN_URL,
    HELIUS_API_KEY,
    HELIUS_TX_URL,
    SELL_SLIPPAGE_BPS,
    SOL_MINT,
    TRADER_BY_ADDRESS,
    TRADERS,
    USDC_MINT,
    WALLET_POLL_INTERVAL_SECONDS,
)
from modules.council_gate import council_gate
from modules.rugcheck_client import fetch_rug_report
from modules.utils import fetch_json, lamports_to_sol, sol_to_lamports

if TYPE_CHECKING:
    from modules.executor import Executor

logger = logging.getLogger("solana-bot.wallet_tracker")

STABLE_MINTS = {SOL_MINT, USDC_MINT, CASH_MINT}


class WalletTracker:
    def __init__(self, session: aiohttp.ClientSession, executor: "Executor") -> None:
        self.session = session
        self.executor = executor
        self._last_signatures: dict[str, str] = {}
        self._seen_signatures: dict[str, set[str]] = {t.address: set() for t in TRADERS}
        self._running = False

    async def _fetch_transactions(self, address: str) -> list[dict[str, Any]] | None:
        url = HELIUS_TX_URL.format(address=address)
        params = {
            "api-key": HELIUS_API_KEY,
            "limit": 5,   # reduced from 10 to cut request weight
            "type": "SWAP",
        }
        try:
            async with self.session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status == 429:
                    logger.warning("Helius 429 — backing off 30s")
                    return None
                if resp.status != 200:
                    return []
                data = await resp.json(content_type=None)
            if isinstance(data, list):
                return data
            return data.get("data", data) if isinstance(data, dict) else []
        except Exception as exc:
            logger.warning("Helius fetch error %s: %s", address[:8], exc)
            return []

    def _estimate_sol_spent(self, tx: dict[str, Any], trader_address: str) -> float:
        sol_spent = 0.0

        for transfer in tx.get("nativeTransfers", []):
            if transfer.get("fromUserAccount") == trader_address:
                sol_spent += lamports_to_sol(transfer.get("amount", 0))

        for transfer in tx.get("tokenTransfers", []):
            mint = transfer.get("mint", "")
            if mint == SOL_MINT and transfer.get("fromUserAccount") == trader_address:
                sol_spent += float(transfer.get("tokenAmount", 0) or 0)

        if sol_spent == 0:
            fee_payer = tx.get("feePayer", "")
            if fee_payer == trader_address:
                account_data = tx.get("accountData", [])
                for acct in account_data:
                    if acct.get("account") == trader_address:
                        native_change = acct.get("nativeBalanceChange", 0)
                        if native_change < 0:
                            sol_spent = lamports_to_sol(abs(native_change))

        return sol_spent

    def _detect_buy(self, tx: dict[str, Any], trader_address: str) -> tuple[str, str] | None:
        if tx.get("type") not in ("SWAP", "UNKNOWN", None):
            if tx.get("type") and "SWAP" not in str(tx.get("type", "")).upper():
                pass

        received_tokens: list[tuple[str, float, str]] = []

        for transfer in tx.get("tokenTransfers", []):
            to_user = transfer.get("toUserAccount", "")
            mint = transfer.get("mint", "")
            amount = float(transfer.get("tokenAmount", 0) or 0)

            if to_user == trader_address and mint not in STABLE_MINTS and amount > 0:
                symbol = transfer.get("tokenSymbol") or mint[:6]
                received_tokens.append((mint, amount, symbol))

        if not received_tokens:
            events = tx.get("events", {}) or {}
            swap_event = events.get("swap", {})
            if swap_event:
                token_outputs = swap_event.get("tokenOutputs", [])
                for output in token_outputs:
                    mint = output.get("mint", "")
                    if mint and mint not in STABLE_MINTS:
                        symbol = output.get("symbol") or mint[:6]
                        received_tokens.append((mint, float(output.get("rawTokenAmount", {}).get("tokenAmount", 0) or 0), symbol))

        if not received_tokens:
            return None

        mint, _, symbol = max(received_tokens, key=lambda x: x[1])
        return mint, symbol

    async def _get_best_pair(self, mint: str) -> dict | None:
        try:
            url = DEXSCREENER_TOKEN_URL.format(mint=mint)
            data = await fetch_json(self.session, "GET", url, label=f"DexScreener {mint[:8]}")
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            return max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
        except Exception:
            return None

    async def _get_market_cap(self, mint: str) -> float | None:
        pair = await self._get_best_pair(mint)
        if not pair:
            return None
        return float(pair.get("marketCap") or pair.get("fdv") or 0)

    async def _is_graduated(self, mint: str) -> bool:
        """Token has graduated from pump.fun — trading on a real DEX with liquidity."""
        pair = await self._get_best_pair(mint)
        if not pair:
            return False
        dex = (pair.get("dexId") or "").lower()
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        graduated_dexes = {"raydium", "orca", "meteora", "pumpswap"}
        return dex in graduated_dexes and liq >= COPY_MIN_GRADUATED_LIQUIDITY_USD

    def _already_holding(self, mint: str) -> bool:
        rm = self.executor.risk_manager
        return rm.is_holding(mint) if rm else False

    async def _process_transaction(self, trader_address: str, tx: dict[str, Any]) -> None:
        signature = tx.get("signature", "")
        if not signature:
            return

        seen = self._seen_signatures.setdefault(trader_address, set())
        if signature in seen:
            return
        seen.add(signature)
        if len(seen) > 500:
            self._seen_signatures[trader_address] = set(list(seen)[-250:])

        buy = self._detect_buy(tx, trader_address)
        if not buy:
            return

        mint, symbol = buy
        trader = TRADER_BY_ADDRESS.get(trader_address)
        if not trader:
            return

        sol_spent = self._estimate_sol_spent(tx, trader_address)
        logger.info(
            "Detected buy from %s (%s) — %s spent %.4f SOL on %s",
            trader.name,
            trader.handle,
            symbol,
            sol_spent,
            mint[:8],
        )

        if sol_spent < COPY_MIN_TRADER_SOL:
            logger.info("Skipping — trader spent %.4f SOL (< %.1f min)", sol_spent, COPY_MIN_TRADER_SOL)
            return
        if sol_spent > COPY_MAX_TRADER_SOL:
            logger.info("Skipping — trader spent %.4f SOL (> %.0f max)", sol_spent, COPY_MAX_TRADER_SOL)
            return

        can_buy, skip = await self.executor.can_trade(mint)
        if not can_buy:
            logger.info("Skipping %s — %s", symbol, skip)
            return

        if COPY_GRADUATED_ONLY:
            if not await self._is_graduated(mint):
                logger.info(
                    "Skipping %s — not graduated (needs Raydium/Orca pool with $%dk+ liq)",
                    symbol, COPY_MIN_GRADUATED_LIQUIDITY_USD // 1000,
                )
                return

        if COPY_SKIP_IF_HOLDING and self._already_holding(mint):
            logger.info("Skipping %s — already holding", symbol)
            return

        market_cap = await self._get_market_cap(mint)
        if market_cap and market_cap > COPY_MAX_MARKET_CAP_USD:
            logger.info(
                "Skipping — market cap $%.0f exceeds $%d limit",
                market_cap,
                COPY_MAX_MARKET_CAP_USD,
            )
            return

        pair = await self._get_best_pair(mint) or {}
        liq = float(pair.get("liquidity", {}).get("usd", 0) or 0)
        dex = (pair.get("dexId") or "?") if pair else "?"

        # Verify Jupiter sell route before council
        sell_ok = False
        try:
            buy_quote = await self.executor.get_quote(
                SOL_MINT, mint, sol_to_lamports(trader.copy_amount_sol),
            )
            test_amount = max(int(buy_quote.get("outAmount", 0)) // 10, 1)
            sell_quote, _ = await self.executor._get_exit_quote(
                mint, test_amount, SELL_SLIPPAGE_BPS,
            )
            sell_ok = bool(sell_quote and int(sell_quote.get("outAmount", 0)) > 0)
            if not sell_ok:
                logger.info("Skipping %s — Jupiter sell route failed (unsellable)", symbol)
                return
        except Exception:
            logger.info("Skipping %s — could not verify sell route", symbol)
            return

        rm = self.executor.risk_manager
        daily_ok = True
        if rm:
            daily_ok, _ = await rm.can_open_new_trade()

        council = None
        rug = await fetch_rug_report(self.session, mint)
        if COPY_USE_COUNCIL:
            approved, council, rug = await council_gate(
                self.session,
                mint=mint,
                symbol=symbol,
                pair=pair,
                source="copy",
                sell_route_ok=sell_ok,
                score=80.0,
                breakdown={"trader": trader.name},
                rug=rug,
                on_loss_cooldown=rm.on_cooldown(mint) if rm else False,
                prior_losses=rm.prior_loss_count(mint) if rm else 0,
                daily_budget_ok=daily_ok,
                trader_name=trader.name,
            )
            if not approved:
                logger.info(
                    "Copy skip %s — HERMES Council %s rejected",
                    symbol, council.score if council else "?",
                )
                return
        elif not rug.ok:
            logger.info("Skipping %s — RugCheck failed", symbol)
            return

        mcap_str = f"${market_cap:,.0f}" if market_cap else "unknown"
        breakdown = {
            "market_cap": mcap_str,
            "liquidity": f"${liq:,.0f}",
            "dex": dex,
            "trader": f"{trader.name} ({trader.handle})",
            "trader_spent": f"{sol_spent:.3f} SOL",
            "council": council.score if council else "legacy",
        }
        buy_sol = await self.executor.calc_buy_size_sol()
        reason = f"Copy {trader.name} — bought {symbol}"
        await self.executor.buy_token(
            mint=mint,
            amount_sol=buy_sol,
            reason=reason,
            symbol=symbol,
            score_breakdown=breakdown,
            slippage_bps=COPY_BUY_SLIPPAGE_BPS,
        )

    async def _poll_wallet(self, trader_address: str) -> None:
        try:
            transactions = await self._fetch_transactions(trader_address)
            if transactions is None:  # 429 returned None
                await asyncio.sleep(30)  # back off 30s on rate limit
                return
            if not transactions:
                return

            newest_sig = transactions[0].get("signature", "")
            last_known = self._last_signatures.get(trader_address)

            if last_known is None:
                self._last_signatures[trader_address] = newest_sig
                for tx in transactions:
                    sig = tx.get("signature", "")
                    if sig:
                        self._seen_signatures[trader_address].add(sig)
                logger.info("Initialized wallet tracker for %s", trader_address[:8])
                return

            if newest_sig == last_known:
                return

            new_txs = []
            for tx in transactions:
                sig = tx.get("signature", "")
                if sig == last_known:
                    break
                new_txs.append(tx)

            self._last_signatures[trader_address] = newest_sig

            for tx in reversed(new_txs):
                await self._process_transaction(trader_address, tx)

        except Exception as exc:
            logger.error("Error polling wallet %s: %s", trader_address[:8], exc)

    async def run(self) -> None:
        self._running = True
        trader_names = ", ".join(f"{t.name}" for t in TRADERS)
        logger.info(
            "Wallet tracker started — polling %d wallets every %ds (%s)",
            len(TRADERS),
            WALLET_POLL_INTERVAL_SECONDS,
            trader_names,
        )

        while self._running:
            # Poll all wallets in parallel — fastest copy path
            await asyncio.gather(
                *[self._poll_wallet(trader.address) for trader in TRADERS],
                return_exceptions=True,
            )
            await asyncio.sleep(WALLET_POLL_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._running = False
