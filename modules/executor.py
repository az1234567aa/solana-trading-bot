from __future__ import annotations

import asyncio
import base64
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import aiohttp
from solders.keypair import Keypair
from solders.message import to_bytes_versioned
from solders.transaction import VersionedTransaction

import config
from config import (
    BUY_MINT_COOLDOWN_SEC,
    BUY_PRIORITY_FEE_LAMPORTS,
    BUY_SIZE_PCT_OF_WALLET,
    BUY_SLIPPAGE_BPS,
    DEFAULT_SLIPPAGE_BPS,
    HELIUS_RPC_URL,
    MAX_BUY_SOL,
    MIN_BUY_SOL,
    MIN_SOL_RESERVE,
    DUST_BALANCE_USD,
    EXIT_DECIMALS,
    EXIT_LABELS,
    EXIT_MINTS,
    JUPITER_MIN_INTERVAL_SEC,
    JUPITER_429_RETRIES,
    JUPITER_PRICE_URL,
    JUPITER_QUOTE_URL,
    JUPITER_SWAP_URL,
    LAMPORTS_PER_SOL,
    MIN_SELL_VALUE_USD,
    PAPER_TRADE,
    SELL_PRIORITY_FEE_LAMPORTS,
    SELL_SLIPPAGE_BPS,
    SELL_SLIPPAGE_RETRY_BPS,
    SELL_TO_STABLE,
    SOL_MINT,
    SOLANA_SEND_RPC_URL,
    WALLET_PRIVATE_KEY,
)
from modules.utils import fetch_json, lamports_to_sol, retry_async, sol_to_lamports

if TYPE_CHECKING:
    from modules.risk_manager import RiskManager

logger = logging.getLogger("solana-bot.executor")


@dataclass
class BuyResult:
    success: bool
    mint: str
    symbol: str
    amount_sol: float
    tokens_received: float
    entry_price_usd: float
    tx_signature: str | None
    reason: str
    score_breakdown: dict[str, Any] | None = None
    decimals: int = 6
    position_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class SellResult:
    success: bool
    mint: str
    symbol: str
    amount_tokens: float
    sol_received: float       # stablecoin amount (~USD) when SELL_TO_STABLE, else SOL
    exit_price_usd: float
    tx_signature: str | None
    sell_pct: float
    exit_mint: str = ""
    exit_label: str = ""
    is_dust: bool = False


class Executor:
    def __init__(self, session: aiohttp.ClientSession, risk_manager: "RiskManager | None" = None) -> None:
        self.session = session
        self.risk_manager = risk_manager
        self.keypair = self._load_keypair()
        self.paper_trade = PAPER_TRADE
        self._sol_price_usd = 150.0
        self._sol_price_last_fetch = 0.0
        self._token_price_cache: dict[str, tuple[float, float | None]] = {}
        self._jupiter_lock = asyncio.Lock()
        self._jupiter_last_call = 0.0
        self._buy_in_flight: set[str] = set()
        self._recent_buys: dict[str, float] = {}  # mint → unix time
        self._balance_cache: dict[str, tuple[float, int, float]] = {}

    def can_buy_mint(self, mint: str) -> tuple[bool, str]:
        if mint in self._buy_in_flight:
            return False, "buy already in progress"
        last = self._recent_buys.get(mint, 0)
        if time.time() - last < BUY_MINT_COOLDOWN_SEC:
            return False, f"bought recently ({BUY_MINT_COOLDOWN_SEC // 60}m cooldown)"
        rm = self.risk_manager
        if rm:
            if rm.is_holding(mint):
                return False, "already holding"
            if rm.on_cooldown(mint):
                return False, "on loss cooldown"
        return True, ""

    async def can_trade(self, mint: str) -> tuple[bool, str]:
        ok, reason = self.can_buy_mint(mint)
        if not ok:
            return ok, reason
        rm = self.risk_manager
        if rm:
            ok, reason = await rm.can_open_new_trade()
            if not ok:
                return False, reason
        size = await self.calc_buy_size_sol()
        if size < MIN_BUY_SOL:
            return False, f"wallet too low ({size:.4f} SOL tradeable, need {MIN_BUY_SOL})"
        return True, ""

    async def get_wallet_sol_balance(self) -> float:
        if self.paper_trade or not self.keypair:
            return 1.0
        body = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getBalance",
            "params": [str(self.keypair.pubkey())],
        }
        for rpc in (SOLANA_SEND_RPC_URL, HELIUS_RPC_URL):
            try:
                async with self.session.post(rpc, json=body) as resp:
                    data = await resp.json()
                    lamports = int(data.get("result", {}).get("value", 0))
                    return lamports / LAMPORTS_PER_SOL
            except Exception:
                continue
        return 0.0

    async def calc_buy_size_sol(self) -> float:
        balance = await self.get_wallet_sol_balance()
        tradeable = max(balance - MIN_SOL_RESERVE, 0.0)
        size = min(MAX_BUY_SOL, tradeable * BUY_SIZE_PCT_OF_WALLET)
        return round(size, 4)

    async def _throttle_jupiter(self) -> None:
        async with self._jupiter_lock:
            elapsed = time.time() - self._jupiter_last_call
            if elapsed < JUPITER_MIN_INTERVAL_SEC:
                await asyncio.sleep(JUPITER_MIN_INTERVAL_SEC - elapsed)
            self._jupiter_last_call = time.time()

    async def _jupiter_request(
        self,
        method: str,
        url: str,
        *,
        params: dict | None = None,
        json_body: dict | None = None,
        label: str = "Jupiter",
    ) -> dict[str, Any]:
        for attempt in range(1, JUPITER_429_RETRIES + 1):
            await self._throttle_jupiter()
            async with self.session.request(
                method, url,
                params=params, json=json_body,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 429:
                    wait = min(30, 3 * (2 ** (attempt - 1)))
                    logger.warning("%s 429 — backing off %ds (attempt %d/%d)",
                                   label, wait, attempt, JUPITER_429_RETRIES)
                    await asyncio.sleep(wait)
                    continue
                if resp.status >= 400:
                    body = await resp.text()
                    raise RuntimeError(f"{label} HTTP {resp.status}: {body[:200]}")
                return await resp.json()

        raise RuntimeError(f"{label} rate-limited after {JUPITER_429_RETRIES} retries")

    def _load_keypair(self) -> Keypair | None:
        if not WALLET_PRIVATE_KEY or WALLET_PRIVATE_KEY.startswith("your_"):
            if not PAPER_TRADE:
                logger.warning("No valid WALLET_PRIVATE_KEY — forcing paper trade mode")
            return None
        try:
            import base58

            raw = base58.b58decode(WALLET_PRIVATE_KEY)
            return Keypair.from_bytes(raw)
        except Exception:
            try:
                import json

                secret = json.loads(WALLET_PRIVATE_KEY)
                return Keypair.from_bytes(bytes(secret))
            except Exception as exc:
                logger.error("Failed to load keypair: %s", exc)
                return None

    @property
    def public_key(self) -> str:
        if self.keypair:
            return str(self.keypair.pubkey())
        return "PAPER_WALLET"

    def _parse_exit_amount(self, out_raw: int, exit_mint: str) -> float:
        if SELL_TO_STABLE and exit_mint in EXIT_DECIMALS:
            return out_raw / (10 ** EXIT_DECIMALS[exit_mint])
        return lamports_to_sol(out_raw)

    async def _exit_value_usd(self, out_raw: int, exit_mint: str) -> float:
        amount = self._parse_exit_amount(out_raw, exit_mint)
        if SELL_TO_STABLE and exit_mint != SOL_MINT:
            return amount
        return amount * await self.get_sol_price_usd()

    async def _get_exit_quote(
        self, mint: str, raw_amount: int, slippage_bps: int,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """Try Phantom Cash (CASH) first, then USDC, then SOL."""
        for exit_mint in EXIT_MINTS:
            try:
                quote = await self.get_quote(mint, exit_mint, raw_amount, slippage_bps=slippage_bps)
                if int(quote.get("outAmount", 0)) > 0:
                    return quote, exit_mint
            except Exception:
                continue
        return None, None

    async def get_sol_price_usd(self) -> float:
        import time
        now = time.time()
        if now - self._sol_price_last_fetch < 60:
            return self._sol_price_usd
        self._sol_price_last_fetch = now
        try:
            data = await fetch_json(
                self.session,
                "GET",
                JUPITER_PRICE_URL,
                params={"ids": SOL_MINT},
                label="SOL price fetch",
            )
            price = (
                data.get(SOL_MINT, {}).get("usdPrice")
                or data.get("data", {}).get(SOL_MINT, {}).get("price")
                or data.get(SOL_MINT, {}).get("price")
            )
            if price:
                self._sol_price_usd = float(price)
        except Exception as exc:
            logger.warning("SOL price fetch failed, using cached: %s", exc)
        return self._sol_price_usd

    async def get_token_decimals(self, mint: str) -> int:
        """Token decimals from Jupiter price API — quote outDecimals is often missing."""
        try:
            data = await fetch_json(
                self.session, "GET", JUPITER_PRICE_URL,
                params={"ids": mint}, label=f"Jupiter decimals {mint[:8]}",
            )
            info = data.get(mint) or data.get("data", {}).get(mint) or {}
            dec = info.get("decimals")
            if dec is not None:
                return int(dec)
        except Exception:
            pass
        return 6

    async def get_token_price_usd(self, mint: str) -> float | None:
        import time
        now = time.time()
        cached_time, cached_price = self._token_price_cache.get(mint, (0, None))
        if now - cached_time < 30:
            return cached_price

        result = await self._price_from_jupiter(mint)

        if not result:
            result = await self._price_from_dexscreener(mint)

        if not result:
            result = await self._price_from_gmgn(mint)

        self._token_price_cache[mint] = (now, result)
        return result

    async def _price_from_jupiter(self, mint: str) -> float | None:
        try:
            data = await fetch_json(
                self.session, "GET", JUPITER_PRICE_URL,
                params={"ids": mint}, label=f"Jupiter price {mint[:8]}",
            )
            price = (
                data.get(mint, {}).get("usdPrice")
                or data.get("data", {}).get(mint, {}).get("price")
                or data.get(mint, {}).get("price")
            )
            return float(price) if price else None
        except Exception:
            return None

    async def _price_from_dexscreener(self, mint: str) -> float | None:
        try:
            from config import DEXSCREENER_TOKEN_URL
            url = DEXSCREENER_TOKEN_URL.format(mint=mint)
            data = await fetch_json(
                self.session, "GET", url, label=f"DexScreener price {mint[:8]}",
            )
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
            price_str = best.get("priceUsd")
            return float(price_str) if price_str else None
        except Exception:
            return None

    async def _price_from_gmgn(self, mint: str) -> float | None:
        try:
            url = f"https://gmgn.ai/defi/quotation/v1/tokens/sol/{mint}"
            headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://gmgn.ai/"}
            data = await fetch_json(
                self.session, "GET", url, headers=headers,
                label=f"GMGN price {mint[:8]}",
            )
            price = (
                data.get("data", {}).get("price")
                or data.get("data", {}).get("priceUsd")
            )
            return float(price) if price else None
        except Exception:
            return None

    async def get_quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        slippage_bps: int = DEFAULT_SLIPPAGE_BPS,
    ) -> dict[str, Any]:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": str(slippage_bps),
        }
        return await self._jupiter_request(
            "GET", JUPITER_QUOTE_URL, params=params, label="Jupiter quote",
        )

    async def execute_swap(
        self, quote: dict[str, Any], *, priority_fee: int = SELL_PRIORITY_FEE_LAMPORTS,
    ) -> str | None:
        if self.paper_trade or not self.keypair:
            out_amount = int(quote.get("outAmount", 0))
            logger.info(
                "[PAPER] Would execute swap — in=%s out=%s",
                quote.get("inAmount"),
                out_amount,
            )
            return f"PAPER_{uuid.uuid4().hex[:16]}"

        payload = {
            "quoteResponse": quote,
            "userPublicKey": str(self.keypair.pubkey()),
            "wrapAndUnwrapSol": True,
            "dynamicComputeUnitLimit": True,
            "prioritizationFeeLamports": priority_fee,
        }
        swap_data = await self._jupiter_request(
            "POST", JUPITER_SWAP_URL, json_body=payload, label="Jupiter swap",
        )
        swap_tx_b64 = swap_data.get("swapTransaction")
        if not swap_tx_b64:
            raise ValueError("Jupiter swap response missing swapTransaction")

        raw_tx = VersionedTransaction.from_bytes(base64.b64decode(swap_tx_b64))

        # ── Transaction guard ────────────────────────────────────────────────
        # Decode account keys and check every instruction before signing.
        # Only Jupiter program IDs are allowed. Plain SOL transfers (System
        # Program + only 2 accounts) are always blocked regardless of source.
        _JUPITER_PROGRAMS = {
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",   # Jupiter v6
            "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",   # Jupiter v4
            "JUP3c2Uh3WA4Ng34tw6kPd2G4eZfYavyzfYzjvMjkU7",   # Jupiter v3
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",   # Whirlpool
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",  # Raydium AMM v4
            "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",  # Raydium CLMM
            "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",  # Raydium CPMM
            "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJe1bC8",  # ATA program
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022
            "ComputeBudget111111111111111111111111111111",     # Compute budget
            "11111111111111111111111111111111",                # System Program
            # Pump.fun bonding curve + PumpSwap AMM (Jupiter routes through these)
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
            "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
        }
        _SYSTEM_PROGRAM = "11111111111111111111111111111111"

        try:
            msg = raw_tx.message
            # solders exposes account_keys as a list of Pubkey objects
            account_keys = [str(k) for k in msg.account_keys]
            instructions = msg.instructions

            non_jupiter = []
            has_jupiter = False
            has_pump = False
            for ix in instructions:
                prog = account_keys[ix.program_id_index]
                if prog not in _JUPITER_PROGRAMS:
                    non_jupiter.append(prog)
                if prog.startswith("JUP"):
                    has_jupiter = True
                if prog in ("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P", "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"):
                    has_pump = True

            # Block plain SOL transfer: System Program instruction with exactly
            # 2 account indices (from + to) = raw lamport send to another wallet
            for ix in instructions:
                prog = account_keys[ix.program_id_index]
                if prog == _SYSTEM_PROGRAM and len(ix.accounts) == 2:
                    recipient = account_keys[ix.accounts[1]]
                    own_pk = str(self.keypair.pubkey())
                    if recipient != own_pk:
                        raise ValueError(
                            f"GUARD BLOCKED — plain SOL transfer to {recipient} detected. "
                            "This is not a Jupiter swap. Transaction refused."
                        )

            if non_jupiter:
                raise ValueError(
                    f"GUARD BLOCKED — unknown programs in tx: {non_jupiter}. "
                    "Only Jupiter swap routes are allowed."
                )

            if not has_jupiter and not has_pump:
                raise ValueError(
                    f"GUARD BLOCKED — transaction contains no Jupiter or Pump.fun swap. "
                    f"Programs seen: {account_keys}. Transaction refused."
                )

        except ValueError:
            raise
        except Exception as guard_err:
            raise ValueError(
                f"GUARD BLOCKED — could not verify transaction safety: {guard_err}"
            ) from guard_err
        # ── End transaction guard ─────────────────────────────────────────────

        signature = self.keypair.sign_message(to_bytes_versioned(raw_tx.message))
        signed_tx = VersionedTransaction.populate(raw_tx.message, [signature])
        encoded_tx = base64.b64encode(bytes(signed_tx)).decode()

        async def _send() -> str:
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "sendTransaction",
                "params": [
                    encoded_tx,
                    {"skipPreflight": True, "maxRetries": 3, "encoding": "base64"},
                ],
            }
            # Use public Solana RPC for sending to avoid Helius rate limits
            async with self.session.post(SOLANA_SEND_RPC_URL, json=body) as resp:
                result = await resp.json()
                if "error" in result:
                    raise RuntimeError(result["error"])
                return result["result"]

        tx_sig = await retry_async(_send, label="Send transaction")
        logger.info("Swap submitted: %s", tx_sig)
        return tx_sig

    async def buy_token(
        self,
        mint: str,
        amount_sol: float,
        reason: str,
        symbol: str = "UNKNOWN",
        score_breakdown: dict[str, Any] | None = None,
        slippage_bps: int | None = None,
    ) -> BuyResult:
        logger.info("BUY signal — %s (%s) for %.4f SOL — %s", symbol, mint[:8], amount_sol, reason)

        ok, skip_reason = await self.can_trade(mint)
        if not ok:
            logger.info("BUY skip %s — %s", symbol, skip_reason)
            return BuyResult(
                success=False, mint=mint, symbol=symbol, amount_sol=amount_sol,
                tokens_received=0, entry_price_usd=0, tx_signature=None,
                reason=reason, score_breakdown=score_breakdown,
            )

        amount_sol = min(amount_sol, await self.calc_buy_size_sol())
        if amount_sol < MIN_BUY_SOL:
            logger.info("BUY skip %s — size %.4f SOL below minimum", symbol, amount_sol)
            return BuyResult(
                success=False, mint=mint, symbol=symbol, amount_sol=amount_sol,
                tokens_received=0, entry_price_usd=0, tx_signature=None,
                reason=reason, score_breakdown=score_breakdown,
            )

        self._buy_in_flight.add(mint)
        lamports = sol_to_lamports(amount_sol)
        slip = slippage_bps if slippage_bps is not None else BUY_SLIPPAGE_BPS

        try:
            quote = await self.get_quote(SOL_MINT, mint, lamports, slippage_bps=slip)
            out_amount = int(quote.get("outAmount", 0))
            quote_decimals = quote.get("outDecimals")
            out_decimals = int(quote_decimals) if quote_decimals is not None else (
                await self.get_token_decimals(mint)
            )
            tokens_received = out_amount / (10**out_decimals)

            sol_price = await self.get_sol_price_usd()
            # Cost-basis entry — avoids bogus Jupiter spot prices on new memes
            if tokens_received > 0:
                token_price = (amount_sol * sol_price) / tokens_received
            else:
                token_price = await self.get_token_price_usd(mint) or 0.0

            tx_sig = await self.execute_swap(quote, priority_fee=BUY_PRIORITY_FEE_LAMPORTS)

            result = BuyResult(
                success=True,
                mint=mint,
                symbol=symbol,
                amount_sol=amount_sol,
                tokens_received=tokens_received,
                entry_price_usd=token_price or 0.0,
                tx_signature=tx_sig,
                reason=reason,
                score_breakdown=score_breakdown,
                decimals=out_decimals,
            )

            if self.risk_manager:
                await self.risk_manager.open_position(result)
                sol_price = await self.get_sol_price_usd()
                cost_usd = amount_sol * sol_price
                try:
                    await self.risk_manager.alerter.send_buy_alert(
                        symbol=symbol, mint=mint, amount_sol=amount_sol,
                        cost_usd=cost_usd, reason=reason, tx_sig=tx_sig,
                        buys_today=self.risk_manager._buys_today,
                        score_breakdown=score_breakdown,
                    )
                except Exception:
                    pass

            self._recent_buys[mint] = time.time()
            logger.info(
                "BUY complete — %s received %.4f tokens @ $%.8f (tx: %s)",
                symbol,
                tokens_received,
                result.entry_price_usd,
                tx_sig,
            )
            return result

        except Exception as exc:
            err = str(exc)
            logger.error("BUY failed for %s: %s", mint[:8], exc)
            # Don't spam Telegram for rate limits or duplicate attempts
            alert = "429" not in err and "rate" not in err.lower()
            if alert and self.risk_manager:
                try:
                    await self.risk_manager.alerter.send_message(
                        f"❌ <b>BUY FAILED — {symbol}</b>\n"
                        f"Error: {err[:200]}"
                    )
                except Exception:
                    pass
            return BuyResult(
                success=False,
                mint=mint,
                symbol=symbol,
                amount_sol=amount_sol,
                tokens_received=0,
                entry_price_usd=0,
                tx_signature=None,
                reason=reason,
                score_breakdown=score_breakdown,
            )
        finally:
            self._buy_in_flight.discard(mint)

    async def sell_token(
        self,
        mint: str,
        amount_tokens: float,
        decimals: int = 6,
        symbol: str = "UNKNOWN",
        sell_pct: float = 100.0,
    ) -> SellResult:
        # Always sell what's actually in the wallet — not stale tracked amounts
        wallet_amount, wallet_decimals = await self.get_token_balance(mint)
        if wallet_amount > 0:
            decimals = wallet_decimals
            amount_tokens = min(amount_tokens, wallet_amount)

        raw_amount = int(amount_tokens * (10**decimals))
        if raw_amount <= 0:
            logger.warning("SELL skip %s — zero balance in wallet", symbol)
            return SellResult(
                success=False, mint=mint, symbol=symbol, amount_tokens=0,
                sol_received=0, exit_price_usd=0, tx_signature=None, sell_pct=sell_pct,
            )

        logger.info(
            "SELL signal — %s (%.2f%%) — %.4f tokens (raw %d)",
            symbol, sell_pct, amount_tokens, raw_amount,
        )

        # Pre-check value — skip dust that spams alerts and never moves the needle
        try:
            preview, preview_mint = await self._get_exit_quote(mint, raw_amount, SELL_SLIPPAGE_BPS)
            if preview and preview_mint:
                preview_usd = await self._exit_value_usd(int(preview.get("outAmount", 0)), preview_mint)
                preview_label = EXIT_LABELS.get(preview_mint, "stable")
                if preview_usd < MIN_SELL_VALUE_USD:
                    logger.info(
                        "SELL skip %s — only $%.2f %s (dust, treating as done)",
                        symbol, preview_usd, preview_label,
                    )
                    return SellResult(
                        success=True, mint=mint, symbol=symbol, amount_tokens=amount_tokens,
                        sol_received=0, exit_price_usd=0, tx_signature=None,
                        sell_pct=sell_pct, is_dust=True,
                    )
        except Exception:
            pass

        last_exc: Exception | None = None
        for slippage in SELL_SLIPPAGE_RETRY_BPS:
            try:
                quote, exit_mint = await self._get_exit_quote(mint, raw_amount, slippage)
                if not quote or not exit_mint:
                    continue
                out_raw = int(quote.get("outAmount", 0))
                if out_raw <= 0:
                    continue
                exit_label = EXIT_LABELS.get(exit_mint, "stable")
                received = self._parse_exit_amount(out_raw, exit_mint)
                received_usd = await self._exit_value_usd(out_raw, exit_mint)
                exit_price_usd = await self.get_token_price_usd(mint) or 0.0

                logger.info(
                    "SELL attempt %s — slippage %d bps, expect $%.2f %s",
                    symbol, slippage, received_usd, exit_label,
                )
                tx_sig = await self.execute_swap(quote)

                logger.info(
                    "SELL complete — %s sold %.4f tokens for %.4f %s (tx: %s)",
                    symbol, amount_tokens, received, exit_label, tx_sig,
                )
                # PnL alert sent by risk_manager on full close — no "got back" spam here
                return SellResult(
                    success=True, mint=mint, symbol=symbol, amount_tokens=amount_tokens,
                    sol_received=received, exit_price_usd=exit_price_usd,
                    tx_signature=tx_sig, sell_pct=sell_pct,
                    exit_mint=exit_mint, exit_label=exit_label,
                )
            except Exception as exc:
                last_exc = exc
                logger.warning("SELL failed %s at %d bps: %s", symbol, slippage, exc)

        logger.error("SELL failed for %s after all retries: %s", mint[:8], last_exc)
        if self.risk_manager:
            try:
                await self.risk_manager.alerter.send_message(
                    f"❌ <b>SELL FAILED — {symbol}</b>\n"
                    f"Tokens still in wallet. Will retry.\n"
                    f"Error: {str(last_exc)[:150]}"
                )
            except Exception:
                pass
        return SellResult(
            success=False, mint=mint, symbol=symbol, amount_tokens=amount_tokens,
            sol_received=0, exit_price_usd=0, tx_signature=None, sell_pct=sell_pct,
        )

    async def get_token_balance(self, mint: str) -> tuple[float, int]:
        if self.paper_trade or not self.keypair:
            return 0.0, 6

        cached = self._balance_cache.get(mint)
        if cached and (time.time() - cached[2]) < 12:
            return cached[0], cached[1]

        payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenAccountsByOwner",
            "params": [
                str(self.keypair.pubkey()),
                {"mint": mint},
                {"encoding": "jsonParsed"},
            ],
        }
        # Public RPC first — saves Helius quota for copy-trade tx polling
        for rpc_url in (SOLANA_SEND_RPC_URL, HELIUS_RPC_URL):
            try:
                async with self.session.post(
                    rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=12),
                ) as resp:
                    if resp.status == 429:
                        continue
                    data = await resp.json()
                accounts = data.get("result", {}).get("value", [])
                if not accounts:
                    continue
                best_raw = 0
                best_amount = 0.0
                best_decimals = 6
                for acc in accounts:
                    info = acc["account"]["data"]["parsed"]["info"]
                    raw = int(info["tokenAmount"]["amount"])
                    if raw <= 0:
                        continue
                    dec = int(info["tokenAmount"]["decimals"])
                    ui = info["tokenAmount"]["uiAmount"]
                    amt = float(ui) if ui is not None else raw / (10**dec)
                    if raw > best_raw:
                        best_raw, best_amount, best_decimals = raw, amt, dec
                if best_raw > 0:
                    self._balance_cache[mint] = (best_amount, best_decimals, time.time())
                    return best_amount, best_decimals
            except Exception:
                continue
        return 0.0, 6

    async def get_sell_quote_usd(self, mint: str, raw_amount: int) -> float | None:
        """How much USD Jupiter would pay right now (CASH/USDC or SOL×price)."""
        try:
            quote, exit_mint = await self._get_exit_quote(mint, raw_amount, SELL_SLIPPAGE_BPS)
            if not quote or not exit_mint:
                return None
            out = int(quote.get("outAmount", 0))
            return await self._exit_value_usd(out, exit_mint) if out > 0 else None
        except Exception:
            return None

    async def get_sell_quote_sol(self, mint: str, raw_amount: int) -> float | None:
        """SOL-equivalent exit value — for multiplier checks."""
        usd = await self.get_sell_quote_usd(mint, raw_amount)
        if usd is None:
            return None
        sol_price = await self.get_sol_price_usd()
        return usd / sol_price if sol_price > 0 else None
