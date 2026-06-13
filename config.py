from __future__ import annotations

import os
from dataclasses import dataclass
from dotenv import load_dotenv
from urllib.parse import unquote

load_dotenv()


def _env_token(key: str, default: str = "") -> str:
    """Decode URL-encoded tokens pasted from browsers (%2B → +, %3D → =)."""
    raw = os.getenv(key, default)
    if not raw:
        return raw
    if "%" in raw:
        return unquote(raw)
    return raw


@dataclass(frozen=True)
class TraderConfig:
    name: str
    handle: str
    address: str
    copy_amount_sol: float


# Mode — ALERTS_ONLY disables all auto-buys (Telegram signals only)
ALERTS_ONLY = os.getenv("ALERTS_ONLY", "false").lower() in ("true", "1", "yes")
AUTO_BUY = os.getenv("AUTO_BUY", "false" if ALERTS_ONLY else "true").lower() in ("true", "1", "yes")

# Copy best GMGN-vetted traders + autonomous market scanner (both run together)
ENABLE_COPY_TRADING = os.getenv(
    "ENABLE_COPY_TRADING", "false" if ALERTS_ONLY else "true",
).lower() in ("true", "1", "yes")
ENABLE_SCANNER = os.getenv("ENABLE_SCANNER", "true").lower() in ("true", "1", "yes")
COPY_BUY_SOL = 0.015  # fallback cap — actual size is wallet-based (see BUY_SIZE_*)

TRADERS: list[TraderConfig] = [
    # Tier 1 — best win rate + selective (milkybids picks)
    TraderConfig("jijo", "@gmgn", "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk", COPY_BUY_SOL),    # 84.6% WR, 79 txns/mo
    TraderConfig("Sheep", "@gmgn", "78N177fzNJpp8pG49xDv1efYcTMSzo9tPTKEA9mAVkh2", COPY_BUY_SOL),   # 89.3% WR
    # nyhrox REMOVED — drain tokens (236ziZ, 64fkyk → H9tCkQ3a7M drainer)
    # AU73 REMOVED — empty throwaway wallet, thin on-chain history, doesn't match claimed WR
    # Tier 2 — verified / still on list
    TraderConfig("flock", "@gmgn", "F1WT79Jkw3BkBDUfCbrKKo15ghZNCEjvnjxQpiCfPuRM", COPY_BUY_SOL),  # 60% WR, Solscan clean
    TraderConfig("insentos", "@gmgn", "7SDs3PjT2mswKQ7Zo4FTucn9gJdtuW4jaacPA65BseHS", COPY_BUY_SOL),  # 66.7% WR
    TraderConfig("ALJ4P5", "@gmgn", "ALJ4P5QNyHeLEjpKGmA1eUfJHSEGQMjY8HLnDkSgjczb", COPY_BUY_SOL),  # 71% WR, 46 txns
]

TRADER_BY_ADDRESS: dict[str, TraderConfig] = {t.address: t for t in TRADERS}

# Environment
WALLET_PRIVATE_KEY: str = os.getenv("WALLET_PRIVATE_KEY", "")
HELIUS_API_KEY: str = os.getenv("HELIUS_API_KEY", "")
TWITTER_BEARER_TOKEN: str = _env_token("TWITTER_BEARER_TOKEN")
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
PAPER_TRADE: bool = os.getenv("PAPER_TRADE", "true").lower() in ("true", "1", "yes")

# Solana constants
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
# Phantom Cash — Phantom's own USD stablecoin (what the app calls "Cash")
CASH_MINT = "CASHx9KJUStyftLFWGvEVf59SGeG9sh5FfcnZMVPCASH"
LAMPORTS_PER_SOL = 1_000_000_000

# API endpoints
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
SOLANA_SEND_RPC_URL = "https://api.mainnet-beta.solana.com"
HELIUS_TX_URL = "https://api-mainnet.helius-rpc.com/v0/addresses/{address}/transactions"
JUPITER_QUOTE_URL = "https://api.jup.ag/swap/v1/quote"
JUPITER_SWAP_URL = "https://api.jup.ag/swap/v1/swap"
JUPITER_PRICE_URL = "https://api.jup.ag/price/v3"
DEXSCREENER_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEXSCREENER_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
DEXSCREENER_TOP_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/top/v1"
DEXSCREENER_TOKEN_URL = "https://api.dexscreener.com/latest/dex/tokens/{mint}"
DEXSCREENER_PAIRS_URL = "https://api.dexscreener.com/latest/dex/pairs/solana/{pair}"
RUGCHECK_URL = "https://api.rugcheck.xyz/v1/tokens/{mint}/report"
TWITTER_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
TWITTER_USER_LOOKUP_URL = "https://api.twitter.com/2/users/by/username/{username}"
TWITTER_USER_TWEETS_URL = "https://api.twitter.com/2/users/{user_id}/tweets"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
TELEGRAM_SEND_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Copy-trade filters (milkybids-style: graduated tokens from vetted wallets)
COPY_MIN_TRADER_SOL = 0.3
COPY_MAX_TRADER_SOL = 20.0
COPY_MAX_MARKET_CAP_USD = 800_000
COPY_GRADUATED_ONLY = True          # only copy tokens on Raydium/Orca (left pump.fun curve)
COPY_MIN_GRADUATED_LIQUIDITY_USD = 15_000
COPY_SKIP_IF_HOLDING = True         # don't buy same token twice
COPY_REBUY_COOLDOWN_HOURS = 24     # after a losing exit, don't copy-buy same token again

# Trade budget — daily loss halt only (no max buys/day cap)
MAX_OPEN_POSITIONS     = 2        # never hold more than 2 coins at once
DAILY_LOSS_LIMIT_USD   = 5.0      # stop buying for the day after $5 net loss
# Lock in wins — stop NEW buys once daily profit hits this (sells still run)
# Set via Railway env e.g. DAILY_PROFIT_TARGET_USD=50 or 100 or 500
DAILY_PROFIT_TARGET_USD = float(os.getenv("DAILY_PROFIT_TARGET_USD", "50"))
MIN_SOL_RESERVE        = 0.04     # always keep this much SOL for gas
MIN_BUY_SOL            = 0.008    # skip trade if affordable size below this
MAX_BUY_SOL            = 0.015    # never risk more than this per trade (~$2)
BUY_SIZE_PCT_OF_WALLET = 0.08     # each buy = 8% of tradeable SOL

# Scanner settings — autonomous discovery (Axiom Pulse "graduated" style)
SCAN_INTERVAL_SECONDS  = int(os.getenv("SCAN_INTERVAL_SECONDS", "30"))
SCAN_MAX_CANDIDATES    = int(os.getenv("SCAN_MAX_CANDIDATES", "10"))
SCAN_MAX_PUMP_EVAL     = int(os.getenv("SCAN_MAX_PUMP_EVAL", "5"))
SCAN_MIN_SCORE         = int(os.getenv("SCAN_MIN_SCORE", "82"))   # raw score floor
SCAN_MIN_LIQUIDITY_USD = 18_000   # graduated pool minimum (was 15k — too thin)
SCAN_MIN_MCAP_USD      = 30_000   # skip micro-dead coins
SCAN_MAX_MCAP_USD      = 600_000  # memecoin sweet spot
SCAN_MIN_AGE_HOURS     = 1.0      # at least 1h old — skip brand-new noise
SCAN_MIN_BUY_PRESSURE  = float(os.getenv("SCAN_MIN_BUY_PRESSURE", "55"))   # % buys in 24h
SCAN_MIN_VOLUME_24H    = float(os.getenv("SCAN_MIN_VOLUME_24H", "200000"))  # $ vol gate
# GMGN trending = swap volume list (weak). GMGN signals = smart-money buys (strong).
SCAN_GMGN_TRENDING_BUY = os.getenv("SCAN_GMGN_TRENDING_BUY", "false").lower() in ("true", "1", "yes")
SCAN_GMGN_SIGNALS_BUY  = os.getenv("SCAN_GMGN_SIGNALS_BUY", "false").lower() in ("true", "1", "yes")
GMGN_SIGNAL_SCORE_BOOST = int(os.getenv("GMGN_SIGNAL_SCORE_BOOST", "5"))  # was +10 on everything
SCAN_GRADUATED_ONLY    = True     # Raydium/Orca/Meteora/PumpSwap — sellable
SCAN_REQUIRE_SELL_TEST = True     # verify Jupiter sell route BEFORE buying
SCANNER_BUY_SOL        = MAX_BUY_SOL

# Pump.fun discovery (frontend-api-v3.pump.fun — free, no key required)
SCAN_PUMPFUN_ENABLED         = os.getenv("SCAN_PUMPFUN_ENABLED", "true").lower() in ("true", "1", "yes")
SCAN_PUMPFUN_LIVE            = True   # currently-live feed
SCAN_PUMPFUN_GRADUATING      = True   # bonding curve near completion (70%+)
SCAN_PUMPFUN_GRADUATED       = True   # recently graduated to PumpSwap
SCAN_PUMPFUN_ALLOW_BONDING   = True   # allow buys on bonding curve if Jupiter can sell
SCAN_PUMPFUN_BONDING_MIN_PCT = float(os.getenv("SCAN_PUMPFUN_BONDING_MIN_PCT", "70"))
SCAN_PUMPFUN_MIN_USD_MCAP    = 8_000   # min ~$8k on pump curve
SCAN_PUMPFUN_MAX_AGE_HOURS   = 6.0    # only fresh pump launches
PUMP_INITIAL_VIRTUAL_SOL     = 30.0   # pump.fun curve starts ~30 virtual SOL
PUMP_BONDING_SOL_TARGET      = 85.0   # graduation threshold (~85 SOL)

DEXSCREENER_MIN_INTERVAL_SEC = float(os.getenv("DEXSCREENER_MIN_INTERVAL_SEC", "1.5"))
DEXSCREENER_429_BACKOFF_SEC = float(os.getenv("DEXSCREENER_429_BACKOFF_SEC", "90"))

# Wallet tracker — sequential poll; lower rate = fewer Helius 429s
WALLET_POLL_INTERVAL_SECONDS = int(os.getenv("WALLET_POLL_INTERVAL_SECONDS", "25"))
WALLET_POLL_GAP_SECONDS = float(os.getenv("WALLET_POLL_GAP_SECONDS", "2.0"))
HELIUS_MIN_INTERVAL_SEC = float(os.getenv("HELIUS_MIN_INTERVAL_SEC", "3.0"))

# Risk manager settings
RISK_POLL_INTERVAL_SECONDS = 5
TP1_MULTIPLIER = 1.5      # sell 25% at 1.5x — bank profit before dump
TP2_MULTIPLIER = 3.0      # sell 25% at 3x
TP3_MULTIPLIER = 10.0    # sell 25% at 10x
TP1_SELL_PCT = 25.0
TP2_SELL_PCT = 25.0
TP3_SELL_PCT = 25.0       # keeps 25% riding with trailing stop for 100x+
STOP_LOSS_PCT = -20.0     # tighter stop — cut losses faster
TRAILING_STOP_PCT = -15.0 # tighter trailing — protect gains after big pump
TRAILING_ACTIVATION_MULTIPLIER = 3.0
TIME_STOP_MINUTES = 15        # flat for 15 min → sell
TIME_STOP_MIN_MULTIPLIER = 1.1  # needs 1.1x in 15 min or exit
MAX_HOLD_MINUTES = 45         # never hold longer than 45 min — force sell

# HERMES Meme Council — 7 rule-based agents (zero LLM credits)
USE_MEME_COUNCIL = os.getenv("USE_MEME_COUNCIL", "true").lower() in ("true", "1", "yes")
MEME_COUNCIL_MIN = int(os.getenv("MEME_COUNCIL_MIN", "5"))  # 5/7 — no weak abstain buys
COPY_COUNCIL_MIN = int(os.getenv("COPY_COUNCIL_MIN", "4"))  # copy trades: speed priority
COPY_USE_COUNCIL = os.getenv("COPY_USE_COUNCIL", "true").lower() in ("true", "1", "yes")
TWITTER_USE_COUNCIL = os.getenv("TWITTER_USE_COUNCIL", "true").lower() in ("true", "1", "yes")
WHALE_MAX_TOP1_PCT = float(os.getenv("WHALE_MAX_TOP1_PCT", "25"))
WHALE_MAX_TOP5_PCT = float(os.getenv("WHALE_MAX_TOP5_PCT", "55"))

# Optional extra scan feeds (leave blank to skip)
DEXTOOLS_API_KEY: str = os.getenv("DEXTOOLS_API_KEY", "")
AXIOM_AUTH_TOKEN: str = os.getenv("AXIOM_AUTH_TOKEN", "")

# Twitter / X caller tracker (alerts + hit-rate stats like alpha groups)
TWITTER_TRACKER_ENABLED = os.getenv("ENABLE_TWITTER_TRACKER", "false").lower() in ("true", "1", "yes")
TWITTER_POLL_SECONDS = int(os.getenv("TWITTER_POLL_SECONDS", "90"))
TWITTER_KEYWORD_SEARCH = os.getenv("TWITTER_KEYWORD_SEARCH", "true").lower() in ("true", "1", "yes")
TWITTER_AUTO_BUY = os.getenv("TWITTER_AUTO_BUY", "false").lower() in ("true", "1", "yes")
TWITTER_STATS_DAYS = int(os.getenv("TWITTER_STATS_DAYS", "30"))
TWITTER_STATS_INTERVAL_HOURS = int(os.getenv("TWITTER_STATS_INTERVAL_HOURS", "24"))

def _parse_twitter_callers(raw: str) -> list[str]:
    """Accounts to watch — set TWITTER_CALLERS on Railway (comma-separated @handles)."""
    if not raw.strip():
        return []
    return [h.strip().lstrip("@") for h in raw.split(",") if h.strip()]

TWITTER_CALLERS: list[str] = _parse_twitter_callers(os.getenv("TWITTER_CALLERS", ""))

# One-time: set RESET_TRADE_STATS=true, redeploy once, then remove or set false
RESET_TRADE_STATS = os.getenv("RESET_TRADE_STATS", "false").lower() in ("true", "1", "yes")

# General
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 1.5
JUPITER_MIN_INTERVAL_SEC = float(os.getenv("JUPITER_MIN_INTERVAL_SEC", "0.8"))
JUPITER_429_RETRIES = 5              # extra retries when rate-limited
BUY_MINT_COOLDOWN_SEC = int(os.getenv("BUY_MINT_COOLDOWN_SEC", "180"))
DEFAULT_SLIPPAGE_BPS = int(os.getenv("DEFAULT_SLIPPAGE_BPS", "300"))
BUY_SLIPPAGE_BPS = int(os.getenv("BUY_SLIPPAGE_BPS", "500"))          # 5% — faster fills
COPY_BUY_SLIPPAGE_BPS = int(os.getenv("COPY_BUY_SLIPPAGE_BPS", "800"))  # 8% — copy speed
BUY_PRIORITY_FEE_LAMPORTS = int(os.getenv("BUY_PRIORITY_FEE_LAMPORTS", "300000"))
SELL_SLIPPAGE_BPS = 1000          # 10% slippage on sells — meme coins move fast
SELL_SLIPPAGE_RETRY_BPS = [1000, 2500, 5000, 10000]
SELL_PRIORITY_FEE_LAMPORTS = 300_000

# All sells swap to USDC (dollars) — falls back to SOL only if no USDC route
SELL_TO_STABLE = True
EXIT_MINTS: list[str] = [USDC_MINT] if SELL_TO_STABLE else [SOL_MINT]
EXIT_DECIMALS: dict[str, int] = {USDC_MINT: 6, SOL_MINT: 9}
EXIT_LABELS: dict[str, str] = {
    USDC_MINT: "USDC",
    SOL_MINT: "SOL",
}
EXIT_MINT = EXIT_MINTS[0]
EXIT_LABEL = EXIT_LABELS[EXIT_MINT]
MIN_SELL_VALUE_USD = 0.50   # skip dust sells that spam alerts and waste fees
DUST_BALANCE_USD = 0.25     # treat tiny leftover as sold — stop retry loop
