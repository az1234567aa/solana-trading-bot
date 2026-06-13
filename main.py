from __future__ import annotations

import asyncio
import logging
import os
import signal

import aiohttp

import config
from config import AUTO_BUY, ENABLE_SCANNER
from modules.alerter import Alerter
from modules.coin_scanner import CoinScanner
from modules.executor import Executor
from modules.risk_manager import RiskManager
from modules.utils import setup_logging
from modules.wallet_tracker import WalletTracker
from modules.twitter_tracker import TwitterTracker

logger = logging.getLogger("solana-bot")


async def main() -> None:
    setup_logging()

    mode = "PAPER TRADE" if config.PAPER_TRADE else "LIVE TRADING"
    if config.ALERTS_ONLY or not AUTO_BUY:
        mode += " · ALERTS ONLY (no auto-buy)"
    logger.info("=" * 60)
    logger.info("Solana Memecoin Trading Bot starting — %s", mode)
    if config.ENABLE_COPY_TRADING and config.TRADERS and AUTO_BUY:
        logger.info("Copy trading ON — %d wallets", len(config.TRADERS))
    elif config.ENABLE_COPY_TRADING and not AUTO_BUY:
        logger.info("Copy trading OFF — AUTO_BUY disabled")
    else:
        logger.info("Copy trading OFF — scanner + Twitter only")
    logger.info(
        "Scanner: %s | HERMES council: %s | Auto-buy: %s",
        "ON" if ENABLE_SCANNER else "OFF",
        "ON" if config.USE_MEME_COUNCIL else "OFF",
        "ON" if AUTO_BUY else "OFF (alerts only)",
    )
    logger.info("Scanner interval: %ds", config.SCAN_INTERVAL_SECONDS)
    if os.getenv("RAILWAY_ENVIRONMENT") and not os.getenv("DATABASE_URL"):
        logger.error(
            "DATABASE_URL missing on Railway — add PostgreSQL or positions reset on every deploy"
        )
    logger.info("=" * 60)

    if not config.HELIUS_API_KEY or config.HELIUS_API_KEY.startswith("your_"):
        logger.warning("HELIUS_API_KEY not set — wallet tracking will fail")
    birdeye = "ON" if config.BIRDEYE_API_KEY and not config.BIRDEYE_API_KEY.startswith("your_") else "OFF"
    logger.info("Birdeye scanner data: %s", birdeye)

    twitter_on = config.TWITTER_TRACKER_ENABLED and config.TWITTER_BEARER_TOKEN
    logger.info(
        "Twitter tracker: %s (%d callers)",
        "ON" if twitter_on else "OFF",
        len(config.TWITTER_CALLERS) if twitter_on else 0,
    )

    # Force Google DNS — Railway's default DNS can't resolve jup.ag domains
    connector = aiohttp.TCPConnector(
        resolver=aiohttp.AsyncResolver(nameservers=["8.8.8.8", "1.1.1.1"]),
        ttl_dns_cache=300,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        alerter = Alerter(session)
        executor = Executor(session)
        risk_manager = RiskManager(executor=executor, alerter=alerter)
        await risk_manager.initialize()   # connects to PostgreSQL, loads open positions
        executor.risk_manager = risk_manager

        coin_scanner = CoinScanner(session, executor) if ENABLE_SCANNER else None
        wallet_tracker = (
            WalletTracker(session, executor)
            if config.ENABLE_COPY_TRADING and config.TRADERS and AUTO_BUY else None
        )
        twitter_tracker = TwitterTracker(session, alerter, executor)

        await alerter.send_startup_message(
            lifetime_pnl=risk_manager.stats.lifetime_pnl_usd,
            lifetime_trades=risk_manager.stats.lifetime_trades,
            open_positions=[
                (p.symbol, p.mint)
                for p in risk_manager.positions.values()
                if not p.closed
            ],
            persistence=(
                "PostgreSQL ✅"
                if risk_manager.store._pool is not None
                else "local file (add DATABASE_URL on Railway)"
            ),
        )
        logger.info("Wallet: %s", executor.public_key)

        shutdown_event = asyncio.Event()

        def _handle_signal() -> None:
            logger.info("Shutdown signal received — stopping bot...")
            shutdown_event.set()
            if wallet_tracker:
                wallet_tracker.stop()
            if coin_scanner:
                coin_scanner.stop()
            twitter_tracker.stop()
            risk_manager.stop()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        async def _run_until_shutdown() -> None:
            await shutdown_event.wait()

        logger.info("All modules running concurrently via asyncio.gather()")

        tasks = [risk_manager.run(), twitter_tracker.run(), _run_until_shutdown()]
        if coin_scanner:
            tasks.insert(0, coin_scanner.run())
        if wallet_tracker:
            tasks.insert(0, wallet_tracker.run())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error("Module %d raised: %s", i, result)

        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
