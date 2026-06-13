"""On-chain tx helpers — distinguish real swaps from paper/simulated trades."""
from __future__ import annotations


def is_on_chain_tx(tx_signature: str | None) -> bool:
    """True only for signatures from a confirmed Solana transaction."""
    if not tx_signature:
        return False
    sig = tx_signature.strip()
    if not sig or sig.startswith("PAPER_"):
        return False
    if sig in ("PAPER_WALLET", "None"):
        return False
    return len(sig) >= 80


def solscan_tx_link(tx_signature: str) -> str:
    return f"https://solscan.io/tx/{tx_signature}"


def solscan_account_link(address: str) -> str:
    return f"https://solscan.io/account/{address}"
