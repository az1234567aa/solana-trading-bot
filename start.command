#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate 2>/dev/null || python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt -q
echo "=========================================="
echo "  Solana Bot — LIVE AUTO-BUY"
echo "  Copy + Scanner + HERMES 7 agents"
echo "  Ctrl+C to stop"
echo "=========================================="
python3 main.py
