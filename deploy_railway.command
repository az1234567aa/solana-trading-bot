#!/bin/bash
# One-click Railway deploy — positions saved in PostgreSQL survive restarts.
set -e
cd "$(dirname "$0")"

echo "=========================================="
echo "  Solana Bot → Railway (24/7 live)"
echo "  Positions persist in PostgreSQL"
echo "=========================================="

if [ ! -f .env ]; then
  echo "ERROR: .env not found. Copy .env.example → .env and fill in keys."
  exit 1
fi

# Stop local bot so Mac + Railway don't double-buy
pkill -f "solana-trading-bot.*main.py" 2>/dev/null || true

if ! command -v railway &>/dev/null; then
  echo "Installing Railway CLI..."
  npm install -g @railway/cli
fi

if ! railway whoami &>/dev/null; then
  echo ""
  echo "→ Log in to Railway in the browser window that opens..."
  railway login
fi

echo ""
echo "→ Link this folder to your Railway project/service."
echo "  Pick: existing project OR create new → solana-trading-bot service"
railway link

echo ""
echo "→ Push env vars from .env (secrets stay local, not committed)..."
while IFS= read -r line || [ -n "$line" ]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [ -z "$line" ] && continue
  key="${line%%=*}"
  val="${line#*=}"
  [ -z "$key" ] && continue
  # DATABASE_URL comes from Postgres plugin — set reference below
  if [ "$key" = "DATABASE_URL" ]; then
    continue
  fi
  railway variables --set "${key}=${val}" --skip-deploys 2>/dev/null || railway variables set "${key}=${val}" 2>/dev/null || true
done < .env

echo ""
echo "→ Linking DATABASE_URL to PostgreSQL..."
echo "  (Add Postgres in Railway dashboard first: + New → Database → PostgreSQL)"
railway variables --set 'DATABASE_URL=${{Postgres.DATABASE_URL}}' 2>/dev/null \
  || railway variables set 'DATABASE_URL=${{Postgres.DATABASE_URL}}' 2>/dev/null \
  || echo "  ⚠ Set DATABASE_URL manually: Variables → DATABASE_URL = \${{Postgres.DATABASE_URL}}"

echo ""
echo "→ Deploying..."
railway up --detach 2>/dev/null || railway up

echo ""
echo "=========================================="
echo "  Done! Check:"
echo "  1. Railway → Deployments → Active"
echo "  2. Railway → Logs → 'PostgreSQL position store ready'"
echo "  3. Telegram → 'Resumed monitoring X open position(s)'"
echo "=========================================="
