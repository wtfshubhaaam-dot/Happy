#!/usr/bin/env bash
# One-command deploy on a fresh Ubuntu/Debian VPS:   bash deploy.sh
# Re-run it any time to update (rebuilds and restarts only what changed).
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  cp .env.example .env
  echo "👉 .env created. Fill BOT_TOKEN, OWNER_ID, MONGO_URI, API_ID, API_HASH and DOMAIN, then run: bash deploy.sh"
  exit 1
fi

for v in BOT_TOKEN OWNER_ID MONGO_URI DOMAIN; do
  val="$(grep -E "^${v}=" .env | head -1 | cut -d= -f2- || true)"
  if [ -z "$val" ] || echo "$val" | grep -qE "your-|example\.com|123456789|ABC-your"; then
    echo "❌ ${v} in .env is still the example value. Edit .env first."; exit 1
  fi
done

# Multi-server mode (only when CLUSTER_SECRET is set): catch the usual mistakes before the container starts.
getv() { grep -E "^$1=" .env | head -1 | cut -d= -f2- || true; }
cs="$(getv CLUSTER_SECRET)"
if [ -n "$cs" ]; then
  [ "${#cs}" -ge 24 ] || { echo "❌ CLUSTER_SECRET must be at least 24 characters. Generate one:  openssl rand -hex 32"; exit 1; }
  [ -n "$(getv NODE_ID)" ] || { echo "❌ NODE_ID is empty (every server needs its own: n1, n2, ...)."; exit 1; }
  case "$(getv NODE_ROLE)" in gateway|worker) ;; *) echo "❌ NODE_ROLE must be gateway or worker."; exit 1;; esac
  echo "ℹ️  Multi-server: node $(getv NODE_ID) as $(getv NODE_ROLE). Use NODE_ROLE=gateway on TWO servers (active + standby)."
fi

SUDO=""; [ "$(id -u)" -ne 0 ] && SUDO="sudo"
if ! command -v docker >/dev/null 2>&1; then
  echo "📦 Installing Docker..."
  curl -fsSL https://get.docker.com | $SUDO sh
fi

DC="$SUDO docker compose"
$SUDO docker compose version >/dev/null 2>&1 || DC="$SUDO docker-compose"

$DC up -d --build --remove-orphans
echo
$DC ps
echo
echo "✅ Done. Logs:  $DC logs -f bot"
echo "   Health:  https://$(grep -E '^DOMAIN=' .env | cut -d= -f2-)/health"
