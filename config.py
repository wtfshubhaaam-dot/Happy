"""
config.py
Only the bare minimum lives in environment variables. Everything else
(channels, captions, timers, admins...) is configured from the Telegram admin
panel and stored in MongoDB.
"""
import os
import re
from dotenv import load_dotenv

load_dotenv()


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


BOT_TOKEN = _required("BOT_TOKEN")
MONGO_URI = _required("MONGO_URI")
OWNER_ID = int(_required("OWNER_ID"))

# Kept for Pyrogram compatibility; python-telegram-bot does not need them.
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "")

# Public https URL of this deployment, no trailing slash (e.g. https://myapp.koyeb.app). Needed for Stream/Download links.
def _auto_base_url() -> str:
    """Render and Koyeb tell the app its own public address; used only when BASE_URL is not set."""
    if os.getenv("RENDER_EXTERNAL_URL"):
        return os.getenv("RENDER_EXTERNAL_URL", "").strip()
    if os.getenv("KOYEB_PUBLIC_DOMAIN"):
        return "https://" + os.getenv("KOYEB_PUBLIC_DOMAIN", "").strip()
    return ""


BASE_URL = (os.getenv("BASE_URL", "").strip() or _auto_base_url()).rstrip("/")

# ONE public domain for ALL servers (e.g. https://hubpa.com). Share links and Stream/Download links are written with it,
# a Cloudflare Worker (cloudflare-worker/hub-router.js) forwards them to a healthy server. Empty = every server uses its own BASE_URL.
PUBLIC_URL = os.getenv("PUBLIC_URL", "").strip().rstrip("/")

# Cloudflare Worker that makes every share link permanent (see cloudflare-worker/worker.js). Optional.
WORKER_URL = os.getenv("WORKER_URL", "").strip().rstrip("/")
WORKER_SECRET = os.getenv("WORKER_SECRET", "").strip()

# Invisible padding so message bubbles are as wide as their inline buttons (0 = off). See utils/padding.py
PAD_WIDTH = int(os.getenv("PAD_WIDTH", "34") or 0)

DB_NAME = os.getenv("DB_NAME", "filesharebot")
PORT = int(os.getenv("PORT", "8080"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Storage quota of your MongoDB plan in MB (Atlas free M0 = 512) - used for the "storage left" bar in Stats
MONGO_LIMIT_MB = float(os.getenv("MONGO_LIMIT_MB", "512"))

# ---------------------------------------------------------------------------------------------------
# SEVERAL MongoDB clusters (optional, see database/mongo.py). MONGO_URI above is the CORE cluster = YOURS: settings, users,
# links, files, cluster lease / server list, the saved user-account session ... never leave it (hard-coded in mongo.CORE_ONLY).
#   MONGO_URI_2  "hot"   cluster: per-click churn (daily limit counters, ad sessions, daily stats, auto-delete queue)
#   MONGO_URI_3  "logs"  cluster: VPS usage + cluster statistics (falls back to "hot", then to core)
# A cluster that is unreachable is skipped automatically (its collections are served from the core cluster meanwhile).
# ---------------------------------------------------------------------------------------------------
MONGO_URI_2 = os.getenv("MONGO_URI_2", "").strip()
MONGO_URI_3 = os.getenv("MONGO_URI_3", "").strip()
DB_NAME_2 = os.getenv("DB_NAME_2", "").strip() or os.getenv("DB_NAME", "filesharebot")
DB_NAME_3 = os.getenv("DB_NAME_3", "").strip() or os.getenv("DB_NAME", "filesharebot")
MONGO_LIMIT_MB_2 = float(os.getenv("MONGO_LIMIT_MB_2", "") or MONGO_LIMIT_MB)
MONGO_LIMIT_MB_3 = float(os.getenv("MONGO_LIMIT_MB_3", "") or MONGO_LIMIT_MB)
# Optional override of which collection lives on which cluster, e.g.  MONGO_ROUTES=daily_usage=hot,ad_sessions=hot,vps_usage=logs
MONGO_ROUTES = os.getenv("MONGO_ROUTES", "").strip()


# ---------------------------------------------------------------------------------------------------
# Multi-server mode (several free Koyeb accounts behind ONE bot). Leave CLUSTER_SECRET empty = single server.
# Every server runs the same code + the same MONGO_URI / BOT_TOKEN. Only these differ per server:
#   NODE_ID    short unique name, e.g. n1, n2, n3
#   NODE_ROLE  "gateway" on ONE or TWO servers (only the one holding the Mongo lease receives Telegram's webhook and routes
#              users; the other gateway is a hot standby that takes over automatically), "worker" on the others
#   BASE_URL   that server's own public https URL
# ---------------------------------------------------------------------------------------------------
CLUSTER_SECRET = os.getenv("CLUSTER_SECRET", "").strip()                       # same long random text on every server
NODE_ID = re.sub(r"[^A-Za-z0-9_-]", "", os.getenv("NODE_ID", "").strip())[:20]
NODE_NAME = os.getenv("NODE_NAME", "").strip()[:24]                              # e.g. "🔥 Phoenix" (default: picked from NODE_ID)
NODE_ROLE = os.getenv("NODE_ROLE", "worker").strip().lower()                     # gateway (eligible to lead; 2 = active + standby) | worker
NODE_CAPACITY = min(10, max(1, int(os.getenv("NODE_CAPACITY", "10") or 10)))              # active users this server should carry
NODE_MAX_TASKS = min(10, max(1, int(os.getenv("NODE_MAX_TASKS", "10") or 10)))            # requests being processed at the same moment
ACTIVE_WINDOW = max(30, int(os.getenv("ACTIVE_WINDOW", "300") or 300))           # seconds without a request = user is inactive

# ---- hardening (all optional) ----
if CLUSTER_SECRET and len(CLUSTER_SECRET) < 24:
    raise RuntimeError("CLUSTER_SECRET is too short (min 24 characters). Generate one:  openssl rand -hex 32")


def _int_env(name: str, default: int) -> int:
    try:
        return max(0, int(os.getenv(name, "") or default))
    except ValueError:
        return default


# HARD caps for concurrent Telegram update processing.
# 0 = automatic: 4x the soft number in multi-server mode, unlimited on a single server.
NODE_HARD_TASKS = min(10, max(1, _int_env("NODE_HARD_TASKS", 0) or NODE_MAX_TASKS)) if CLUSTER_SECRET and NODE_ID else 0
LINK_RATE_PER_10S = _int_env("LINK_RATE_PER_10S", 60)
# Origin lock: same random text in the Cloudflare Worker (ORIGIN_SECRET) and on every server. Only active together with PUBLIC_URL,
# then /<token> answers ONLY requests that came through the Worker (needs DL_MODE=proxy).
ORIGIN_SECRET = os.getenv("ORIGIN_SECRET", "").strip()
# On shutdown a server stops taking new updates and waits this long for the running ones to finish.
DRAIN_SECONDS = _int_env("DRAIN_SECONDS", 12)
# Connections this server may open to EACH MongoDB cluster. Atlas free/shared tiers allow only 500 connections IN TOTAL, shared by
# every server (and by the friend's own apps on his clusters): 6 servers x 50 would already eat 300. Single server keeps 50.
# Bound pools in cluster mode. 50 nodes x 5 = up to 250 pooled connections
# per cluster, excluding monitor/other application connections.
MONGO_MAX_POOL = min(5, max(1, _int_env("MONGO_MAX_POOL", 5))) if CLUSTER_SECRET and NODE_ID else max(1, _int_env("MONGO_MAX_POOL", 20))
# How long a call to a SECONDARY cluster may wait before that cluster is declared down (core keeps the long 15 s timeout).
MONGO_SECONDARY_TIMEOUT_MS = max(1000, _int_env("MONGO_SECONDARY_TIMEOUT_MS", 4000))
# Old servers (before HMAC signing) send the raw cluster secret. Accept that ONLY while rolling out the update: CLUSTER_ALLOW_LEGACY=1,
# then remove it again once every server runs the new code.
CLUSTER_ALLOW_LEGACY = os.getenv("CLUSTER_ALLOW_LEGACY", "").strip() in ("1", "true", "yes")
