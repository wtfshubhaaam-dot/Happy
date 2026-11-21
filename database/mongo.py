"""
database/mongo.py - one or SEVERAL MongoDB clusters behind one tiny router.

Why: every user click writes to MongoDB (daily-limit counter, stats, auto-delete queue, ad sessions ...). On free Atlas clusters
(M0: ~100 ops/s throttle, 500 connections, 512 MB) one cluster is the first thing that breaks when 5-6 servers share it.
This module lets you spread the load over several clusters without touching the rest of the code:

    MONGO_URI    core  (YOURS)  settings, users, links, files, cluster lease + server list, transfer session ...
    MONGO_URI_2  hot            daily_usage, ad_sessions, daily_stats, pending_deletes   (high churn, expendable, TTL data)
    MONGO_URI_3  logs           vps_usage, cluster_stats                                  (statistics only)

Rules
  * Collections in CORE_ONLY can NEVER be routed away (secrets, the user list, the link table, the control plane). Somebody else's
    cluster must not hold your audience, your links or the key to your Telegram account - and a cluster you do not control must not be
    able to steer where servers send the cluster secret (cluster_nodes).
  * A secondary cluster that is unreachable is skipped (a background monitor pings it every 15 s, a failed call marks it down at once):
    its collections are served from the core cluster meanwhile, so a friend's cluster going away never takes the bot down.
  * Only the Collection methods the bot really uses are proxied. Cursors (find / aggregate) are not retried.
"""
import asyncio
import logging
import time

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import AutoReconnect, ConnectionFailure, ServerSelectionTimeoutError

import config

log = logging.getLogger(__name__)

# Never routable: secrets, audience, link table, control plane.
CORE_ONLY = frozenset({
    "daily_usage", "ad_sessions", "pending_deletes", "daily_stats",
    "settings", "users", "links", "files", "known_chats", "transfer", "transfer_sent",
    "ap_channels", "ap_posts", "cluster_nodes", "cluster_assign", "cluster_lease", "cluster_update_inbox",
})

# Default placement when MONGO_URI_2 / MONGO_URI_3 are set.
DEFAULT_ROUTES = {
    "daily_usage": "core",  # quota must never split across clusters
    "ad_sessions": "core",   # verification sessions must remain on one authority
    "daily_stats": "core",  # authoritative counters cannot safely split on failover
    "pending_deletes": "core",  # deleting files must not depend on split queues
    "vps_usage": "logs",
    "cluster_stats": "logs",
}

# where to look when a cluster is missing / down (core is always the last resort)
CHAIN = {"core": ("core",), "hot": ("hot", "core"), "logs": ("logs", "hot", "core")}

_NET_ERRORS = (ServerSelectionTimeoutError, AutoReconnect, ConnectionFailure)   # NetworkTimeout is an AutoReconnect
_ASYNC_METHODS = frozenset({
    "find_one", "insert_one", "insert_many", "update_one", "update_many", "replace_one", "delete_one", "delete_many",
    "find_one_and_update", "find_one_and_delete", "find_one_and_replace", "count_documents", "create_index", "bulk_write",
    "distinct",
})
DOWN_FOR = 45          # seconds a cluster stays "down" after a failed call (the monitor may clear it earlier)
MONITOR_EVERY = 15


class Cluster:
    def __init__(self, name: str, uri: str, db_name: str, limit_mb: float):
        self.name, self.db_name, self.limit_mb = name, db_name, limit_mb
        timeout = 15000 if name == "core" else config.MONGO_SECONDARY_TIMEOUT_MS
        self.client = AsyncIOMotorClient(
            uri,
            serverSelectionTimeoutMS=timeout,
            connectTimeoutMS=min(10000, timeout),
            socketTimeoutMS=30000,
            maxPoolSize=config.MONGO_MAX_POOL,
            minPoolSize=0,  # no permanently reserved sockets across 50+ nodes
            maxIdleTimeMS=120000,
            retryWrites=True,
            retryReads=True,
            appName=f"fsb-{config.NODE_ID or 'single'}"[:30],     # shows up in Atlas -> which server holds which connection
        )
        self.db = self.client[db_name]
        self.down_until = 0.0
        self.last_error = ""

    @property
    def up(self) -> bool:
        return self.name == "core" or time.monotonic() >= self.down_until

    def mark_down(self, err, secs: float = DOWN_FOR):
        if self.name == "core":
            return
        first = self.up
        self.down_until = time.monotonic() + secs
        self.last_error = f"{type(err).__name__}: {err}"[:160]
        if first:
            log.error("MongoDB cluster '%s' is DOWN (%s) - its collections are served from the core cluster meanwhile",
                      self.name, self.last_error)

    def mark_up(self):
        if not self.up:
            log.warning("MongoDB cluster '%s' is back", self.name)
        self.down_until = 0.0
        self.last_error = ""


# ------------------------------------------------------------------------------------------- configuration
clusters: dict = {"core": Cluster("core", config.MONGO_URI, config.DB_NAME, config.MONGO_LIMIT_MB)}
if config.MONGO_URI_2:
    clusters["hot"] = Cluster("hot", config.MONGO_URI_2, config.DB_NAME_2, config.MONGO_LIMIT_MB_2)
if config.MONGO_URI_3:
    clusters["logs"] = Cluster("logs", config.MONGO_URI_3, config.DB_NAME_3, config.MONGO_LIMIT_MB_3)
core = clusters["core"]


def _parse_routes() -> dict:
    routes = dict(DEFAULT_ROUTES)
    for part in filter(None, (p.strip() for p in config.MONGO_ROUTES.split(","))):
        name, _, grp = part.partition("=")
        name, grp = name.strip(), grp.strip().lower()
        if name in CORE_ONLY:
            log.error("MONGO_ROUTES: '%s' can only live on the core cluster - ignored", name)
        elif grp not in CHAIN:
            log.error("MONGO_ROUTES: unknown cluster group %r for '%s' (use core, hot or logs) - ignored", grp, name)
        else:
            routes[name] = grp
    return routes


ROUTES = _parse_routes()


def group_of(name: str) -> str:
    return "core" if name in CORE_ONLY else ROUTES.get(name, "core")


def chain_for(name: str) -> list:
    """Clusters that may hold `name`, best first (only configured ones, core always last)."""
    return [clusters[g] for g in CHAIN[group_of(name)] if g in clusters]


def pick(name: str) -> Cluster:
    for cl in chain_for(name):
        if cl.up:
            return cl
    return core


def routes_of(cluster_name: str) -> list:
    """Collections that currently live on this cluster (for the panel)."""
    known = set(CORE_ONLY) | set(ROUTES)
    return sorted(n for n in known if chain_for(n) and chain_for(n)[0].name == cluster_name)


class Col:
    """Looks like a Motor collection; every call goes to the cluster that holds the collection RIGHT NOW."""

    def __init__(self, name: str):
        self.name = name

    def raw(self):
        return pick(self.name).db[self.name]

    def all_raw(self) -> list:
        """The collection on every cluster that may hold it (used to create indexes everywhere)."""
        return [(cl, cl.db[self.name]) for cl in chain_for(self.name)]

    def __getattr__(self, attr):
        if attr not in _ASYNC_METHODS:
            return getattr(self.raw(), attr)        # find / aggregate / ... : a cursor, not retried

        async def call(*a, **kw):
            cl = pick(self.name)
            try:
                return await getattr(cl.db[self.name], attr)(*a, **kw)
            except _NET_ERRORS as e:
                if cl.name == "core":
                    raise
                cl.mark_down(e)
                return await getattr(pick(self.name).db[self.name], attr)(*a, **kw)   # same call on the next cluster
        return call


_cols: dict = {}


def col(name: str) -> Col:
    c = _cols.get(name)
    if c is None:
        c = _cols[name] = Col(name)
    return c


# ------------------------------------------------------------------------------------------- health monitor
async def ping_all(timeout: float = None) -> dict:
    """Ping every secondary cluster once; mark each up / down. Returns {name: error or ''}."""
    out = {}
    wait = timeout or config.MONGO_SECONDARY_TIMEOUT_MS / 1000 + 1
    for name, cl in clusters.items():
        if name == "core":
            continue
        try:
            await asyncio.wait_for(cl.db.command("ping"), wait)
            cl.mark_up()
            out[name] = ""
        except asyncio.CancelledError:
            raise
        except Exception as e:
            cl.mark_down(e)
            out[name] = f"{type(e).__name__}: {e}"[:160]
    return out


async def monitor():
    while True:
        try:
            await asyncio.sleep(MONITOR_EVERY)
            if len(clusters) > 1:
                await ping_all()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("MongoDB monitor error")
