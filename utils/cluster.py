"""
utils/cluster.py - ONE bot, MANY servers (e.g. several free Koyeb accounts) sharing ONE MongoDB.

How it works
  * Telegram can send updates to only one place, so one server is the GATEWAY (NODE_ROLE=gateway).
    Telegram -> gateway (/tg/<secret>) -> the gateway looks at the user and forwards the update to the
    least-loaded server (/internal/update). That server handles it completely (checks, files, Stream/Download
    links on ITS OWN domain, ads), so file traffic is spread over all servers.
  * Every server writes a heartbeat to MongoDB every 10 s: active users, running tasks, streams, CPU, RAM.
  * A user is pinned to a server while he is active (ACTIVE_WINDOW seconds without a request = inactive).
    A new / returning user goes to the server with the lowest load score:
        40 % active users vs capacity + 20 % running tasks + 15 % streams + 15 % CPU + 10 % RAM
    Full or overheated servers are skipped while another one is free. If ALL are full, the least loaded one still
    takes the user (an "overflow" is counted and the owner is told to add a server).
  * A dead server (no heartbeat / forward failed) is skipped; its users move to another server automatically.
  * Admins, channel posts and chat-member updates always run on the gateway (the admin panel keeps its state there).
  * Background jobs (auto-delete, autoposter, backup) run on the gateway only.
  * GATEWAY FAILOVER: set NODE_ROLE=gateway on TWO servers. They compete for a lease in MongoDB (cluster_lease, Mongo server
    time, 30 s, renewed every 10 s). The winner is the active gateway (sets the Telegram webhook, routes, runs the jobs); the
    other one is a hot standby that works as a normal worker and takes over within ~40 s (a few seconds after a clean deploy).
    Telegram keeps undelivered updates meanwhile (the standby answers 503, Telegram retries).
  * The gateway answers Telegram only AFTER the chosen server accepted the update, and a stopping server drains its running
    updates (DRAIN_SECONDS) - a normal redeploy loses nothing. A hard crash can still lose the updates being processed right then.
  * Heartbeats are compared with MongoDB's clock, not the servers' own clocks (no false DOWN from clock skew).
  * The gateway samples the cluster every 15 s -> hourly peak stats in MongoDB (shown in Panel -> Servers).

Add a server later: deploy the same code on a new Koyeb account with a new NODE_ID, NODE_ROLE=worker and its own
BASE_URL. It joins automatically after its first heartbeat; nothing else has to be changed or restarted.
"""
import asyncio
import datetime as dt
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import math
import os
import re
import time
import uuid
from collections import deque
from urllib.parse import urlparse

import aiohttp
from aiohttp import web
from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError
from telegram import Update

import config
from database import db as dbm

log = logging.getLogger(__name__)

ENABLED = bool(config.CLUSTER_SECRET and config.NODE_ID)
CAN_GATEWAY = ENABLED and config.NODE_ROLE == "gateway"   # eligible to lead (the lease decides who really does)
IS_GATEWAY = False                                         # True only on the server that holds the lease RIGHT NOW
INSTANCE_ID = uuid.uuid4().hex[:12]                        # one per process: a rolling deploy never mixes old and new instance
OWNER = f"{config.NODE_ID}:{INSTANCE_ID}"
EPOCH = dt.datetime(1970, 1, 1)
LEASE = 30             # seconds the gateway lease is valid (Mongo server time)
LEASE_EVERY = 10       # renew / try to take over this often
LEASE_MARGIN = 8       # a gateway that could not renew steps down this long BEFORE its lease can expire (no split brain)
WINDOW = config.ACTIVE_WINDOW
HB_EVERY = 10          # seconds between heartbeats
STALE_AFTER = 35       # seconds without heartbeat -> server counts as down
REFRESH_EVERY = 5      # gateway re-reads the server list this often
PING_EVERY = 240       # gateway pings every server (keeps free Koyeb instances awake, wakes sleeping ones)
SAMPLE_EVERY = 15      # cluster sampling for the peak statistics
FLUSH_EVERY = 60
PERSIST_EVERY = 30     # user -> server pins are saved to MongoDB (survive a gateway restart)
DOWN_FOR = 30          # a server that failed a forward is skipped for this long
KEEP_DAYS = 400
ALLOWED_UPDATES = ["message", "callback_query", "channel_post", "my_chat_member"]

_app = None            # the telegram Application (set by attach())
_tasks: set = set()
_session = None
_sem = None
_seen_ids: deque = deque(maxlen=4000)
_seen_set: set = set()
_processing_ids: set = set()
_admitted_ids: set = set()  # reserved updates not yet executing
_active_user_slots: dict = {}  # user ID -> number of admitted in-flight updates
_draining = False      # shutting down: take no new updates
_deadline = 0.0        # monotonic time until which OUR gateway lease is surely valid
_hooks: dict = {}      # {"promote": fn, "demote": fn} set by main.py (start / stop the gateway-only background jobs)
_gw_tasks: list = []   # gateway-only tasks (router, stats, webhook) - cancelled when leadership is lost
_bg: list = []         # heartbeat + lease loop
_lease_ready = False   # the lease record exists in MongoDB


# ----------------------------------------------------------------------------- server names
_NAMES = [
    ('👑', 'Royal Phantom'),
    ('🔥', 'Inferno King'),
    ('🐉', 'Shadow Dragon'),
    ('⚡', 'Thunder Beast'),
    ('💎', 'Diamond Viper'),
    ('🌌', 'Cosmic Emperor'),
    ('🖤', 'Dark Venom'),
    ('🦅', 'Sky Predator'),
    ('🔱', 'Trident X'),
    ('☠️', 'Death Reaper'),
    ('🌑', 'Midnight Wolf'),
    ('🦁', 'Golden Lion'),
    ('🚀', 'Hyper Nova'),
    ('🥷', 'Silent Ninja'),
    ('💀', 'Ghost Rider'),
    ('🔥', 'Crimson Fury'),
    ('⚔️', 'Blade Titan'),
    ('🐺', 'Alpha Hunter'),
    ('🌪️', 'Storm Breaker'),
    ('👾', 'Cyber Demon'),
    ('🪐', 'Saturn Lord'),
    ('❄️', 'Frost Phantom'),
    ('🦈', 'Ocean Predator'),
    ('🛸', 'Alien X'),
    ('🌋', 'Lava Emperor'),
    ('🦇', 'Night Hunter'),
    ('💠', 'Neon Samurai'),
    ('⚡', 'Voltage King'),
    ('🐍', 'Toxic Cobra'),
    ('🌠', 'Galaxy Rider'),
    ('🔮', 'Mystic Shadow'),
    ('🛡️', 'Iron Guardian'),
    ('🐲', 'Dragon Emperor'),
    ('🌊', 'Tsunami X'),
    ('🧊', 'Ice Vortex'),
    ('🔥', 'Phoenix Rising'),
    ('🦂', 'Venom Scorpion'),
    ('🚀', 'Rocket Titan'),
    ('👑', 'Supreme Overlord'),
    ('🌌', 'Dark Nebula'),
    ('🦅', 'Falcon Strike'),
    ('⚔️', 'Shadow Samurai'),
    ('💎', 'Crystal Knight'),
    ('🌪️', 'Cyclone Fury'),
    ('🐆', 'Black Panther'),
    ('🔱', 'Poseidon X'),
    ('🌑', 'Eclipse Hunter'),
    ('⚡', 'Quantum Flash'),
    ('🐉', 'Eternal Dragon'),
    ('👑', 'Infinity Emperor'),
]


def _default_name(node_id: str) -> str:
    """Stable display label for n1..n50; fallback for custom node IDs."""
    m = re.fullmatch(r"n([1-9][0-9]*)", node_id or "", re.IGNORECASE)
    if m and 1 <= int(m.group(1)) <= len(_NAMES):
        emoji, name = _NAMES[int(m.group(1)) - 1]
        return f"{emoji} {name}"
    return f"🖥️ Node {node_id or 'unknown'}"


NODE_LABEL = config.NODE_NAME or _default_name(config.NODE_ID)       # shown to users, e.g. "⚡ Falcon"
HOST = "Render" if os.getenv("RENDER") else ("Koyeb" if os.getenv("KOYEB_APP_NAME") else "")


def short_name(label: str) -> str:
    """Name without the emoji, for tables."""
    return re.sub(r"^[^\w]+", "", label or "").strip() or label


def _derive(label: str) -> str:
    return hashlib.sha256(f"{label}:{config.BOT_TOKEN}:{config.CLUSTER_SECRET}".encode()).hexdigest()[:48]


WEBHOOK_SECRET = _derive("tg")
_AUTH_KEY = _derive("cluster-auth").encode()
SIG_SKEW = 60          # seconds a signed server-to-server request stays valid


def _sig(ts: str, target: str, body: bytes) -> str:
    """HMAC over timestamp + the RECEIVER's node id + the body. The raw cluster secret never leaves a server, so a wrong / hijacked
    URL in the server list (it lives in MongoDB) can neither steal it nor replay the request on another node."""
    return hmac.new(_AUTH_KEY, ts.encode() + b"." + target.encode() + b"." + body, hashlib.sha256).hexdigest()


def _signed_ok(headers, body: bytes) -> bool:
    ts, got = headers.get("X-Cluster-Ts", ""), headers.get("X-Cluster-Sig", "")
    try:
        if abs(time.time() - int(ts)) > SIG_SKEW:
            return False
    except ValueError:
        return False
    return _same(got, _sig(ts, config.NODE_ID, body))


class _Seen:
    """Remember the last N keys (update ids)."""

    def __init__(self, n: int):
        self.q, self.s = deque(maxlen=n), set()

    def hit(self, key) -> bool:
        """True = seen before; otherwise remember it."""
        if key is None:
            return False
        if key in self.s:
            return True
        if len(self.q) == self.q.maxlen:
            self.s.discard(self.q[0])
        self.q.append(key)
        self.s.add(key)
        return False


_gw_seen = _Seen(4000)   # acknowledged handoffs only; not a durable queue
_gw_inflight: set = set()  # concurrent Telegram retries must not be forwarded twice
_local_inflight: set = set()  # updates accepted by this worker but not yet completed


class _Live:
    tasks = 0           # updates being processed on THIS server right now
    tasks_peak = 0
    reserved = 0  # accepted updates waiting to execute, not just running tasks


live = _Live()


def attach(app):
    global _app
    _app = app


def _spawn(coro):
    t = asyncio.ensure_future(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)
    return t


# ----------------------------------------------------------------------------- local metrics
def _read_cgroup_mem():
    """(used_mb, limit_mb) of the container, or None. Works with cgroup v2 and v1."""
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            raw = f.read().strip()
        if raw != "max":
            limit = int(raw)
            with open("/sys/fs/cgroup/memory.current") as f:
                cur = int(f.read())
            inactive = 0
            try:
                with open("/sys/fs/cgroup/memory.stat") as f:
                    for line in f:
                        if line.startswith("inactive_file "):
                            inactive = int(line.split()[1])
                            break
            except OSError:
                pass
            return max(cur - inactive, 0) / 1048576, limit / 1048576
    except (OSError, ValueError):
        pass
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            limit = int(f.read())
        if limit < (1 << 50):
            with open("/sys/fs/cgroup/memory/memory.usage_in_bytes") as f:
                cur = int(f.read())
            return cur / 1048576, limit / 1048576
    except (OSError, ValueError):
        pass
    return None


_prev = {"t": time.time(), "p": time.process_time()}
_mcache = {"t": 0.0, "v": {}}


def local_metrics() -> dict:
    """CPU / RAM / tasks / streams of THIS server (cached for 4 s, so several callers share one reading)."""
    now = time.time()
    if now - _mcache["t"] < 4 and _mcache["v"]:
        v = dict(_mcache["v"])
        v["tasks"], v["streams"] = live.tasks, _streams()
        return v
    from utils import usage
    p = time.process_time()
    proc_cpu = max(0.0, min(100.0, 100.0 * (p - _prev["p"]) / max(now - _prev["t"], 1e-3)))
    _prev["t"], _prev["p"] = now, p
    last = usage.last or {}
    cpu = max(proc_cpu, float(last.get("cpu", 0.0)))
    mem = _read_cgroup_mem()
    if mem and mem[1] > 0:
        used, total = mem
    elif last:
        used, total = float(last.get("ram_used_mb", 0)), float(last.get("ram_total_mb", 0))
    else:
        used, total = usage._read_rss_mb(), 0.0
    ram = 100.0 * used / total if total else 0.0
    v = {"cpu": round(cpu, 1), "ram": round(ram, 1), "ram_used_mb": round(used), "ram_total_mb": round(total),
         "tasks": live.tasks, "streams": _streams()}
    _mcache["t"], _mcache["v"] = now, dict(v)
    return v


def _streams() -> int:
    from utils import usage
    return usage.live.streams


def self_doc() -> dict:
    return {"url": config.BASE_URL, "name": NODE_LABEL, "host": HOST, "role": "gateway" if IS_GATEWAY else ("standby" if CAN_GATEWAY else "worker"),
            "cap": config.NODE_CAPACITY,
            "max_tasks": config.NODE_MAX_TASKS,
            "hb": time.time(), **local_metrics(), "reserved": live.reserved}


# ----------------------------------------------------------------------------- scoring (pure functions)
def load_score(n: dict, users: int) -> float:
    cap = max(1, n.get("cap", 30))
    mt = max(1, n.get("max_tasks", 20))
    return (0.55 * min(users / cap, 1.5) + 0.25 * min(n.get("tasks", 0) / mt, 1.5)
            + 0.12 * min(n.get("cpu", 0) / 100, 1.0)
            + 0.08 * min(n.get("ram", 0) / 100, 1.0))


def is_hot(n: dict) -> bool:
    return n.get("cpu", 0) >= 90 or n.get("ram", 0) >= 92 or n.get("tasks", 0) >= n.get("max_tasks", 20)


def choose(nodes: dict, counts: dict):
    """nodes: {id: doc} of HEALTHY servers. Returns (node id or None, overflow flag)."""
    if not nodes:
        return None, False
    free = [i for i, n in nodes.items() if counts.get(i, 0) < n.get("cap", 30) and not is_hot(n)]
    if not free:
        return None, True  # never silently exceed a configured per-node active-user cap
    best = min(free, key=lambda i: (load_score(nodes[i], counts.get(i, 0)), counts.get(i, 0), i))
    return best, False


def hour_key(ts: float = None) -> str:
    d = dt.datetime.fromtimestamp(ts or time.time(), dbm.LIMIT_TZ)
    return d.strftime("%Y-%m-%d-%H")


async def _load_nodes() -> list:
    """Server list from MongoDB. 'hb' is converted to THIS server's clock with MongoDB's own time, so servers whose
    clocks differ by a few seconds are not marked DOWN by mistake."""
    docs = await dbm.cl_nodes_col.find({}).to_list(None)
    srv_now = None
    try:
        srv_now = (await dbm.db.command("hello")).get("localTime")
    except Exception:
        pass                                                # fall back to the heartbeat's own timestamp
    now = time.time()
    for d in docs:
        ts = d.get("hb_srv")
        if ts is not None and srv_now is not None:
            try:
                d["hb"] = now - max(0.0, (srv_now - ts).total_seconds())
            except Exception:
                pass
    return docs


# ----------------------------------------------------------------------------- the router (gateway only)
class CapacityUnavailable(Exception):
    """No safe capacity: caller must return a retryable response."""


class Router:
    def __init__(self):
        self.nodes: dict = {}           # id -> heartbeat doc
        self.down_until: dict = {}      # id -> time until which it is skipped
        self.users: dict = {}           # user id -> [node id, last seen, last saved]
        self.ctr = {"assign": 0, "overflow": 0, "failover": 0}

    # ---- health / counts
    def healthy(self, nid: str, now: float) -> bool:
        n = self.nodes.get(nid)
        if not n or self.down_until.get(nid, 0) > now:
            return False
        if nid != config.NODE_ID and now - n.get("hb", 0) >= STALE_AFTER:
            return False
        # Do not route new work to a worker reporting exhausted processing slots.
        # Older node documents without `reserved` remain compatible.
        reserved = n.get("reserved")
        if reserved is not None and reserved >= min(10, n.get("cap", 10)):
            return False
        return True

    def healthy_nodes(self, now: float) -> dict:
        return {i: n for i, n in self.nodes.items() if self.healthy(i, now)}

    def counts(self, now: float) -> dict:
        c: dict = {}
        for ent in self.users.values():
            if now - ent[1] < WINDOW:
                c[ent[0]] = c.get(ent[0], 0) + 1
        return c

    # ---- routing
    def route(self, uid: int, now: float) -> str:
        ent = self.users.get(uid)
        if ent and now - ent[1] < WINDOW and self.healthy(ent[0], now):
            ent[1] = now                                  # still active on the same server
            return ent[0]
        if ent and now - ent[1] < WINDOW:                 # its server died -> move it
            self.ctr["failover"] += 1
        nid, overflow = choose(self.healthy_nodes(now), self.counts(now))
        if nid is None:
            self.ctr["overflow"] += 1
            raise CapacityUnavailable("All healthy servers are at their active-user capacity")
        self.users[uid] = [nid, now, 0.0]
        self.ctr["assign"] += 1
        return nid

    def mark_down(self, nid: str, secs: float = DOWN_FOR):
        if nid != config.NODE_ID:
            self.down_until[nid] = time.time() + secs
            self.ctr["failover"] += 1

    def target(self, data: dict) -> str:
        """Which server must process this Telegram update?"""
        uid, kind = _update_user(data)
        if uid is None or kind not in ("message", "callback_query") or _is_admin(uid):
            return config.NODE_ID
        return self.route(uid, time.time())

    # ---- refresh from MongoDB
    async def refresh(self):
        docs = await _load_nodes()
        nodes = {d["_id"]: d for d in docs}
        me = self_doc()
        nodes[config.NODE_ID] = {**nodes.get(config.NODE_ID, {}), **me}
        self.nodes = nodes

    async def restore(self):
        try:
            cur = dbm.cl_assign_col.find({"seen": {"$gte": time.time() - WINDOW}})
            async for d in cur:
                self.users[d["_id"]] = [d["node"], d["seen"], d["seen"]]
        except Exception:
            log.warning("Could not restore user -> server pins", exc_info=True)

    async def persist(self, now: float):
        ops = []
        for uid, ent in self.users.items():
            if ent[1] - ent[2] >= PERSIST_EVERY - 1 and now - ent[1] < WINDOW:
                ops.append(UpdateOne({"_id": uid}, {"$set": {
                    "node": ent[0], "seen": ent[1],
                    "expire_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)}}, upsert=True))
                ent[2] = ent[1]
        for i in range(0, len(ops), 500):
            await dbm.cl_assign_col.bulk_write(ops[i:i + 500], ordered=False)
        for uid in [u for u, e in self.users.items() if now - e[1] > WINDOW * 6]:   # forget long-gone users
            del self.users[uid]

    def snapshot(self) -> dict:
        """Everything the Servers screen needs, right now."""
        now = time.time()
        counts = self.counts(now)
        rows = []
        for nid, n in sorted(self.nodes.items()):
            ok = self.healthy(nid, now)
            users = counts.get(nid, 0)
            if not ok:
                state = "DOWN"
            elif users >= n.get("cap", 30):
                state = "FULL"
            elif is_hot(n):
                state = "HOT"
            else:
                state = "OK"
            rows.append({"id": nid, "name": n.get("name") or nid, "host": n.get("host", ""), "state": state, "users": users, "cap": n.get("cap", 30), "tasks": n.get("tasks", 0),
                         "streams": n.get("streams", 0), "cpu": n.get("cpu", 0), "ram": n.get("ram", 0),
                         "ram_mb": n.get("ram_used_mb", 0), "ram_total": n.get("ram_total_mb", 0),
                         "load": round(100 * min(load_score(n, users), 1.0)) if ok else 0,
                         "gateway": n.get("role") == "gateway", "standby": n.get("role") == "standby", "age": int(now - n.get("hb", 0)), "url": n.get("url", "")})
        return {"nodes": rows, "active": sum(r["users"] for r in rows),
                "capacity": sum(r["cap"] for r in rows if r["state"] != "DOWN"),
                "up": sum(1 for r in rows if r["state"] != "DOWN")}


router = Router()


def _update_user(data: dict):
    for key in ("message", "callback_query", "edited_message", "my_chat_member", "chat_member"):
        obj = data.get(key)
        if isinstance(obj, dict) and isinstance(obj.get("from"), dict):
            if key == "message" and (obj.get("chat") or {}).get("type") not in (None, "private"):
                return obj["from"].get("id"), "group"
            return obj["from"].get("id"), key
    return None, next((k for k in data if k != "update_id"), "")


def _is_admin(uid: int) -> bool:
    from utils.helpers import is_admin
    return is_admin(uid)


# ----------------------------------------------------------------------------- processing an update on THIS server
async def process_local(data: dict):
    """Process admitted updates, deduplicating in-flight work and recording only success.

    This is process-local protection; a hard crash still requires durable recovery.
    """
    uid = data.get("update_id")
    try:
        if uid is not None and uid in _seen_set:
            return True
        if uid is not None and uid in _processing_ids:
            return False
        if uid is not None:
            _processing_ids.add(uid)
        update = Update.de_json(data, _app.bot)
        if update is None:
            return True
        user_id, kind = _update_user(data)
        if user_id is not None and kind in ("message", "callback_query"):
            _touch(user_id)
        global _sem
        if _sem is None:
            _sem = asyncio.Semaphore(config.NODE_MAX_TASKS)
        live.tasks += 1
        live.tasks_peak = max(live.tasks_peak, live.tasks)
        try:
            async with _sem:
                await _app.process_update(update)
            if uid is not None:
                if len(_seen_ids) == _seen_ids.maxlen:
                    _seen_set.discard(_seen_ids[0])
                _seen_ids.append(uid)
                _seen_set.add(uid)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Update %s failed", uid)
            return False
        finally:
            live.tasks -= 1
    finally:
        if uid is not None:
            _processing_ids.discard(uid)
        live.reserved = max(0, live.reserved - 1)
        user_id, _ = _update_user(data)
        if user_id is not None and user_id in _active_user_slots:
            remaining = _active_user_slots[user_id] - 1
            if remaining:
                _active_user_slots[user_id] = remaining
            else:
                del _active_user_slots[user_id]
        if uid is not None:
            _admitted_ids.discard(uid)


def _admit_local(data: dict, *, spawn: bool = True) -> bool:
    """Reserve a bounded processing slot BEFORE acknowledging HTTP.

    This is a process-local operation; the router separately enforces per-node
    user assignments.  No unlimited asyncio task backlog is permitted.
    """
    uid = data.get("update_id")
    if uid is not None and (uid in _admitted_ids or uid in _processing_ids or uid in _seen_set):
        return False
    if _draining or live.reserved >= min(10, config.NODE_MAX_TASKS):
        return False
    user_id, _ = _update_user(data)
    if user_id is not None and user_id not in _active_user_slots and len(_active_user_slots) >= 10:
        return False
    live.reserved += 1
    if user_id is not None:
        _active_user_slots[user_id] = _active_user_slots.get(user_id, 0) + 1
    if uid is not None:
        _admitted_ids.add(uid)
    try:
        if spawn:
            _spawn(process_local(data))
    except BaseException:
        live.reserved -= 1
        if user_id is not None:
            remaining = _active_user_slots[user_id] - 1
            if remaining:
                _active_user_slots[user_id] = remaining
            else:
                del _active_user_slots[user_id]
        if uid is not None:
            _admitted_ids.discard(uid)
        raise
    return True


# ----------------------------------------------------------------------------- "connecting" animation
_act: dict = {}          # user id -> monotonic time of his last update on THIS server
_pending: set = set()    # users who just (re)connected to this server and have not seen the animation yet


def _touch(uid: int):
    now = time.monotonic()
    last = _act.get(uid)
    _act[uid] = now
    if last is None or now - last > WINDOW:      # new here, or came back after being inactive
        _pending.add(uid)
    if len(_act) > 20000:
        for u in [u for u, t in _act.items() if now - t > WINDOW * 2]:
            del _act[u]
            _pending.discard(u)


def server_line() -> str:
    """One line for the user: which server he is connected to ('' when not in multi-server mode)."""
    return f"🖥 <b>Server:</b> {html.escape(NODE_LABEL)}" if ENABLED else ""


def _frames() -> list:
    lab = html.escape(NODE_LABEL)
    return [
        "📡 <b>Connecting…</b>\n\n▰▱▱▱▱▱▱▱▱▱ 10%\n<i>Scanning servers</i>",
        "🛰 <b>Connecting…</b>\n\n▰▰▰▱▱▱▱▱▱▱ 30%\n<i>Finding the fastest route</i>",
        "🔐 <b>Connecting…</b>\n\n▰▰▰▰▰▰▱▱▱▱ 60%\n<i>Securing the connection</i>",
        f"⚡ <b>Connecting…</b>\n\n▰▰▰▰▰▰▰▰▱▱ 85%\n<i>Syncing with {lab}</i>",
        f"✅ <b>Connected</b>\n\n🖥 Server: <b>{lab}</b>\n📶 Status: Online · Secure\n\n▰▰▰▰▰▰▰▰▰▰ 100%",
    ]


async def _delete_later(bot, chat_id: int, message_id: int, after: float):
    await asyncio.sleep(after)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


async def connect_animation(bot, chat_id: int):
    """Shown once when a user connects (or reconnects) to this server. Never blocks or breaks the file delivery."""
    if not ENABLED or chat_id not in _pending:
        return
    _pending.discard(chat_id)
    if _is_admin(chat_id):
        return
    from telegram.constants import ParseMode
    frames = _frames()
    try:
        m = await bot.send_message(chat_id, frames[0], parse_mode=ParseMode.HTML)
        for f in frames[1:]:
            await asyncio.sleep(0.6)
            await bot.edit_message_text(f, chat_id=chat_id, message_id=m.message_id, parse_mode=ParseMode.HTML)
        _spawn(_delete_later(bot, chat_id, m.message_id, 8))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.debug("Connect animation skipped: %s", type(e).__name__)


def _http():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=20, connect=10),
            connector=aiohttp.TCPConnector(limit=200, keepalive_timeout=60))
    return _session


def _safe_node_url(url: str) -> bool:
    """The server list lives in MongoDB and we send the cluster secret to these URLs: https only, no credentials in the URL,
    no IP literal that points into a private / loopback / link-local network."""
    try:
        p = urlparse(url)
        if p.scheme != "https" or not p.hostname or p.username or p.password:
            return False
        try:
            ip = ipaddress.ip_address(p.hostname)
        except ValueError:
            return p.hostname != "localhost" and not p.hostname.endswith((".local", ".internal"))
        return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
                    or ip.is_multicast or ip.is_unspecified)
    except Exception:
        return False


# Rolling gateway handoff metrics: bounded memory, no MongoDB writes per request.
_handoff_samples = deque(maxlen=240)  # (monotonic timestamp, seconds, outcome)
def _record_handoff(start: float, outcome: str):
    _handoff_samples.append((time.monotonic(), max(0.0, time.monotonic() - start), outcome))

def handoff_pressure() -> dict:
    now = time.monotonic()
    recent = [(delay, outcome) for ts, delay, outcome in _handoff_samples if now - ts <= 120]
    latencies = sorted(delay for delay, outcome in recent if outcome == "ok")
    p95 = latencies[min(len(latencies) - 1, math.ceil(len(latencies) * .95) - 1)] if latencies else 0.0
    return {"requests": len(recent), "p95": p95,
            "busy": sum(outcome == "busy" for _, outcome in recent),
            "errors": sum(outcome == "error" for _, outcome in recent)}


def scale_recommendation(snapshot: dict, pressure: dict, streak: int) -> int:
    """Return additional servers to suggest; sustained signals only.

    Current observed capacity is authoritative (not an assumed 50-server fleet).
    Ignore sparse latency samples and network errors alone: adding servers does
    not fix a broken network or database.
    """
    if streak < 4 or snapshot.get("up", 0) <= 0:
        return 0
    cap = max(1, snapshot.get("capacity", 0))
    active = snapshot.get("active", 0)
    # 25% spare capacity; recommendations use actual per-node capacities.
    per_node = max(1, cap / snapshot["up"])
    from_users = max(0, math.ceil((active / .75 - cap) / per_node))
    from_latency = (1 if pressure["requests"] >= 20 and pressure["p95"] >= 1.5 else 0)
    from_busy = (1 if pressure["busy"] >= 3 else 0)
    return min(20, max(from_users, from_latency, from_busy))


async def forward(nid: str, data: dict):
    """True = the server accepted the update, "busy" = it answered 503 (overloaded or shutting down), False = unreachable."""
    url = (router.nodes.get(nid) or {}).get("url")
    if not url:
        return False
    if not _safe_node_url(url):
        log.error("Not forwarding to %s: its URL %r is not a safe public https URL", nid, url)
        return False
    body = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode()
    ts = str(int(time.time()))
    started = time.monotonic()
    try:
        async with _http().post(f"{url}/internal/update", data=body,
                                headers={"Content-Type": "application/json", "X-Cluster-Ts": ts, "X-Cluster-Sig": _sig(ts, nid, body)},
                                timeout=aiohttp.ClientTimeout(total=9, connect=4)) as r:
            if r.status == 200:
                _record_handoff(started, "ok")
                return True
            if r.status == 503:
                _record_handoff(started, "busy")
                return "busy"
            log.warning("Server %s answered HTTP %s", nid, r.status)
    except Exception as e:
        log.warning("Forward to %s failed: %s", nid, type(e).__name__)
    _record_handoff(started, "error")
    return False


async def dispatch(data: dict):
    """Only acknowledge successful handoff; never reroute an ambiguous network failure.

    An HTTP timeout could mean the remote node accepted the update. Retrying on a
    different node could duplicate delivery, so ask Telegram to retry instead.
    """
    nid = router.target(data)
    if nid == config.NODE_ID:
        await _enqueue_durable(data, config.NODE_ID)
        return
    res = await forward(nid, data)
    if res is True:
        return
    if res == "busy":
        # Explicit rejection: safe to choose a different worker on Telegram retry.
        router.mark_down(nid, 5)
        router.users.pop(_update_user(data)[0], None)
    else:
        # Ambiguous network failure: the worker may have accepted the update.
        # Preserve its sticky assignment so a Telegram retry goes to the SAME
        # worker, avoiding a duplicate on a different worker.
        log.warning("Unconfirmed handoff to %s; preserving sticky routing", nid)
    raise CapacityUnavailable("Worker did not acknowledge update")


# ----------------------------------------------------------------------------- durable MongoDB inbox
# The HTTP 200 means durable acceptance, not completed delivery. Telegram API side
# effects cannot be exactly-once without idempotency in the individual handlers.
INBOX_LEASE_SECONDS = 180
INBOX_MAX_ATTEMPTS = 8
ORPHAN_SCAN_EVERY = 10


def orphaned_nodes(nodes: list, now: float) -> list[str]:
    """Only recover work from known nodes with stale heartbeats."""
    return [n["_id"] for n in nodes
            if n.get("_id") != config.NODE_ID
            and isinstance(n.get("hb"), (int, float))
            and now - n["hb"] > STALE_AFTER]



async def _enqueue_durable(data: dict, node: str) -> None:
    update_id = data.get("update_id")
    if type(update_id) is not int or update_id < 0:
        raise ValueError("Invalid Telegram update_id")
    # _id is global across all nodes: a failover cannot create two inbox entries.
    now = dt.datetime.now(dt.timezone.utc)
    await dbm.cl_inbox_col.update_one(
        {"_id": update_id},
        {"$setOnInsert": {"payload": data, "node": node, "state": "queued",
                          "attempts": 0, "retry_at": now, "created": now,
                          "expire_at": now + dt.timedelta(days=7)}}, upsert=True)


async def _renew_inbox_lease(update_id: int):
    """Extend the claim while a slow Telegram handler is still executing."""
    while True:
        await asyncio.sleep(30)
        now = dt.datetime.now(dt.timezone.utc)
        result = await dbm.cl_inbox_col.update_one(
            {"_id": update_id, "owner": OWNER, "state": "processing"},
            {"$set": {"lease_until": now + dt.timedelta(seconds=INBOX_LEASE_SECONDS)}})
        if not result.matched_count:
            raise RuntimeError("Lost durable update lease")


async def _process_durable(data: dict) -> None:
    update_id = data["update_id"]
    success = False
    renewer = asyncio.create_task(_renew_inbox_lease(update_id))
    try:
        success = bool(await process_local(data))
        if renewer.done() and not renewer.cancelled() and renewer.exception() is not None:
            log.error("Inbox lease renewal failed for update %s", update_id)
            success = False
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Durable update %s failed", update_id)
    finally:
        renewer.cancel()
        await asyncio.gather(renewer, return_exceptions=True)
        now = dt.datetime.now(dt.timezone.utc)
        if success:
            try:
                await dbm.cl_inbox_col.update_one(
                    {"_id": update_id, "owner": OWNER, "state": "processing"},
                    {"$set": {"state": "done", "finished": now,
                              "expire_at": now + dt.timedelta(days=7)},
                     "$unset": {"payload": "", "owner": "", "lease_until": ""}})
            except Exception:
                log.exception("Could not mark update %s done; it may retry", update_id)
        else:
            try:
                record = await dbm.cl_inbox_col.find_one({"_id": update_id}, {"attempts": 1})
                attempts = int((record or {}).get("attempts", 0))
                state = "failed" if attempts >= INBOX_MAX_ATTEMPTS else "queued"
                await dbm.cl_inbox_col.update_one(
                    {"_id": update_id, "owner": OWNER, "state": "processing"},
                    {"$set": {"state": state, "retry_at": now + dt.timedelta(seconds=min(300, 2 ** min(attempts, 8)))},
                     "$unset": {"owner": "", "lease_until": ""}})
            except Exception:
                log.exception("Could not schedule retry for %s; lease recovery will retry", update_id)


async def inbox_loop():
    """Poll persisted updates; only claim work with a free local capacity slot."""
    dead_nodes = []
    last_scan = 0.0
    while not _draining:
        try:
            if live.reserved >= min(10, config.NODE_MAX_TASKS):
                await asyncio.sleep(.3)
                continue
            now = dt.datetime.now(dt.timezone.utc)
            if time.monotonic() - last_scan >= ORPHAN_SCAN_EVERY:
                # Never steal work if health data cannot be read.
                try:
                    dead_nodes = orphaned_nodes(await _load_nodes(), time.time())
                except Exception:
                    dead_nodes = []
                    log.exception("Cannot verify node health for inbox recovery")
                last_scan = time.monotonic()
            eligible = ({"node": {"$in": [config.NODE_ID] + dead_nodes}}
                        if dead_nodes else {"node": config.NODE_ID})
            doc = await dbm.cl_inbox_col.find_one_and_update(
                {"$and": [eligible, {"$or": [
                    {"state": "queued", "retry_at": {"$lte": now}},
                    {"state": "processing", "lease_until": {"$lte": now}}]}]},
                {"$set": {"state": "processing", "owner": OWNER,
                          "lease_until": now + dt.timedelta(seconds=INBOX_LEASE_SECONDS)},
                 "$inc": {"attempts": 1}},
                sort=[("created", 1)], return_document=ReturnDocument.AFTER)
            if doc is None:
                await asyncio.sleep(.35)
                continue
            data = doc.get("payload")
            if not isinstance(data, dict):
                await dbm.cl_inbox_col.update_one({"_id": doc["_id"], "owner": OWNER},
                                                 {"$set": {"state": "failed"}})
                continue
            if not _admit_local(data, spawn=False):
                await dbm.cl_inbox_col.update_one(
                    {"_id": doc["_id"], "owner": OWNER},
                    {"$set": {"state": "queued", "retry_at": now + dt.timedelta(seconds=1)},
                     "$unset": {"owner": "", "lease_until": ""}})
                await asyncio.sleep(.3)
                continue
            _spawn(_process_durable(data))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Durable inbox poll failed")
            await asyncio.sleep(2)


# ----------------------------------------------------------------------------- HTTP routes
def _same(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8", "ignore"), b.encode("utf-8", "ignore"))


async def tg_webhook(request: web.Request):
    if (request.match_info["secret"] != WEBHOOK_SECRET
            or not _same(request.headers.get("X-Telegram-Bot-Api-Secret-Token", ""), WEBHOOK_SECRET)):
        raise web.HTTPForbidden()
    if _draining or not IS_GATEWAY:                        # standby / shutting down: Telegram keeps the update and retries
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "5"})
    try:
        data = await request.json()
    except Exception:
        raise web.HTTPBadRequest()
    if not isinstance(data, dict):
        raise web.HTTPBadRequest()
    update_id = data.get("update_id")
    # A duplicate is safe to acknowledge only after a prior handoff succeeded.
    if update_id is not None and update_id in _gw_seen.s:
        return web.Response(text="ok")
    if update_id is not None and update_id in _gw_inflight:
        # The first request may still fail; never acknowledge this retry prematurely.
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "2"})
    if update_id is not None:
        _gw_inflight.add(update_id)
    try:
        await asyncio.wait_for(dispatch(data), timeout=9.5)
        _gw_seen.hit(update_id)
        return web.Response(text="ok")
    except (CapacityUnavailable, asyncio.TimeoutError):
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "2"})
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Gateway dispatch failed")
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "2"})
    finally:
        if update_id is not None:
            _gw_inflight.discard(update_id)


async def internal_update(request: web.Request):
    # Refuse oversized forwarded Telegram updates before JSON parsing or
    # admission. aiohttp's max_client_size should also be configured upstream.
    if getattr(request, "content_length", None) is not None and request.content_length > 1024 * 1024:
        raise web.HTTPRequestEntityTooLarge(max_size=1024 * 1024, actual_size=request.content_length)
    try:
        raw = await request.read()
    except web.HTTPRequestEntityTooLarge:
        raise
    except Exception:
        raise web.HTTPBadRequest()
    if len(raw) > 1024 * 1024:
        raise web.HTTPRequestEntityTooLarge(max_size=1024 * 1024, actual_size=len(raw))
    if not _signed_ok(request.headers, raw):
        # Rolling upgrade only (CLUSTER_ALLOW_LEGACY=1): an old server still sends the raw secret in X-Cluster-Secret.
        if not (config.CLUSTER_ALLOW_LEGACY and _same(request.headers.get("X-Cluster-Secret", ""), config.CLUSTER_SECRET)):
            raise web.HTTPForbidden()
    try:
        data = json.loads(raw)
    except Exception:
        raise web.HTTPBadRequest()
    if not isinstance(data, dict):
        raise web.HTTPBadRequest()
    try:
        await _enqueue_durable(data, config.NODE_ID)
    except ValueError:
        raise web.HTTPBadRequest()
    except Exception:
        log.exception("Durable inbox unavailable; rejecting update for Telegram retry")
        raise web.HTTPServiceUnavailable(headers={"Retry-After": "3"})
    return web.Response(text="ok")


def setup_routes(app: web.Application):
    if not ENABLED:
        return
    app.router.add_post("/internal/update", internal_update)
    if CAN_GATEWAY:                                        # standby gateways answer 503 here until they hold the lease
        app.router.add_post("/tg/{secret}", tg_webhook)


# ----------------------------------------------------------------------------- background loops
_warned: dict = {}


async def _check_conflicts():
    """Catch the config mistake that breaks multi-server mode: two servers with the SAME NODE_ID (they overwrite each
    other, the gateway thinks there is only ONE server). Two NODE_ROLE=gateway servers are fine: active + standby."""
    try:
        now = time.time()
        docs = await _load_nodes()
    except Exception:
        return
    me = config.NODE_ID
    for d in docs:
        nid = d.get("_id")
        fresh = now - d.get("hb", 0) < STALE_AFTER
        if nid == me and fresh and d.get("url") and config.BASE_URL and d.get("url") != config.BASE_URL:
            msg = (f"NODE_ID '{me}' is used by TWO servers ({d.get('url')} and {config.BASE_URL}). "
                   "Every server needs its own NODE_ID (n1, n2, n3...). Fix the env var and redeploy.")
            key = f"dup:{me}"
        else:
            continue
        if now - _warned.get(key, 0) > 600:
            _warned[key] = now
            log.error("CONFIG ERROR: %s", msg)
            try:
                from utils.helpers import notify_owner
                await notify_owner(_app.bot, "cl_" + key, "⚠️ " + msg, every=1800)
            except Exception:
                pass


async def beat():
    doc = {**self_doc(), "tasks_peak": live.tasks_peak, "inst": INSTANCE_ID}
    stage = {k: {"$literal": v} for k, v in doc.items()}      # $literal: a value starting with "$" is never read as a field
    stage["hb_srv"] = "$$NOW"                                   # MongoDB's own clock (see _load_nodes)
    await dbm.cl_nodes_col.update_one({"_id": config.NODE_ID}, [{"$set": stage}], upsert=True)
    live.tasks_peak = live.tasks


async def heartbeat_loop():
    n = 0
    while True:
        try:
            await beat()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Heartbeat failed: %s", type(e).__name__)
        n += 1
        try:
            if not IS_GATEWAY and n % 3 == 0:              # workers pick up panel changes made on the gateway (~30 s)
                await dbm.settings.load()
            if n % 6 == 1:                                 # config mistakes (same NODE_ID / two gateways) -> owner alert
                await _check_conflicts()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Settings reload failed: %s", type(e).__name__)
        await asyncio.sleep(HB_EVERY)


async def _ping_all():
    # Probe concurrently so one unreachable node cannot block the other 49.
    async def probe(nid, n):
        try:
            async with _http().get(f"{n['url']}/healthz", timeout=aiohttp.ClientTimeout(total=4)) as r:
                await r.read()
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
    await asyncio.gather(*(probe(nid, n) for nid, n in list(router.nodes.items())
                           if nid != config.NODE_ID and n.get("url")))


async def router_loop():
    """Gateway: keep the server list fresh, keep the other servers awake."""
    last_ping = 0.0
    while True:
        try:
            await router.refresh()
            if time.time() - last_ping >= PING_EVERY:
                last_ping = time.time()
                _spawn(_ping_all())
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("Router refresh failed: %s", type(e).__name__)
        await asyncio.sleep(REFRESH_EVERY)


def _new_acc(key: str) -> dict:
    return {"key": key, "samples": 0, "sum": 0, "peak": 0, "cap": 0, "up": 0, "n": {}}


async def _flush(acc: dict):
    if not acc["samples"]:
        return
    c, router.ctr = router.ctr, {"assign": 0, "overflow": 0, "failover": 0}
    mx = {"peak_active": acc["peak"], "cap_total": acc["cap"], "nodes_up": acc["up"]}
    for nid, v in acc["n"].items():
        for k, val in v.items():
            mx[f"n.{nid}.{k}"] = val
    try:
        await dbm.cl_stats_col.update_one(
            {"_id": acc["key"]},
            {"$inc": {"samples": acc["samples"], "active_sum": acc["sum"], **c}, "$max": mx,
             "$set": {"expire_at": dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=KEEP_DAYS)}}, upsert=True)
    except Exception:
        for k, v in c.items():                              # keep the counters for the next flush
            router.ctr[k] += v
        log.warning("Could not save cluster stats", exc_info=True)
    acc.update(samples=0, sum=0, peak=0, cap=0, up=0, n={})


class NodeStatusAlerts:
    """Owner-only heartbeat transitions. Startup establishes a baseline, not alerts.

    Two consecutive stale/fresh samples are required to avoid flapping.
    Missing nodes are not classified as offline without a prior heartbeat.
    """
    def __init__(self):
        self.status = {}  # node_id -> confirmed healthy bool
        self.pending = {}  # node_id -> (candidate bool, consecutive samples)

    def transitions(self, nodes, now):
        events = []
        for nid, doc in nodes.items():
            if not doc.get("hb"):
                continue
            fresh = now - doc["hb"] < STALE_AFTER
            if nid not in self.status:
                self.status[nid] = fresh
                continue
            if self.status[nid] == fresh:
                self.pending.pop(nid, None)
                continue
            candidate, count = self.pending.get(nid, (fresh, 0))
            count = count + 1 if candidate == fresh else 1
            if count < 2:
                self.pending[nid] = (fresh, count)
                continue
            self.status[nid] = fresh
            self.pending.pop(nid, None)
            events.append((nid, doc.get("name") or nid, fresh))
        # Do not treat a disappeared database document as a proven outage.
        return events


async def stats_loop(bot):
    """Gateway: peak statistics + owner warning when every server is (almost) full."""
    from utils.helpers import notify_owner
    acc = _new_acc(hour_key())
    last_flush = last_persist = time.time()
    pressure_streak = 0
    last_scale_notice = 0.0
    last_scale_count = 0
    node_alerts = NodeStatusAlerts()
    try:
        while True:
            await asyncio.sleep(SAMPLE_EVERY)
            try:
                now = time.time()
                key = hour_key(now)
                if key != acc["key"]:
                    await _flush(acc)
                    acc = _new_acc(key)
                snap = router.snapshot()
                for nid, name, recovered in node_alerts.transitions(router.nodes, now):
                    online = sum(1 for d in router.nodes.values() if now - d.get("hb", 0) < STALE_AFTER)
                    status = "🟢" if recovered else "🔴"
                    action = "wapas online aa gaya" if recovered else "offline ho gaya (sleep ya crash ho sakta hai)"
                    message = f"{status} {name} ({nid}) {action}.\n📡 {online}/{len(router.nodes)} servers online."
                    try:
                        await bot.send_message(config.OWNER_ID, message)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.warning("Could not send server status alert for %s", nid, exc_info=True)
                acc["samples"] += 1
                acc["sum"] += snap["active"]
                acc["peak"] = max(acc["peak"], snap["active"])
                acc["cap"] = max(acc["cap"], snap["capacity"])
                acc["up"] = max(acc["up"], snap["up"])
                for r in snap["nodes"]:
                    if r["state"] == "DOWN":
                        continue
                    cur = acc["n"].setdefault(r["id"], {})
                    for k, v in (("users", r["users"]), ("tasks", r["tasks"]), ("streams", r["streams"]),
                                 ("cpu", r["cpu"]), ("ram", r["ram"])):
                        cur[k] = max(cur.get(k, 0), v)
                pressure = handoff_pressure()
                # Only sustained pressure, not one slow request or a brief burst.
                is_pressured = (snap["capacity"] > 0 and snap["active"] >= .75 * snap["capacity"]) or (
                    pressure["requests"] >= 20 and pressure["p95"] >= 1.5) or pressure["busy"] >= 3
                pressure_streak = pressure_streak + 1 if is_pressured else 0
                extra = scale_recommendation(snap, pressure, pressure_streak)
                if extra and (now - last_scale_notice >= 1800 or extra > last_scale_count):
                    # A short private owner-only recommendation. No automatic deployments.
                    await notify_owner(bot, "scale_advice",
                        f"⚡ Bot par load badh raha hai. {extra} aur server add karo. "
                        f"Abhi {snap['up']} online hain.", every=1800)
                    last_scale_notice, last_scale_count = now, extra
                if now - last_flush >= FLUSH_EVERY:
                    await _flush(acc)
                    last_flush = now
                if now - last_persist >= PERSIST_EVERY:
                    await router.persist(now)
                    last_persist = now
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Cluster sampling failed")
    except asyncio.CancelledError:
        await _flush(acc)
        raise


async def _set_webhook(bot, attempts: int = 6):
    if not config.BASE_URL.startswith("https://"):
        raise RuntimeError("Gateway BASE_URL must be a public HTTPS URL")
    url = f"{config.BASE_URL}/tg/{WEBHOOK_SECRET}"
    for attempt in range(attempts):
        try:
            result = await bot.set_webhook(url=url, secret_token=WEBHOOK_SECRET, allowed_updates=ALLOWED_UPDATES,
                                  max_connections=100, drop_pending_updates=False)
            if result is False:
                raise RuntimeError("Telegram rejected webhook registration")
            log.info("Telegram webhook set -> %s/tg/***", config.BASE_URL)
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("set_webhook failed (%s), retrying", type(e).__name__)
            if attempt < attempts - 1:
                await asyncio.sleep(5 * (attempt + 1))
    raise RuntimeError(f"Telegram webhook registration failed after {attempts} attempt(s)")


async def _register_webhook_or_step_down(bot):
    """Fail closed if Telegram rejects the webhook; release leadership for standby."""
    try:
        await _set_webhook(bot)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Gateway cannot register Telegram webhook; relinquishing lease")
        global IS_GATEWAY
        IS_GATEWAY = False
        try:
            await dbm.cl_lease_col.update_one(
                {"_id": "gateway", "owner": OWNER}, {"$set": {"until": EPOCH}})
        except Exception:
            log.exception("Could not release gateway lease after webhook failure")
        # Do not cancel this task from within itself. Lease loop will handle
        # background cleanup on the next iteration; stop serving immediately.


def set_hooks(promote, demote):
    """main.py: functions that start / stop the gateway-only background jobs (auto-delete, autoposter, backup...)."""
    _hooks["promote"], _hooks["demote"] = promote, demote


async def _try_lease():
    """One attempt to take or renew the gateway lease. Atomic in MongoDB and judged by MongoDB's clock.
    Returns True when WE hold the lease now."""
    global _deadline
    t0 = time.monotonic()
    flt = {"_id": "gateway", "$expr": {"$or": [{"$eq": ["$owner", OWNER]}, {"$lt": ["$until", "$$NOW"]}]}}
    upd = [{"$set": {
        "owner": {"$literal": OWNER}, "node": {"$literal": config.NODE_ID}, "url": {"$literal": config.BASE_URL},
        "until": {"$add": ["$$NOW", LEASE * 1000]},
        "term": {"$cond": [{"$eq": ["$owner", OWNER]}, {"$ifNull": ["$term", 1]}, {"$add": [{"$ifNull": ["$term", 0]}, 1]}]},
    }}]
    global _lease_ready
    if not _lease_ready:                                    # create the lease record once (no $expr involved in an upsert)
        try:
            await dbm.cl_lease_col.update_one({"_id": "gateway"}, {"$setOnInsert": {"owner": None, "until": EPOCH, "term": 0}},
                                              upsert=True)
        except DuplicateKeyError:                           # two servers created it at the same moment: fine
            pass
        _lease_ready = True
    doc = await dbm.cl_lease_col.find_one_and_update(flt, upd, return_document=ReturnDocument.AFTER)   # None = held by another
    if doc and doc.get("owner") == OWNER:
        _deadline = t0 + LEASE - LEASE_MARGIN               # measured from BEFORE the call = conservative
        return True
    return False


async def _promote(app):
    """This server just won the lease: become the active gateway."""
    global IS_GATEWAY
    try:
        await dbm.settings.load()                           # the previous gateway may have changed settings in the last 30 s
    except Exception:
        log.warning("Settings reload before promotion failed", exc_info=True)
    router.users.clear()
    await router.refresh()
    await router.restore()                                  # keep users on the servers they were already using
    IS_GATEWAY = True
    log.warning("Node %s is now the active GATEWAY", config.NODE_ID)
    if _hooks.get("promote"):
        _hooks["promote"]()
    _gw_tasks.extend([asyncio.create_task(router_loop()), asyncio.create_task(stats_loop(app.bot)),
                      asyncio.create_task(_register_webhook_or_step_down(app.bot))])
    try:
        await beat()                                        # role shows up in Panel -> Servers at once
    except Exception:
        pass


async def _demote():
    """This server lost (or gave back) the lease: stop acting as gateway. Becomes a normal worker / standby."""
    global IS_GATEWAY
    if not IS_GATEWAY and not _gw_tasks:
        return
    IS_GATEWAY = False                                      # the webhook answers 503 from now on
    old_tasks = list(_gw_tasks)
    for t in old_tasks:
        t.cancel()
    _gw_tasks.clear()
    if old_tasks:
        # Ensure old gateway-only loops have actually stopped before another
        # lease owner can safely take over periodic work.
        await asyncio.gather(*old_tasks, return_exceptions=True)
    if _hooks.get("demote"):
        _hooks["demote"]()
    log.warning("Node %s is no longer the gateway", config.NODE_ID)


async def lease_loop(app):
    while True:
        try:
            ok = await _try_lease()
        except asyncio.CancelledError:
            raise
        except Exception as e:                              # MongoDB unreachable: keep leading only while the lease is surely valid
            log.warning("Gateway lease check failed: %s", type(e).__name__)
            ok = IS_GATEWAY and time.monotonic() < _deadline
        try:
            if ok and not IS_GATEWAY and not _draining:
                await _promote(app)
            elif not ok and IS_GATEWAY:
                await _demote()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Gateway promotion / demotion failed")
        await asyncio.sleep(LEASE_EVERY)


async def start(app) -> list:
    """Called from main.post_init when ENABLED. Returns the background tasks."""
    if int(os.getenv("WEB_CONCURRENCY", "1")) != 1 or int(os.getenv("UVICORN_WORKERS", "1")) != 1:
        raise RuntimeError("Cluster strict capacity requires exactly one application process per NODE_ID")
    await beat()                                            # appear in the server list at once
    tasks = [asyncio.create_task(heartbeat_loop()), asyncio.create_task(inbox_loop())]
    if CAN_GATEWAY:
        tasks.append(asyncio.create_task(lease_loop(app)))  # wins the lease -> _promote() (router, stats, webhook, jobs)
    _bg.extend(tasks)
    log.info("Cluster mode: node %s (%s), capacity %s users", config.NODE_ID,
             "gateway-capable (active or standby, decided by the lease)" if CAN_GATEWAY else "worker", config.NODE_CAPACITY)
    return tasks


async def shutdown():
    """Leave cleanly: stop taking updates, hand the gateway lease over, let the running updates finish."""
    global _draining
    _draining = True                                        # /internal/update and /tg answer 503 from now on
    for t in list(_bg):
        t.cancel()
    try:
        await _demote()
    except Exception:
        pass
    try:                                                    # tell the others at once (only if the record is still OURS)
        await dbm.cl_nodes_col.update_one({"_id": config.NODE_ID, "inst": INSTANCE_ID}, {"$set": {"hb": 0, "hb_srv": EPOCH}})
        if CAN_GATEWAY:
            await dbm.cl_lease_col.update_one({"_id": "gateway", "owner": OWNER}, {"$set": {"until": EPOCH}})
    except Exception:
        pass
    pending = [t for t in _tasks if not t.done()]
    if pending:
        log.info("Draining %s running update(s) (max %ss)", len(pending), config.DRAIN_SECONDS)
        await asyncio.wait(pending, timeout=config.DRAIN_SECONDS)
    if _session and not _session.closed:
        await _session.close()


# ----------------------------------------------------------------------------- reading the statistics (panel)
async def load_hours(since: dt.date) -> list:
    try:
        cur = dbm.cl_stats_col.find({"_id": {"$gte": since.strftime("%Y-%m-%d") + "-00"}}).sort("_id", 1)
        return await cur.to_list(None)
    except Exception:
        log.exception("Could not read cluster stats")
        return []


def servers_needed(peak: int, cap_per_node: int, headroom: float = 1.25) -> int:
    return max(1, math.ceil(peak * headroom / max(cap_per_node, 1)))
