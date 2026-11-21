"""
database/db.py
Async MongoDB layer (Motor) + an in-memory settings cache.

Collections
  settings        one document (_id="main") holding every panel setting
  users           _id=user id, banned / blocked flags
  links           _id=random token, ids=[DB-channel message ids], label, views
  files           one record per uploaded file (for the Stats screen)
  pending_deletes scheduled auto-deletes (survive bot restarts)
  ap_channels     Autoposter target channels/groups (timings, ON/OFF, loop mode, shuffled queue_state)
  ap_posts        Autoposter posts (thumbnail + share-link token + posted flag)
"""
import asyncio
import copy
import datetime as dt
import logging
import random
import re
import secrets
import time
from zoneinfo import ZoneInfo

from pymongo import ReturnDocument, UpdateOne
from pymongo.errors import DuplicateKeyError

import config
from database import mongo

log = logging.getLogger(__name__)

# The CORE cluster (config.MONGO_URI = yours). Optional extra clusters are handled by database/mongo.py: every *_col below is a
# small proxy that talks to whichever cluster currently holds that collection (and falls back to core when a friend's cluster is down).
db = mongo.core.db
col = mongo.col                       # col("name") -> routed collection (used by backup / restore)
settings_col = col("settings")
users_col = col("users")
links_col = col("links")
files_col = col("files")
pending_col = col("pending_deletes")
ads_col = col("ad_sessions")          # one-time Mini App ad sessions (auto-expire)            [hot]
usage_col = col("daily_usage")        # _id="<user_id>:<YYYY-MM-DD>", used=<links opened today> [hot]
ap_channels_col = col("ap_channels")  # Autoposter targets: _id=chat id, slots, enabled, done
ap_posts_col = col("ap_posts")        # Autoposter posts: _id=share-link token, chan=chat id, seq, posted
vps_col = col("vps_usage")            # _id="YYYY-MM-DD-HH|<node>" (IST): hourly VPS samples of ONE server - utils/usage.py [logs]
cl_nodes_col = col("cluster_nodes")   # _id=node id: url, heartbeat, capacity, cpu, ram, tasks, streams (utils/cluster.py) [core only]
cl_assign_col = col("cluster_assign") # _id=user id: node the user is pinned to + last seen (short-lived)               [core only]
cl_stats_col = col("cluster_stats")   # _id="YYYY-MM-DD-HH" (IST): peak active users, per-node peaks, overflow events    [logs]
cl_inbox_col = col("cluster_update_inbox")  # durable update inbox [core only]
cl_lease_col = col("cluster_lease")   # _id="gateway": owner, until (Mongo server time), term (utils/cluster.py)         [core only]
stats_col = col("daily_stats")        # _id="YYYY-MM-DD" (IST): views, gates, imp, clk, valued, rev, verified, fallback, blocked [hot]

# A user can open at most this many links per day (admins are exempt). One link = 1 count, all its files are sent.
# Day changes at midnight in this timezone.
DAILY_LIMIT = 10
LIMIT_TZ = ZoneInfo("Asia/Kolkata")

# Backup code that is written under every file in the Database Channel, e.g.  FSB:Ab3dE9xYz1Q2:0
# It is hidden from users on delivery and is used to rebuild links if the bot / channel is lost.
CODE_RE = re.compile(r"FSB:([A-Za-z0-9_-]{6,}):(\d+)")
CODE_TAIL_RE = re.compile(r"\s*<code>FSB:[A-Za-z0-9_-]{6,}:\d+</code>\s*$")

DEFAULT_CAPTION = (
    "👋 Hello {mention}!\n\n"
    "Please join all the channels below to access your files,\n"
    "then tap <b>Verify</b>."
)

DEFAULTS = {
    "admins": [],                 # extra admin user ids (OWNER_ID is always admin)
    "db_channel": None,           # {"id": int, "title": str}
    "fsub_enabled": True,
    "fsub_channels": [],          # [{"id": int, "title": str, "link": str}], order = display order
    "start_image": None,          # Telegram file_id
    "start_caption": DEFAULT_CAPTION,
    "custom_buttons": [],         # rows of [[text, url], ...]
    "autodelete": 0,              # minutes, 0 = off
    "delivery_mode": "forward",   # Telegram chat forwarding only
    "protect": False,             # protect_content on delivered files (forward mode only)
    "custom_caption": "",         # HTML, replaces original caption when set
    "remove_caption": False,      # strip original caption
    "daily_limit": 10,            # links per user per day (admins exempt)
    # ---- Monetag rewarded ads (Telegram Mini App) - all configured from the Ads screen of the panel
    "ads_enabled": False,
    "ads_zone": "",               # Monetag main SDK zone id
    "ads_sdk_url": "https://libtl.com/sdk.js",   # SDK script URL exactly as in your Monetag snippet
    "ads_postback": True,         # server-to-server verification (Monetag postback) - strongest mode
    "ads_secret": "",             # secret used in the postback URL (auto-generated)
    "ads_block_adblock": True,    # refuse users running an ad blocker / ad-blocking DNS
    "ads_tries": 3,               # no-fill tries before files are given without an ad
    "ads_last_postback": 0,
    "ads_admin_test": False,      # show the ad gate to admins too (for testing)
    # ---- Backup (utils/backup.py)
    "auto_backup": True,          # send a backup file to the owner every 24 h
    "bk_last": 0,                 # time of the last backup
}


class SettingsCache:
    """Settings are read from memory (fast) and written through to MongoDB."""

    def __init__(self):
        self._data = copy.deepcopy(DEFAULTS)

    async def load(self):
        doc = await settings_col.find_one({"_id": "main"}) or {}
        doc.pop("_id", None)
        self._data = {**copy.deepcopy(DEFAULTS), **doc}

    def get(self, key):
        return self._data.get(key, copy.deepcopy(DEFAULTS.get(key)))

    async def set(self, key, value):
        self._data[key] = value
        await settings_col.update_one({"_id": "main"}, {"$set": {key: value}}, upsert=True)

    async def reset(self, keep=("admins", "db_channel", "ads_secret", "bk_last")):
        """Restore defaults but keep admins and the database channel."""
        kept = {k: self._data.get(k) for k in keep}
        fresh = copy.deepcopy(DEFAULTS)
        fresh.update(kept)
        self._data = fresh
        await settings_col.replace_one({"_id": "main"}, {"_id": "main", **fresh}, upsert=True)


settings = SettingsCache()


# (collection, key, options) - created on EVERY cluster that may hold the collection (a fallback to core must find them too)
_INDEXES = [
    (links_col, "created", {}),
    (pending_col, "delete_at", {}),
    (files_col, "msg_id", {}),
    (files_col, "code", {}),
    (usage_col, "expire_at", {"expireAfterSeconds": 0}),            # old counters clean themselves
    (ads_col, "expire_at", {"expireAfterSeconds": 0}),
    (vps_col, "expire_at", {"expireAfterSeconds": 0}),              # VPS usage history is kept ~13 months
    (cl_inbox_col, "expire_at", {"expireAfterSeconds": 0}),
    (cl_inbox_col, [("node", 1), ("state", 1), ("retry_at", 1)], {}),
    (cl_assign_col, "expire_at", {"expireAfterSeconds": 0}),
    (cl_stats_col, "expire_at", {"expireAfterSeconds": 0}),
    (ads_col, [("uid", 1), ("status", 1)], {}),
    (ap_posts_col, [("chan", 1), ("seq", 1)], {}),
    (ap_posts_col, [("chan", 1), ("posted", 1), ("seq", 1)], {}),
    # Stats screen counts banned / blocked users: without these two small partial indexes every count scans ALL users
    (users_col, "banned", {"partialFilterExpression": {"banned": True}}),
    (users_col, "blocked", {"partialFilterExpression": {"blocked": True}}),
]


async def init_db():
    """Ping Mongo, create indexes on every cluster, load the settings cache."""
    await db.command("ping")                       # core must work, otherwise the bot cannot start
    if len(mongo.clusters) > 1:
        bad = await mongo.ping_all(timeout=8)      # a dead friend's cluster is skipped, not fatal
        for name, err in bad.items():
            if err:
                log.error("MongoDB cluster '%s' is not reachable at startup (%s) - core serves its collections for now", name, err)
    for c, key, opts in _INDEXES:
        for cl, raw in c.all_raw():
            if not cl.up:
                continue
            try:
                await raw.create_index(key, **opts)
            except Exception as e:
                if cl.name == "core":
                    raise
                log.error("Index on cluster '%s' failed: %s", cl.name, e)
    await settings.load()


# ---------------------------------------------------------------- users
async def add_user(user_id: int):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"blocked": False}, "$setOnInsert": {"joined": time.time(), "banned": False}},
        upsert=True,
    )


async def touch_user(user_id: int) -> bool:
    """Register / refresh the user in ONE round trip. Returns True if the user is banned."""
    doc = await users_col.find_one_and_update(
        {"_id": user_id},
        {"$set": {"blocked": False}, "$setOnInsert": {"joined": time.time(), "banned": False}},
        upsert=True, projection={"banned": 1}, return_document=ReturnDocument.BEFORE,
    )
    return bool(doc and doc.get("banned"))


async def is_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"_id": user_id}, {"banned": 1})
    return bool(doc and doc.get("banned"))


async def set_banned(user_id: int, flag: bool):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"banned": flag}, "$setOnInsert": {"joined": time.time(), "blocked": False}},
        upsert=True,
    )


async def set_blocked(user_id: int, flag: bool):
    await users_col.update_one({"_id": user_id}, {"$set": {"blocked": flag}})


_BROADCAST_FILTER = {"banned": {"$ne": True}, "blocked": {"$ne": True}}


async def iter_broadcast_ids():
    async for doc in users_col.find(_BROADCAST_FILTER, {"_id": 1}):
        yield doc["_id"]


async def count_broadcastable() -> int:
    return await users_col.count_documents(_BROADCAST_FILTER)


async def stats() -> dict:
    return {
        "users": await users_col.count_documents({}),
        "files": await files_col.count_documents({}),
        "links": await links_col.count_documents({}),
        "banned": await users_col.count_documents({"banned": True}),
        "blocked": await users_col.count_documents({"blocked": True}),
    }


async def _cluster_status(cl, detail: bool) -> dict:
    out = {"name": cl.name, "ok": False, "error": "", "ping_ms": None, "read_ms": None, "limit_mb": cl.limit_mb,
           "routes": mongo.routes_of(cl.name), "down": not cl.up}
    try:
        t = time.perf_counter()
        await cl.db.command("ping")
        out["ping_ms"] = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        await cl.db["settings"].find_one({"_id": "main"})
        out["read_ms"] = (time.perf_counter() - t) * 1000
        st = await cl.db.command("dbStats", scale=1)
        out.update(ok=True, objects=st.get("objects", 0), data=st.get("dataSize", 0),
                   storage=st.get("storageSize", 0), index=st.get("indexSize", 0),
                   collections=st.get("collections", 0))
        out["used"] = out["storage"] + out["index"]          # what Atlas counts against the quota
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"[:200]
        return out
    try:
        out["conn"] = (await cl.client.admin.command("serverStatus")).get("connections")
    except Exception:
        out["conn"] = None                                   # free tier does not allow serverStatus
    if detail:
        cols = []
        for name in sorted(await cl.db.list_collection_names()):
            try:
                c = await cl.db.command("collStats", name)
                cols.append((name, c.get("count", 0), c.get("storageSize", 0) + c.get("totalIndexSize", 0)))
            except Exception:
                pass
        out["cols"] = sorted(cols, key=lambda x: -x[2])
    return out


async def mongo_status(detail: bool = True) -> dict:
    """Health, speed and storage of the CORE database (for the Stats screen) + 'extra' = the other clusters, if any."""
    out = await _cluster_status(mongo.core, detail)
    out["extra"] = [await _cluster_status(cl, False) for n, cl in mongo.clusters.items() if n != "core"]
    return out


# ---------------------------------------------------------------- files & links
def new_token() -> str:
    return secrets.token_urlsafe(9)  # 12 random URL-safe chars, unguessable


def make_code(token: str, idx: int) -> str:
    return f"FSB:{token}:{idx}"


def parse_code(text):
    """(token, idx) from a caption that contains a backup code, else None."""
    m = CODE_RE.search(text or "")
    return (m.group(1), int(m.group(2))) if m else None


async def add_file(msg_id: int, kind: str, name: str, added_by: int, code=None, cap=None):
    """cap = the ORIGINAL caption (HTML) without the backup code - that is what users get."""
    doc = {"msg_id": msg_id, "kind": kind, "name": name, "by": added_by, "date": time.time()}
    if code:
        doc["code"] = code
        doc["cap"] = cap or ""
    await files_col.insert_one(doc)


_cap_cache: dict = {}          # tuple(ids) -> (monotonic time, {msg_id: caption})
CAP_TTL = 60


async def caption_overrides(ids: list) -> dict:
    """{msg_id: original caption} for files that carry a backup code (so the code never reaches users).
    Cached for CAP_TTL seconds: a popular link is opened hundreds of times, its captions never change."""
    key, now = tuple(ids), time.monotonic()
    hit = _cap_cache.get(key)
    if hit and now - hit[0] < CAP_TTL:
        return dict(hit[1])
    out = {}
    async for d in files_col.find({"msg_id": {"$in": ids}, "code": {"$exists": True}},
                                  {"msg_id": 1, "cap": 1}):
        out[d["msg_id"]] = d.get("cap") or ""
    if len(_cap_cache) > 2000:
        _cap_cache.clear()
    _cap_cache[key] = (now, dict(out))
    return out


async def restore_file(token: str, idx: int, new_msg_id: int, kind: str, name: str, cap: str, by: int):
    """Point link <token> slot <idx> at its new message id (file was re-forwarded into a new channel / bot).
    Safe when many files of ONE link arrive at the same time: every file only touches its own array slot."""
    code = make_code(token, idx)
    await files_col.update_one(
        {"code": code},
        {"$set": {"msg_id": new_msg_id, "kind": kind, "name": name, "cap": cap, "by": by,
                  "date": time.time()}},
        upsert=True,
    )
    try:   # create the link if it does not exist (idempotent)
        await links_col.update_one(
            {"_id": token},
            {"$setOnInsert": {"ids": [], "creator": by, "label": "Restored link",
                              "created": time.time(), "views": 0}},
            upsert=True)
    except DuplicateKeyError:
        pass   # another file of the same link created it a moment ago
    # one atomic write per slot; missing slots before it become null (skipped on delivery until restored)
    await links_col.update_one({"_id": token}, {"$set": {f"ids.{idx}": new_msg_id}})
    _link_cache.pop(token, None)
    _cap_cache.clear()


async def create_link(ids: list, creator: int, label: str, token: str = None, free: bool = False) -> str:
    """free=True: a free-preview link (Autoposter Image + Video type): no ad gate, does not use the daily limit."""
    token = token or new_token()
    doc = {"_id": token, "ids": ids, "creator": creator, "label": label,
           "created": time.time(), "views": 0}
    if free:
        doc["free"] = True
    await links_col.insert_one(doc)
    return token


_link_cache: dict = {}         # token -> (monotonic time, link doc)  - per server, hits only (a new link is visible at once)
LINK_TTL = 15                  # seconds. A revoked link may still open for this long on OTHER servers (this one forgets it at once)


async def get_link(token: str):
    now = time.monotonic()
    hit = _link_cache.get(token)
    if hit and now - hit[0] < LINK_TTL:
        return hit[1]
    doc = await links_col.find_one({"_id": token})
    if doc is not None:
        if len(_link_cache) > 5000:
            _link_cache.clear()
        _link_cache[token] = (now, doc)
    return doc


async def delete_link(token: str):
    await links_col.delete_one({"_id": token})
    _link_cache.pop(token, None)


# --- write buffering: counters are added up in memory and written once every few seconds (one op per hot document instead of one per click)
_view_buf: dict = {}           # token -> views to add
_stat_buf: dict = {}           # "YYYY-MM-DD" -> {field: amount}
FLUSH_EVERY = 5


async def inc_views(token: str):
    _view_buf[token] = _view_buf.get(token, 0) + 1
    await stat_inc(views=1)


async def stat_inc(**fields):
    """Add to today's counters (IST day). Analytics must never break the bot, so this never raises and never waits on MongoDB."""
    buf = _stat_buf.setdefault(_today(), {})
    for k, v in fields.items():
        buf[k] = buf.get(k, 0) + v


async def flush_counters():
    """Write the buffered counters. Called by stats_flush_loop every FLUSH_EVERY seconds and once more on shutdown."""
    global _view_buf, _stat_buf
    views, _view_buf = _view_buf, {}
    stats, _stat_buf = _stat_buf, {}
    if views:
        try:
            await links_col.bulk_write([UpdateOne({"_id": t}, {"$inc": {"views": n}}) for t, n in views.items()], ordered=False)
        except Exception:
            for t, n in views.items():                       # keep them for the next round
                _view_buf[t] = _view_buf.get(t, 0) + n
            log.warning("Could not save link views (will retry)", exc_info=True)
    for day, inc in stats.items():
        try:
            await stats_col.update_one({"_id": day}, {"$inc": inc}, upsert=True)
        except Exception:
            buf = _stat_buf.setdefault(day, {})
            for k, v in inc.items():
                buf[k] = buf.get(k, 0) + v
            log.warning("Could not save daily stats (will retry)", exc_info=True)


async def stats_flush_loop():
    try:
        while True:
            await asyncio.sleep(FLUSH_EVERY)
            await flush_counters()
    except asyncio.CancelledError:
        await flush_counters()
        raise


async def list_links(skip: int, limit: int, creator=None):
    query = {"creator": creator} if creator is not None else {}
    cursor = links_col.find(query).sort("created", -1).skip(skip).limit(limit)
    return await cursor.to_list(limit)


async def count_links(creator=None) -> int:
    return await links_col.count_documents({"creator": creator} if creator is not None else {})


# ---------------------------------------------------------------- auto-delete queue
async def schedule_delete(chat_id: int, ids: list, minutes: int):
    """Queue the auto-delete. Runs AFTER the files were sent, so a database hiccup is logged, never raised (the user must not get an error)."""
    when = time.time() + minutes * 60
    docs = [{"chat_id": chat_id, "ids": ids[i:i + 100], "delete_at": when} for i in range(0, len(ids), 100)]  # Telegram deletes max 100 per call
    try:
        if docs:
            await pending_col.insert_many(docs)
    except Exception:
        log.error("Auto-delete could not be scheduled for chat %s (%s message(s))", chat_id, len(ids), exc_info=True)


async def due_deletes(limit: int = 100):
    cursor = pending_col.find({"delete_at": {"$lte": time.time()}}).limit(limit)
    return await cursor.to_list(limit)


async def remove_pending(doc_id):
    await pending_col.delete_one({"_id": doc_id})


# ---------------------------------------------------------------- daily limit (per user)
def _today() -> str:
    return dt.datetime.now(LIMIT_TZ).strftime("%Y-%m-%d")


def _next_midnight() -> dt.datetime:
    now = dt.datetime.now(LIMIT_TZ)
    return (now + dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)


def limit_value() -> int:
    try:
        return max(0, int(settings.get("daily_limit")))
    except (TypeError, ValueError):
        return DAILY_LIMIT


async def claim_quota(user_id: int):
    """Atomically take ONE link from today's allowance (no in-process locks, safe even with several workers).
    Returns (granted 0/1, used_after)."""
    limit = limit_value()
    if limit <= 0:
        return 0, 0
    key = f"{user_id}:{_today()}"
    expire = _next_midnight() + dt.timedelta(days=1)
    for _ in range(3):
        doc = await usage_col.find_one_and_update(
            {"_id": key, "used": {"$lt": limit}},
            {"$inc": {"used": 1}, "$set": {"expire_at": expire}},
            projection={"used": 1}, return_document=ReturnDocument.AFTER)
        if doc:
            return 1, int(doc["used"])
        try:
            await usage_col.insert_one({"_id": key, "used": 1, "expire_at": expire})
            return 1, 1
        except DuplicateKeyError:
            cur = await usage_col.find_one({"_id": key}, {"used": 1})
            if cur and int(cur.get("used", 0)) >= limit:
                return 0, int(cur["used"])
            # else: we raced with the very first insert of the day -> try the update again
    return 0, limit


async def reserve_quota(user_id: int, wanted: int = 1) -> int:
    """Kept for compatibility: 1 link = 1 count."""
    granted, _ = await claim_quota(user_id)
    return granted


async def refund_quota(user_id: int, n: int):
    """Give back a link count that was reserved but could not be delivered."""
    if n > 0:
        await usage_col.update_one({"_id": f"{user_id}:{_today()}", "used": {"$gte": n}}, {"$inc": {"used": -n}})


# ---------------------------------------------------------------- per-link progress (for links bigger than the daily limit)
async def get_progress(user_id: int, token: str) -> int:
    doc = await users_col.find_one({"_id": user_id}, {"prog": 1})
    return int(((doc or {}).get("prog") or {}).get(token, 0))


async def set_progress(user_id: int, token: str, n: int):
    """n = how many files of this link the user already got; 0 clears it (link finished)."""
    op = {"$set": {f"prog.{token}": n}} if n else {"$unset": {f"prog.{token}": ""}}
    await users_col.update_one({"_id": user_id}, op, upsert=True)


async def used_today(user_id: int) -> int:
    doc = await usage_col.find_one({"_id": f"{user_id}:{_today()}"}, {"used": 1})
    return (doc or {}).get("used", 0)


# ---------------------------------------------------------------- autoposter
def _blank_queue() -> dict:
    return {"cycle": 0, "cursor": 0, "queue": [], "shuffle_status": "idle", "last_batch_size": 0,
            "last_posted_at": None, "last_reshuffled_at": None, "last_post_id": None}


async def ap_add_channel(cid: int, title: str):
    await ap_channels_col.update_one(
        {"_id": cid},
        {"$set": {"title": title},
         "$setOnInsert": {"slots": [], "enabled": True, "done": [], "created": time.time(),
                          "loop": True, "queue_state": _blank_queue()}},
        upsert=True,
    )


async def ap_get_channel(cid: int):
    return await ap_channels_col.find_one({"_id": cid})


async def ap_list_channels():
    return await ap_channels_col.find({}).sort("created", 1).to_list(200)


async def ap_set(cid: int, **fields):
    await ap_channels_col.update_one({"_id": cid}, {"$set": fields})


async def ap_remove_channel(cid: int):
    await ap_channels_col.delete_one({"_id": cid})
    await ap_posts_col.delete_many({"chan": cid})


async def ap_claim_slot(cid: int, key: str) -> bool:
    """Atomically mark '<date> <HH:MM>' as run for this channel. True only for the caller that got it."""
    r = await ap_channels_col.update_one(
        {"_id": cid, "done": {"$ne": key}},
        {"$push": {"done": {"$each": [key], "$slice": -60}}},
    )
    return r.modified_count == 1


async def ap_last_seq(cid: int) -> int:
    doc = await ap_posts_col.find_one({"chan": cid}, {"seq": 1}, sort=[("seq", -1)])
    return int((doc or {}).get("seq", 0))


async def ap_add_post(cid: int, token: str, thumb_token: str, seq: int, title: str, cap: str,
                      kind: str, n_files: int, by: int, **extra):
    """token = the share-link token of the files; thumb_token = backup code token of the thumbnail copy in the DB channel.
    extra: Image + Video posts add ptype="iv" and img_token (the free image link); classic posts add nothing."""
    await ap_posts_col.insert_one({
        "_id": token, "chan": cid, "seq": seq, "thumb_token": thumb_token, "title": title,
        "cap": cap, "kind": kind, "n_files": n_files, "by": by,
        "posted": False, "posted_at": None, "created": time.time(), **extra,
    })


async def ap_get_post(token: str):
    return await ap_posts_col.find_one({"_id": token})


async def ap_list_posts(cid: int, skip: int, limit: int):
    return await ap_posts_col.find({"chan": cid}).sort("seq", 1).skip(skip).limit(limit).to_list(limit)


async def ap_counts(cid: int):
    total = await ap_posts_col.count_documents({"chan": cid})
    posted = await ap_posts_col.count_documents({"chan": cid, "posted": True})
    return total, posted


async def ap_pending(cid: int, limit: int):
    return await ap_posts_col.find({"chan": cid, "posted": False}).sort("seq", 1).limit(limit).to_list(limit)


async def ap_pending_count(cid: int) -> int:
    return await ap_posts_col.count_documents({"chan": cid, "posted": False})


async def ap_mark_posted(token: str):
    await ap_posts_col.update_one({"_id": token}, {"$set": {"posted": True, "posted_at": time.time()}})


async def ap_delete_post(token: str):
    await ap_posts_col.delete_one({"_id": token})


async def ap_wipe(cid: int, only_posted: bool = False, links: bool = False) -> int:
    """Delete every post of a channel (optionally only the posted ones, optionally their share links too)."""
    q = {"chan": cid, **({"posted": True} if only_posted else {})}
    docs = await ap_posts_col.find(q, {"_id": 1, "img_token": 1}).to_list(None)
    tokens = [d["_id"] for d in docs]
    if tokens:
        await ap_posts_col.delete_many({"_id": {"$in": tokens}})
        if links:
            extra = [d["img_token"] for d in docs if d.get("img_token")]      # Image + Video: the free image links too
            await links_col.delete_many({"_id": {"$in": tokens + extra}})
        await ap_channels_col.update_one({"_id": cid}, {"$set": {"queue_state": _blank_queue()}})
    return len(tokens)


async def ap_reset(cid: int):
    await ap_posts_col.update_many({"chan": cid}, {"$set": {"posted": False, "posted_at": None}})


# ---- shuffled queue (cycle = every post once in random order; cursor = how far the cycle has got)
async def ap_rebuild_cycle(cid: int, only_unused: bool = False):
    """Full rebuild = every post, all marked pending, freshly shuffled (new cycle).
    only_unused = only the posts not yet posted (used when one-pass mode / first start of an old channel)."""
    ch = await ap_channels_col.find_one({"_id": cid})
    if not ch:
        return None
    qs = ch.get("queue_state") or {}
    flt = {"chan": cid, **({"posted": False} if only_unused else {})}
    ids = [d["_id"] for d in await ap_posts_col.find(flt, {"_id": 1}).to_list(None)]
    random.shuffle(ids)
    last = qs.get("last_post_id")
    if len(ids) > 1 and ids[0] == last:      # never open a cycle with the post that closed the last one
        j = random.randrange(1, len(ids))
        ids[0], ids[j] = ids[j], ids[0]
    if not only_unused:
        await ap_posts_col.update_many({"chan": cid}, {"$set": {"posted": False, "posted_at": None}})
    prev = int(qs.get("cycle", 0))
    cycle = prev + 1 if ids and not (only_unused and prev > 0) else prev
    new_qs = {"cycle": cycle, "cursor": 0, "queue": ids, "shuffle_status": "ready" if ids else "empty",
              "last_batch_size": 0, "last_posted_at": qs.get("last_posted_at"),
              "last_reshuffled_at": time.time(), "last_post_id": last}
    return await ap_channels_col.find_one_and_update(
        {"_id": cid}, {"$set": {"queue_state": new_qs}}, return_document=ReturnDocument.AFTER)


async def ap_enqueue_new(cid: int, tokens: list):
    """New posts are mixed (shuffled) into the part of the running cycle that is still to be posted."""
    if not tokens:
        return
    ch = await ap_channels_col.find_one({"_id": cid})
    qs = (ch or {}).get("queue_state") or {}
    queue, cursor = list(qs.get("queue") or []), int(qs.get("cursor", 0))
    if not queue or cursor >= len(queue):
        return                                # no running cycle: the next rebuild includes them
    rest = queue[cursor:] + [str(t) for t in tokens]
    random.shuffle(rest)
    await ap_channels_col.update_one(
        {"_id": cid}, {"$set": {"queue_state.queue": queue[:cursor] + rest, "queue_state.shuffle_status": "ready"}})


async def ap_advance(cid: int, token: str):
    """A post of the queue was published: mark it posted and move the cursor."""
    now = time.time()
    await ap_posts_col.update_one({"_id": token}, {"$set": {"posted": True, "posted_at": now}})
    await ap_channels_col.update_one(
        {"_id": cid},
        {"$inc": {"queue_state.cursor": 1},
         "$set": {"queue_state.last_posted_at": now, "queue_state.last_post_id": token}})


async def ap_skip_cursor(cid: int):
    await ap_channels_col.update_one({"_id": cid}, {"$inc": {"queue_state.cursor": 1}})


async def ap_finalize(cid: int, sent: int, exhausted: bool):
    await ap_channels_col.update_one(
        {"_id": cid},
        {"$set": {"queue_state.last_batch_size": sent,
                  "queue_state.shuffle_status": "cycle complete • awaiting next shuffle" if exhausted else "in progress"}})


async def ap_upcoming(cid: int, limit: int = 10) -> list:
    """The next not-yet-posted posts, in queue order (for the Queue screen)."""
    ch = await ap_channels_col.find_one({"_id": cid})
    qs = (ch or {}).get("queue_state") or {}
    queue, cursor = qs.get("queue") or [], int(qs.get("cursor", 0))
    window = queue[cursor:cursor + limit * 4]
    if not window:
        return []
    docs = {d["_id"]: d for d in await ap_posts_col.find({"_id": {"$in": window}, "chan": cid}).to_list(None)}
    return [docs[t] for t in window if t in docs and not docs[t].get("posted")][:limit]


async def thumb_msg_id(code: str):
    """Current DB-channel message id of a thumbnail (looked up by backup code, so it survives a channel restore)."""
    doc = await files_col.find_one({"code": code}, {"msg_id": 1})
    return (doc or {}).get("msg_id")
