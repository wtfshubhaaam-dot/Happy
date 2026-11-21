"""Tests for the multi-MongoDB router (database/mongo.py) and the write buffering / caches in database/db.py.
No MongoDB needed: motor / pymongo are replaced by small fakes.     Run:  python3 tests/test_mongo_router.py"""
import asyncio, os, sys, time, types, importlib

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)
os.environ.update(BOT_TOKEN="123:abc", OWNER_ID="1", MONGO_URI="mongodb://core", MONGO_URI_2="mongodb://hot", MONGO_URI_3="mongodb://logs",
                  DB_NAME="t", CLUSTER_SECRET="", NODE_ID="")
sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *a, **k: None)

# ---------- fakes
class ServerSelectionTimeoutError(Exception): pass
class AutoReconnect(Exception): pass
class ConnectionFailure(Exception): pass
class DuplicateKeyError(Exception): pass
pm = types.ModuleType("pymongo"); pm.ReturnDocument = types.SimpleNamespace(AFTER="after", BEFORE="before")
pm.UpdateOne = lambda f, u, **k: ("UpdateOne", f, u)
pme = types.ModuleType("pymongo.errors")
pme.ServerSelectionTimeoutError, pme.AutoReconnect, pme.ConnectionFailure, pme.DuplicateKeyError = ServerSelectionTimeoutError, AutoReconnect, ConnectionFailure, DuplicateKeyError
sys.modules.update({"pymongo": pm, "pymongo.errors": pme})

class FakeCol:
    def __init__(self, client, db, name): self.client, self.db, self.name = client, db, name
    def _gate(self, op):
        self.client.log.append((self.client.uri, self.db, self.name, op))
        if self.client.broken: raise ServerSelectionTimeoutError("down")
        if self.client.dup and op == "insert_one": raise DuplicateKeyError("dup")
    def find(self, *a, **k): return ("cursor", self.client.uri, self.name)
    def __getattr__(self, op):
        async def f(*a, **k):
            self._gate(op); self.client.calls.append((self.name, op, a, k)); return self.client.ret.get(op)
        return f
class FakeDB:
    def __init__(self, client, name): self.client, self.name = client, name
    def __getitem__(self, n): return FakeCol(self.client, self.name, n)
    async def command(self, *a, **k):
        if self.client.broken: raise ServerSelectionTimeoutError("down")
        return {"ok": 1}
class FakeClient:
    instances = {}
    def __init__(self, uri, **kw):
        self.uri, self.kw, self.broken, self.dup, self.log, self.calls, self.ret = uri, kw, False, False, [], [], {}
        FakeClient.instances[uri] = self
    def __getitem__(self, n): return FakeDB(self, n)
motor = types.ModuleType("motor"); mm = types.ModuleType("motor.motor_asyncio"); mm.AsyncIOMotorClient = FakeClient
sys.modules.update({"motor": motor, "motor.motor_asyncio": mm})

ok = fail = 0
def check(name, cond):
    global ok, fail
    if cond: ok += 1; print("  ok  ", name)
    else: fail += 1; print("  FAIL", name)

def fresh(env):
    """re-import config + mongo + db with another environment"""
    for k in ("MONGO_URI_2", "MONGO_URI_3", "MONGO_ROUTES"): os.environ.pop(k, None)
    os.environ.update(env)
    for m in ("config", "database.mongo", "database.db"): sys.modules.pop(m, None)
    import database
    for a in ("mongo", "db"):
        if hasattr(database, a): delattr(database, a)          # `from database import x` would otherwise return the stale module
    FakeClient.instances.clear()
    import config; from database import mongo, db
    return config, mongo, db

# ================= single cluster = old behaviour
print("single cluster")
config, mongo, db = fresh({})
check("only core exists", list(mongo.clusters) == ["core"])
check("everything resolves to core", all(mongo.pick(n).name == "core" for n in ("daily_usage", "vps_usage", "users", "ad_sessions")))
check("single-server pool follows bounded default", config.MONGO_MAX_POOL == 20 and FakeClient.instances["mongodb://core"].kw["maxPoolSize"] == 20)

# ================= three clusters
print("routing")
config, mongo, db = fresh({"MONGO_URI_2": "mongodb://hot", "MONGO_URI_3": "mongodb://logs"})
check("critical daily usage, ads, stats and deletion data remain on core", all(mongo.pick(n).name == "core" for n in ("daily_usage", "ad_sessions", "daily_stats", "pending_deletes")))
check("vps_usage / cluster_stats -> logs", all(mongo.pick(n).name == "logs" for n in ("vps_usage", "cluster_stats")))
check("users, links, settings, files, transfer, lease, nodes stay on core", all(mongo.pick(n).name == "core" for n in
      ("users", "links", "settings", "files", "transfer", "transfer_sent", "cluster_lease", "cluster_nodes", "cluster_assign", "ap_posts", "known_chats")))
check("secondary clusters get a short timeout, core a long one",
      FakeClient.instances["mongodb://hot"].kw["serverSelectionTimeoutMS"] == 4000 and FakeClient.instances["mongodb://core"].kw["serverSelectionTimeoutMS"] == 15000)
check("db.usage_col is pinned to core", mongo.pick("daily_usage").name == "core")
async def t_route():
    await db.usage_col.update_one({"_id": "1:2026-01-01"}, {"$inc": {"used": 1}})
    await db.users_col.update_one({"_id": 1}, {"$set": {"x": 1}})
    uris = [e[0] for e in FakeClient.instances["mongodb://hot"].log] + [e[0] for e in FakeClient.instances["mongodb://core"].log]
    check("quota and users writes both go to core", any(e[2] == "daily_usage" for e in FakeClient.instances["mongodb://core"].log) and FakeClient.instances["mongodb://core"].log[-1][2] == "users" and not FakeClient.instances["mongodb://hot"].log)
    check("cursor methods (find) are passed through", db.users_col.find({})[1] == "mongodb://core")
asyncio.run(t_route())

# ================= only two clusters: logs falls back to hot
print("two clusters")
config, mongo, db = fresh({"MONGO_URI_2": "mongodb://hot"})
check("without MONGO_URI_3 the logs collections go to hot", mongo.pick("vps_usage").name == "hot")

# ================= MONGO_ROUTES override, and the core-only guard
print("MONGO_ROUTES")
config, mongo, db = fresh({"MONGO_URI_2": "mongodb://hot", "MONGO_ROUTES": "users=hot,links=logs,vps_usage=core,ad_sessions=nonsense,daily_usage=core"})
check("users / links can NOT be moved (core only)", mongo.pick("users").name == "core" and mongo.pick("links").name == "core")
check("allowed override applied (vps_usage -> core, daily_usage -> core)", mongo.pick("vps_usage").name == "core" and mongo.pick("daily_usage").name == "core")
check("invalid ad_sessions override cannot move core-only data", mongo.pick("ad_sessions").name == "core")
check("routes_of excludes core-only data from hot", "ad_sessions" not in mongo.routes_of("hot") and "users" not in mongo.routes_of("hot"))

# ================= failover of a non-critical collection (critical data never splits)
print("failover")
config, mongo, db = fresh({"MONGO_URI_2": "mongodb://hot", "MONGO_URI_3": "mongodb://logs", "MONGO_ROUTES": "vps_usage=hot"})
async def t_fail():
    hot, core_c = FakeClient.instances["mongodb://hot"], FakeClient.instances["mongodb://core"]
    core_c.ret["find_one_and_update"] = {"used": 3}
    hot.broken = True
    r = await db.vps_col.find_one_and_update({"_id": "k"}, {"$inc": {"used": 1}})
    check("non-critical hot collection retries on core during outage", r == {"used": 3})
    check("hot marked down and non-critical reads fail over to core", mongo.clusters["hot"].up is False and mongo.pick("vps_usage").name == "core")
    n_hot = len(hot.log)
    await db.vps_col.find_one({"_id": "k"})
    check("no further attempts on dead hot cluster", len(hot.log) == n_hot)
    check("logs collections fall to core when logs is down", (mongo.clusters["logs"].mark_down(Exception("x")), mongo.pick("cluster_stats").name)[1] == "core")
    hot.broken = False; mongo.clusters["logs"].mark_up()
    res = await mongo.ping_all(timeout=1)
    check("monitor ping restores hot for non-critical data", mongo.clusters["hot"].up is True and res["hot"] == "" and mongo.pick("vps_usage").name == "hot")
    check("daily_usage remains on core before and after hot recovery", mongo.pick("daily_usage").name == "core")
    core_c.broken = True
    try: await db.users_col.find_one({"_id": 1}); check("core failure not hidden", False)
    except ServerSelectionTimeoutError: check("core failure not hidden", True)
    core_c.broken = False
    hot.dup = True
    try: await db.vps_col.insert_one({"_id": "dup"}); check("DuplicateKeyError", False)
    except DuplicateKeyError: check("DuplicateKeyError is not treated as outage", mongo.clusters["hot"].up is True)
    hot.dup = False
asyncio.run(t_fail())

# ================= index creation on every cluster that may hold a collection
print("indexes")
async def t_idx():
    for inst in FakeClient.instances.values(): inst.log.clear()
    await db.init_db()
    seen = {(e[0], e[2]) for inst in FakeClient.instances.values() for e in inst.log if e[3] == "create_index"}
    check("daily_usage TTL index exists on authoritative core", ("mongodb://core", "daily_usage") in seen)
    check("vps_usage index on selected hot and fallback core", {("mongodb://hot", "vps_usage"), ("mongodb://core", "vps_usage")} <= seen)
    check("users partial indexes (banned / blocked count) on core only", ("mongodb://core", "users") in seen and ("mongodb://hot", "users") not in seen)
    FakeClient.instances["mongodb://hot"].broken = True
    for inst in FakeClient.instances.values(): inst.log.clear()
    await db.init_db()
    check("a dead secondary at startup does not stop the bot", True)
    check("... and is marked down", mongo.clusters["hot"].up is False)
    FakeClient.instances["mongodb://hot"].broken = False; mongo.clusters["hot"].mark_up()
asyncio.run(t_idx())

# ================= buffered counters
print("buffered counters")
async def t_buf():
    hot = FakeClient.instances["mongodb://hot"]; core_c = FakeClient.instances["mongodb://core"]
    hot.calls.clear(); core_c.calls.clear()
    for _ in range(500): await db.inc_views("tok1")
    await db.inc_views("tok2"); await db.stat_inc(gates=2, rev=0.5)
    check("500 clicks = zero database operations until the flush", not hot.calls and not core_c.calls)
    await db.flush_counters()
    stat = [c for c in core_c.calls if c[0] == "daily_stats"]
    views = [c for c in core_c.calls if c[0] == "links" and c[1] == "bulk_write"]
    check("ONE daily_stats update with the summed counters", len(stat) == 1 and stat[0][2][1] == {"$inc": {"views": 501, "gates": 2, "rev": 0.5}})
    ops = views[0][2][0] if views else []
    check("ONE bulk write for the link views (500 + 1)", len(views) == 1 and sorted((o[1]["_id"], o[2]["$inc"]["views"]) for o in ops) == [("tok1", 500), ("tok2", 1)])
    hot.broken = core_c.broken = True
    await db.inc_views("tok3"); await db.stat_inc(imp=1)
    await db.flush_counters()
    check("failed flush keeps the numbers for the next round", db._view_buf.get("tok3") == 1 and sum(v.get("imp", 0) for v in db._stat_buf.values()) == 1)
    hot.broken = core_c.broken = False; hot.calls.clear(); core_c.calls.clear()
    mongo.clusters["hot"].mark_up()                      # (the monitor would do this within 15 s)
    await db.flush_counters()
    check("...and writes them once the database is back", any(c[1] == "bulk_write" for c in core_c.calls) and any(c[0] == "daily_stats" for c in core_c.calls))
asyncio.run(t_buf())

# ================= link cache + caption cache
print("caches")
async def t_cache():
    core_c = FakeClient.instances["mongodb://core"]; core_c.calls.clear()
    core_c.ret["find_one"] = {"_id": "abc", "ids": [1, 2]}
    a = await db.get_link("abc"); b = await db.get_link("abc")
    check("second lookup served from memory", a == b and len([c for c in core_c.calls if c[1] == "find_one"]) == 1)
    core_c.ret["find_one"] = None
    await db.get_link("nope"); await db.get_link("nope")
    check("misses are NOT cached (a link created a moment ago must open at once)", len([c for c in core_c.calls if c[1] == "find_one"]) == 3)
    await db.delete_link("abc"); core_c.ret["find_one"] = None
    check("revoking a link forgets it on this server immediately", await db.get_link("abc") is None)
asyncio.run(t_cache())

# ================= schedule_delete never raises after the files were sent
print("schedule_delete")
async def t_sched():
    hot = FakeClient.instances["mongodb://hot"]; core_c = FakeClient.instances["mongodb://core"]
    hot.broken = core_c.broken = True
    try: await db.schedule_delete(5, list(range(250)), 10); check("no exception", True)
    except Exception: check("no exception", False)
    hot.broken = core_c.broken = False; core_c.calls.clear()
    mongo.clusters["hot"].mark_up()
    await db.schedule_delete(5, list(range(250)), 10)
    ins = [c for c in core_c.calls if c[1] == "insert_many" and c[0] == "pending_deletes"]
    check("250 ids -> 3 documents in ONE insert_many", len(ins) == 1 and len(ins[0][2][0]) == 3)
asyncio.run(t_sched())

print(f"\n{ok} passed, {fail} failed"); sys.exit(1 if fail else 0)
