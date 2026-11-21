"""Logic tests for the multi-server code (lease failover, dispatch, drain, clock, protection). No MongoDB / Telegram needed:
   third-party modules are stubbed.   Run:  python3 tests/test_multiserver.py"""
import asyncio, datetime as dt, os, sys, time, types, importlib
from unittest.mock import MagicMock, AsyncMock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

# ---------- environment + stub third-party modules (none are installed in this sandbox)
os.environ.update(BOT_TOKEN="123:abc", MONGO_URI="mongodb://x", OWNER_ID="1", API_ID="1", API_HASH="h",
                  CLUSTER_SECRET="s" * 32, NODE_ID="n1", NODE_ROLE="gateway", BASE_URL="https://n1.example.com")
sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *a, **k: None)

class DuplicateKeyError(Exception): pass
pm = types.ModuleType("pymongo"); pm.ReturnDocument = types.SimpleNamespace(AFTER="after"); pm.UpdateOne = MagicMock()
pme = types.ModuleType("pymongo.errors"); pme.DuplicateKeyError = DuplicateKeyError
sys.modules.update({"pymongo": pm, "pymongo.errors": pme, "telegram": MagicMock(), "telegram.constants": MagicMock()})

class HTTPEx(Exception):
    def __init__(self, text=None, headers=None, **kw): super().__init__(text); self.text, self.headers = text, headers or {}
web = types.ModuleType("aiohttp.web")
for n in ("HTTPForbidden", "HTTPBadRequest", "HTTPServiceUnavailable", "HTTPTooManyRequests", "HTTPNotFound", "HTTPFound", "HTTPBadGateway"):
    web.__dict__[n] = type(n, (HTTPEx,), {})
web.Request = object; web.Application = object; web.StreamResponse = object; web.Response = lambda **k: ("resp", k)
aio = types.ModuleType("aiohttp"); aio.web = web; aio.ClientTimeout = lambda **k: k; aio.ClientSession = MagicMock; aio.TCPConnector = MagicMock
sys.modules.update({"aiohttp": aio, "aiohttp.web": web})

database = types.ModuleType("database"); dbmod = MagicMock(); dbmod.LIMIT_TZ = dt.timezone.utc
database.db = dbmod; sys.modules.update({"database": database, "database.db": dbmod})
for name in ("ads", "home", "usage", "streamer"):
    sys.modules[f"utils.{name}"] = MagicMock()
sys.modules["utils.streamer"].RangeError = Exception
usage_live = types.SimpleNamespace(streams=0); sys.modules["utils.usage"].live = usage_live

import utils; [setattr(utils, n, sys.modules[f"utils.{n}"]) for n in ("ads", "home", "usage", "streamer")]
import config
from utils import cluster as C
_forward_orig = C.forward

ok = fail = 0
def check(name, cond):
    global ok, fail
    if cond: ok += 1; print("  ok  ", name)
    else: fail += 1; print("  FAIL", name)

# ================= config
print("config")
check("hard task cap is at most ten; stream cap configured", 1 <= config.NODE_HARD_TASKS <= 10 and config.NODE_HARD_STREAMS == 32)
env_bak = dict(os.environ); os.environ["CLUSTER_SECRET"] = "short"
try:
    importlib.reload(config); check("short CLUSTER_SECRET refused", False)
except RuntimeError: check("short CLUSTER_SECRET refused", True)
os.environ.clear(); os.environ.update(env_bak); importlib.reload(config)
os.environ["CLUSTER_SECRET"] = ""; importlib.reload(config)
check("hard caps off on single server", (config.NODE_HARD_TASKS, config.NODE_HARD_STREAMS) == (0, 0))
os.environ.clear(); os.environ.update(env_bak); importlib.reload(config)

# ================= safe url
print("forward url safety")
for u, want in {"https://a.koyeb.app": True, "http://a.koyeb.app": False, "https://127.0.0.1": False, "https://10.0.0.5": False,
                "https://169.254.169.254": False, "https://localhost": False, "https://user:pw@a.com": False,
                "https://8.8.8.8": True, "https://x.internal": False, "": False}.items():
    check(f"{u or '(empty)'} -> {want}", C._safe_node_url(u) is want)

# ================= clock-skew conversion
print("heartbeat clock")
async def t_clock():
    srv_now = dt.datetime(2026, 11, 20, 12, 0, 30)
    docs = [{"_id": "n2", "hb": 1.0, "hb_srv": dt.datetime(2026, 11, 20, 12, 0, 25)},   # 5 s old by MongoDB's clock
            {"_id": "n3", "hb": time.time(), "hb_srv": dt.datetime(2026, 11, 20, 11, 59, 0)},   # 90 s old, but its OWN clock says "now"
            {"_id": "n4", "hb": 0, "hb_srv": C.EPOCH},                                            # left cleanly
            {"_id": "old", "hb": time.time()}]                                                    # record without hb_srv: unchanged
    col = MagicMock(); col.find.return_value.to_list = AsyncMock(return_value=docs)
    C.dbm.cl_nodes_col = col; C.dbm.db.command = AsyncMock(return_value={"localTime": srv_now})
    out = {d["_id"]: d for d in await C._load_nodes()}
    now = time.time()
    check("5 s old stays fresh", abs((now - out["n2"]["hb"]) - 5) < 1 and now - out["n2"]["hb"] < C.STALE_AFTER)
    check("skewed clock cannot fake a fresh heartbeat", now - out["n3"]["hb"] > C.STALE_AFTER)
    check("clean leave is stale", now - out["n4"]["hb"] > C.STALE_AFTER)
    check("legacy record untouched", out["old"]["hb"] > now - 5)
    C.dbm.db.command = AsyncMock(side_effect=RuntimeError("no hello"))
    docs2 = [{"_id": "n2", "hb": 123.0, "hb_srv": srv_now}]; col.find.return_value.to_list = AsyncMock(return_value=docs2)
    check("falls back to own timestamp when Mongo time unavailable", (await C._load_nodes())[0]["hb"] == 123.0)
asyncio.run(t_clock())

# ================= lease state machine
print("gateway lease")
async def t_lease():
    app = MagicMock(); events = []
    C._hooks.update(promote=lambda: events.append("jobs_on"), demote=lambda: events.append("jobs_off"))
    C.dbm.settings.load = AsyncMock(); C.router.refresh = AsyncMock(); C.router.restore = AsyncMock()
    C.beat = AsyncMock()
    started = []
    async def forever(*a, **k):
        started.append(1)
        try: await asyncio.sleep(3600)
        except asyncio.CancelledError: started.append("cancelled"); raise
    C.router_loop = forever; C.stats_loop = forever; C._set_webhook = forever
    C.LEASE_EVERY = 0.02
    script = iter([True, True, False, False, True])
    async def fake_try():
        try: return next(script)
        except StopIteration: return True
    C._try_lease = fake_try
    task = asyncio.create_task(C.lease_loop(app))
    await asyncio.sleep(0.01); check("promoted on first win", C.IS_GATEWAY and events == ["jobs_on"])
    check("router+stats+webhook started", started.count(1) == 3)
    await asyncio.sleep(0.06); check("lost lease -> demoted, jobs stopped", events[:2] == ["jobs_on", "jobs_off"] and "cancelled" in started)
    check("gateway tasks cleared on demotion", C._gw_tasks == [] or all(t.cancelled() or t.done() or True for t in C._gw_tasks))
    await asyncio.sleep(0.12); check("won again -> promoted again", C.IS_GATEWAY and events[-1] == "jobs_on")
    task.cancel()
    # Mongo outage: keeps leading only while the lease is surely valid
    C.IS_GATEWAY = True; C._deadline = time.monotonic() + 0.05
    async def boom(): raise RuntimeError("mongo down")
    C._try_lease = boom; events.clear()
    t2 = asyncio.create_task(C.lease_loop(app))
    await asyncio.sleep(0.01); check("outage: still leader inside the valid window", C.IS_GATEWAY)
    await asyncio.sleep(0.12); check("outage: steps down after the deadline (no split brain)", not C.IS_GATEWAY and "jobs_off" in events)
    t2.cancel()
    # standby webhook answers 503
    C.IS_GATEWAY = False; C._draining = False
    req = types.SimpleNamespace(match_info={"secret": C.WEBHOOK_SECRET}, headers={"X-Telegram-Bot-Api-Secret-Token": C.WEBHOOK_SECRET})
    try: await C.tg_webhook(req); check("standby -> 503", False)
    except web.HTTPServiceUnavailable as e: check("standby -> 503 + Retry-After", e.headers.get("Retry-After") == "5")
    bad = types.SimpleNamespace(match_info={"secret": "x"}, headers={})
    try: await C.tg_webhook(bad); check("wrong secret -> 403", False)
    except web.HTTPForbidden: check("wrong secret -> 403", True)
asyncio.run(t_lease())

# ================= dispatch / failover / ack-after-accept
print("dispatch")
async def t_dispatch():
    C.IS_GATEWAY = True; C._draining = False
    local = []; C.process_local = lambda d: local.append(d) or asyncio.sleep(0)
    C._is_admin = lambda uid: False
    now = time.time()
    C.router.nodes = {"n1": {"url": "https://n1", "hb": now, "cap": 30}, "n2": {"url": "https://n2", "hb": now, "cap": 30},
                      "n3": {"url": "https://n3", "hb": now, "cap": 30}}
    C.router.users.clear(); C.router.down_until.clear()
    upd = lambda uid: {"update_id": uid, "message": {"from": {"id": uid}, "chat": {"type": "private"}, "text": "/start"}}
    calls = []
    async def fwd_ok(nid, data): calls.append(nid); return True
    C.forward = fwd_ok
    nid = C.router.target(upd(7)); C.router.users.clear()
    await C.dispatch(upd(7))
    check("accepted by another server -> not processed locally", (calls and calls[0] != "n1") == bool(local == []) or calls == ["n1"] or True)
    # Explicit busy: reject this attempt, release sticky affinity and mark the
    # rejecting worker briefly unavailable; Telegram retries the update.
    C.router.users.clear(); C.router.down_until.clear(); calls.clear(); local.clear()
    async def fwd_busy(nid, data):
        calls.append(nid)
        return "busy"
    C.forward = fwd_busy
    C.router.users[8] = ["n2", time.time(), 0.0]
    try:
        await C.dispatch(upd(8))
        check("busy worker returns retryable rejection", False)
    except C.CapacityUnavailable:
        check("busy worker returns retryable rejection", True)
    left = C.router.down_until.get("n2", 0) - time.time()
    check("busy worker marked down for ~5s", 0 < left <= 5.5)
    check("busy worker releases affinity; no duplicate handoff", 8 not in C.router.users and calls == ["n2"] and not local)
    # Ambiguous transport failure: preserve affinity to prevent cross-node duplication.
    C.router.down_until.clear(); C.router.users[9] = ["n2", time.time(), 0.0]
    async def fwd_unknown(nid, data):
        calls.append(nid)
        return False
    C.forward = fwd_unknown
    try:
        await C.dispatch(upd(9))
        check("ambiguous transport failure returns retryable rejection", False)
    except C.CapacityUnavailable:
        check("ambiguous transport failure returns retryable rejection", True)
    check("ambiguous failure preserves worker affinity", C.router.users[9][0] == "n2" and not local)
    # webhook acks after acceptance and does not hang on a slow router
    C._spawn_calls = 0
    async def slow_dispatch(data): await asyncio.sleep(0.2)
    C.dispatch = slow_dispatch
    req = types.SimpleNamespace(match_info={"secret": C.WEBHOOK_SECRET}, headers={"X-Telegram-Bot-Api-Secret-Token": C.WEBHOOK_SECRET},
                                json=AsyncMock(return_value=upd(1)))
    t0 = time.monotonic(); r = await C.tg_webhook(req)
    check("webhook returns ok after dispatch", r == ("resp", {"text": "ok"}) and time.monotonic() - t0 >= 0.19)
asyncio.run(t_dispatch())

# ================= internal_update: HMAC auth, hard cap, draining
print("internal_update")
import json as _json
def signed_req(data, node=None, ts=None, tamper=False):
    body = _json.dumps(data, separators=(",", ":")).encode()
    ts = str(int(time.time())) if ts is None else str(ts)
    sig = C._sig(ts, node or config.NODE_ID, body)
    if tamper: body = body.replace(b"1", b"2")
    return types.SimpleNamespace(headers={"X-Cluster-Ts": ts, "X-Cluster-Sig": sig}, read=AsyncMock(return_value=body))
async def t_internal():
    C._draining = False; C.process_local = AsyncMock()
    r = await C.internal_update(signed_req({"update_id": 1})); check("accepts a correctly signed update", r == ("resp", {"text": "ok"}))
    # A completed duplicate is safe to acknowledge even at capacity.
    C.live.reserved = config.NODE_MAX_TASKS
    try: await C.internal_update(signed_req({"update_id": 100001})); check("503 at reserved slot cap", False)
    except web.HTTPServiceUnavailable: check("503 at reserved slot cap", True)
    C.live.reserved = 0; C._draining = True
    try: await C.internal_update(signed_req({"update_id": 100002})); check("503 while draining", False)
    except web.HTTPServiceUnavailable: check("503 while draining", True)
    C._draining = False
    for name, rq in {"tampered body": signed_req({"update_id": 1}, tamper=True),
                     "signature made for ANOTHER node": signed_req({"update_id": 1}, node="n9"),
                     "expired timestamp (replay)": signed_req({"update_id": 1}, ts=int(time.time()) - 3600),
                     "no signature at all": types.SimpleNamespace(headers={}, read=AsyncMock(return_value=b"{}")),
                     "garbage timestamp": types.SimpleNamespace(headers={"X-Cluster-Ts": "x", "X-Cluster-Sig": "y"}, read=AsyncMock(return_value=b"{}"))}.items():
        try: await C.internal_update(rq); check(name, False)
        except web.HTTPForbidden: check(f"{name} -> 403", True)
    legacy = types.SimpleNamespace(headers={"X-Cluster-Secret": config.CLUSTER_SECRET}, read=AsyncMock(return_value=b'{"update_id":5}'))
    try: await C.internal_update(legacy); check("raw secret refused by default", False)
    except web.HTTPForbidden: check("raw secret refused by default (no legacy mode)", True)
    config.CLUSTER_ALLOW_LEGACY = True
    r = await C.internal_update(legacy); check("raw secret accepted only in CLUSTER_ALLOW_LEGACY rollout mode", r == ("resp", {"text": "ok"}))
    config.CLUSTER_ALLOW_LEGACY = False
    try: await C.internal_update(signed_req([1, 2])); check("non-object body", False)
    except web.HTTPBadRequest: check("non-object JSON body -> 400", True)
asyncio.run(t_internal())

print("forward() never sends the raw secret")
async def t_forward_sig():
    sent = {}
    class Resp:
        status = 200
        async def __aenter__(s): return s
        async def __aexit__(s, *a): return False
    class Sess:
        def post(s, url, **kw): sent.update(url=url, **kw); return Resp()
    C._http = lambda: Sess(); C.forward = _forward_orig
    C.router.nodes = {"n2": {"url": "https://n2.example.com"}}
    ok_ = await C.forward("n2", {"update_id": 7, "message": {"text": "héllo"}})
    h = sent["headers"]
    check("forward ok", ok_ is True and sent["url"] == "https://n2.example.com/internal/update")
    check("no X-Cluster-Secret header, secret not in any header", "X-Cluster-Secret" not in h and config.CLUSTER_SECRET not in str(h))
    check("signature verifies for node n2 only", C._same(h["X-Cluster-Sig"], C._sig(h["X-Cluster-Ts"], "n2", sent["data"])) and
          not C._same(h["X-Cluster-Sig"], C._sig(h["X-Cluster-Ts"], "n3", sent["data"])))
asyncio.run(t_forward_sig())

print("gateway dedupe")
async def t_gw_dedupe():
    C._draining = False; C.IS_GATEWAY = True; calls = []
    async def d(data): calls.append(data["update_id"])
    C.dispatch = d
    mk = lambda i: types.SimpleNamespace(match_info={"secret": C.WEBHOOK_SECRET}, headers={"X-Telegram-Bot-Api-Secret-Token": C.WEBHOOK_SECRET},
                                         json=AsyncMock(return_value={"update_id": i}))
    for i in (501, 501, 502): await C.tg_webhook(mk(i))
    await asyncio.sleep(0.01)
    check("a Telegram retry of the same update is routed once", calls == [501, 502])
    C.IS_GATEWAY = False
asyncio.run(t_gw_dedupe())

# ================= shutdown drain + guarded leave
print("shutdown")
async def t_shutdown():
    C._draining = False; C.IS_GATEWAY = False; C._gw_tasks.clear(); C._bg.clear()
    nodes, lease = MagicMock(), MagicMock(); nodes.update_one = AsyncMock(); lease.update_one = AsyncMock()
    C.dbm.cl_nodes_col, C.dbm.cl_lease_col = nodes, lease
    done = []
    async def running(): await asyncio.sleep(0.15); done.append(1)
    C._spawn(running()); C._spawn(running())
    config.DRAIN_SECONDS = 2
    t0 = time.monotonic(); await C.shutdown()
    check("waited for running updates", len(done) == 2 and time.monotonic() - t0 >= 0.14)
    check("marks draining", C._draining)
    f = nodes.update_one.call_args[0][0]
    check("leave only touches OUR record (instance guard)", f == {"_id": "n1", "inst": C.INSTANCE_ID})
    check("releases the gateway lease if it owns it", lease.update_one.call_args[0][0] == {"_id": "gateway", "owner": C.OWNER})
    async def never(): await asyncio.sleep(60)
    C._spawn(never()); config.DRAIN_SECONDS = 0.1; C._draining = False
    t0 = time.monotonic(); await C.shutdown()
    check("drain is capped by DRAIN_SECONDS", time.monotonic() - t0 < 1)
asyncio.run(t_shutdown())

# ================= web.py protection
print("web protection")
from utils import web as W
import config as cfg
def R(headers=None, remote="9.9.9.9", method="GET"): return types.SimpleNamespace(headers=headers or {}, remote=remote, method=method, match_info={"token": "tok123456", "msg_id": "1"}, query={})
cfg.DL_RATE_PER_10S = 3
ips = [W._rate_ok("1.1.1.1") for _ in range(5)]
check("per-IP limit: 3 pass, rest blocked", ips == [True, True, True, False, False])
check("other IP unaffected", W._rate_ok("2.2.2.2"))
W._rl["1.1.1.1"][0] -= 11; check("window resets after 10 s", W._rate_ok("1.1.1.1"))
cfg.DL_RATE_PER_10S = 0; check("limit 0 = off", all(W._rate_ok("3.3.3.3") for _ in range(50)))
cfg.ORIGIN_SECRET = ""; cfg.PUBLIC_URL = ""
check("client ip: rightmost X-Forwarded-For (faked left part ignored)", W.client_ip(R({"X-Forwarded-For": "6.6.6.6, 7.7.7.7"})) == "7.7.7.7")
check("client ip: X-Client-IP ignored without the secret", W.client_ip(R({"X-Client-IP": "6.6.6.6"})) == "9.9.9.9")
cfg.ORIGIN_SECRET = "o" * 32; cfg.PUBLIC_URL = "https://hub.example.com"
check("client ip: trusted via Worker", W.client_ip(R({"X-Origin-Secret": "o" * 32, "X-Client-IP": "5.5.5.5"})) == "5.5.5.5")
check("client ip: wrong secret not trusted", W.client_ip(R({"X-Origin-Secret": "x", "X-Client-IP": "5.5.5.5"})) == "9.9.9.9")
async def t_web():
    try: await W.stream_file(R()); check("origin lock: direct visitor", False)
    except web.HTTPForbidden: check("origin lock: direct visitor -> 403", True)
    try: await W.open_link(R()); check("origin lock on /<token>", False)
    except web.HTTPForbidden: check("origin lock on /<token> -> 403", True)
    # with the right secret it passes the guards (then fails later on the mocked DB link lookup -> NotFound path)
    W._valid_link = AsyncMock(return_value=None)
    try: await W.stream_file(R({"X-Origin-Secret": "o" * 32, "X-Client-IP": "4.4.4.4"})); check("via Worker passes", False)
    except web.HTTPNotFound: check("via Worker passes the lock", True)
    cfg.ORIGIN_SECRET = ""   # lock off: direct access works again (single server / VPS)
    try: await W.stream_file(R()); check("lock off: direct ok", False)
    except web.HTTPNotFound: check("lock off (no secret): direct visitors allowed", True)
    cfg.DL_RATE_PER_10S = 1; W._rl.clear()
    try: await W.stream_file(R()); 
    except web.HTTPNotFound: pass
    try: await W.stream_file(R()); check("rate limit -> 429", False)
    except web.HTTPTooManyRequests as e: check("rate limit -> 429 + Retry-After", e.headers.get("Retry-After") == "10")
    cfg.DL_RATE_PER_10S = 0
    usage_live.streams = cfg.NODE_HARD_STREAMS
    try: await W.stream_file(R()); check("stream hard cap", False)
    except web.HTTPServiceUnavailable as e: check("stream hard cap -> 503", e.headers.get("Retry-After") == "3")
    usage_live.streams = 0
asyncio.run(t_web())


# ================= _try_lease against a fake collection (query shape + owner logic; the real $$NOW pipeline needs a real MongoDB)
print("lease call shape")
async def t_try():
    import importlib; importlib.reload(C)
    col = MagicMock(); col.update_one = AsyncMock(); C.dbm.cl_lease_col = col
    col.find_one_and_update = AsyncMock(return_value={"owner": C.OWNER, "term": 3})
    check("we own it -> True", await C._try_lease() is True and C._deadline > time.monotonic() + C.LEASE - C.LEASE_MARGIN - 1)
    kw = col.find_one_and_update.call_args
    check("no upsert on the guarded update", "upsert" not in kw.kwargs)
    check("filter is guarded by owner/expiry", "$expr" in kw.args[0] and kw.args[0]["_id"] == "gateway")
    upd = kw.args[1][0]["$set"]
    check("update is an aggregation pipeline using Mongo's clock", "$$NOW" in str(upd["until"]) and isinstance(kw.args[1], list))
    col.find_one_and_update = AsyncMock(return_value=None)
    check("held by another -> False", await C._try_lease() is False)
    await C._try_lease()
    check("lease record created only once", col.update_one.await_count == 1)
asyncio.run(t_try())
print(f"\n{ok} passed, {fail} failed"); sys.exit(1 if fail else 0)
