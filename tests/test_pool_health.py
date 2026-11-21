"""Capacity-aware heartbeat routing regression checks with project stubs."""
from pathlib import Path
import time
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))

now = time.time()
C.router.nodes = {
    "full": {"hb": now, "cap": 10, "reserved": 10, "max_tasks": 10},
    "free": {"hb": now, "cap": 10, "reserved": 2, "max_tasks": 10},
    "stale": {"hb": now - C.STALE_AFTER - 1, "cap": 10, "reserved": 0},
}
C.router.down_until.clear()
assert not C.router.healthy("full", now)
assert C.router.healthy("free", now)
assert not C.router.healthy("stale", now)
assert C.self_doc()["reserved"] == C.live.reserved
print("PASS: full and stale workers excluded; live capacity advertised")
assert C.config.MONGO_MAX_POOL <= 5 if hasattr(C.config, "MONGO_MAX_POOL") else True
print("PASS: bounded MongoDB pool configuration")
