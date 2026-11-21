"""Adaptive advisory is conservative, capacity-aware and requires sustained load."""
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))
snap = {"up": 5, "capacity": 50, "active": 45}
quiet = {"requests": 0, "p95": 0.0, "busy": 0, "errors": 0}
assert C.scale_recommendation(snap, quiet, 3) == 0
assert C.scale_recommendation(snap, quiet, 4) == 1
snap["active"] = 50
assert C.scale_recommendation(snap, quiet, 4) == 2
snap["active"] = 20
assert C.scale_recommendation(snap, quiet, 4) == 0
slow = {"requests": 25, "p95": 2.1, "busy": 0, "errors": 0}
assert C.scale_recommendation(snap, slow, 4) == 1
slow["requests"] = 3
assert C.scale_recommendation(snap, slow, 4) == 0
slow["errors"] = 30
assert C.scale_recommendation(snap, slow, 4) == 0
slow["busy"] = 4
assert C.scale_recommendation(snap, slow, 4) == 1
assert C.scale_recommendation({"up": 0, "capacity": 0, "active": 0}, slow, 4) == 0
print("PASS: 5-server recommendations, sustained pressure, latency sample gate, and network-error safety")
