"""Heartbeat state transitions, with no Telegram/MongoDB dependencies."""
import time
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))
now = time.time()
monitor = C.NodeStatusAlerts()
nodes = {"n1": {"name": "👑 Royal Phantom", "hb": now}, "n2": {"name": "🔥 Inferno King", "hb": now}}
assert monitor.transitions(nodes, now) == []  # no startup spam
nodes["n2"]["hb"] = now - C.STALE_AFTER - 2
assert monitor.transitions(nodes, now) == []  # one stale sample
assert monitor.transitions(nodes, now + 1) == [("n2", "🔥 Inferno King", False)]
assert monitor.transitions(nodes, now + 2) == []  # no duplicate alert
nodes["n2"]["hb"] = now + 3
assert monitor.transitions(nodes, now + 3) == []
assert monitor.transitions(nodes, now + 4) == [("n2", "🔥 Inferno King", True)]
assert monitor.transitions(nodes, now + 5) == []
print("PASS: startup silence, debounced offline and recovery, no duplicate notices")
