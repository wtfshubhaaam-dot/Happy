"""Focused regression tests for ambiguous handoff; uses existing dependency stubs."""
import asyncio
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))
C.router.nodes = {
    "n1": {"url": "https://n1.example.com", "hb": time.time(), "cap": 10},
    "n2": {"url": "https://n2.example.com", "hb": time.time(), "cap": 10},
}
C.router.users.clear()
C.router.down_until.clear()
C._draining = False
C._is_admin = lambda uid: False

def upd(user):
    return {"update_id": user, "message": {"from": {"id": user}, "chat": {"type": "private"}}}

async def verify():
    C.router.users[51] = ["n2", time.time(), 0]
    calls = []
    async def lost_ack(nid, data):
        calls.append(nid)
        return False
    C.forward = lost_ack
    for _ in range(2):
        try:
            await C.dispatch(upd(51))
            raise AssertionError("Unacknowledged delivery must fail")
        except C.CapacityUnavailable:
            pass
    assert calls == ["n2", "n2"], calls
    assert "n2" not in C.router.down_until
    print("PASS: ambiguous handoff retries remain on original worker")

    C.router.users[52] = ["n2", time.time(), 0]
    async def busy(nid, data):
        return "busy"
    C.forward = busy
    try:
        await C.dispatch(upd(52))
        raise AssertionError("Busy worker must fail")
    except C.CapacityUnavailable:
        pass
    assert "n2" in C.router.down_until
    assert 52 not in C.router.users
    print("PASS: explicit 503 clears affinity for safe reassignment")

asyncio.run(verify())
