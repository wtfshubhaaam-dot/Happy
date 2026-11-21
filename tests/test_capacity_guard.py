"""Focused capacity guard checks using the existing multi-server test stubs."""
import asyncio
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))

async def main():
    C._draining = False
    C.live.reserved = C.config.NODE_MAX_TASKS
    C._seen_set.add(987654)
    class Request:
        headers = {}
        async def read(self):
            return b'{"update_id":987654}'
    C._signed_ok = lambda headers, raw: True
    response = await C.internal_update(Request())
    print("stub response:", response)
    assert response == ("resp", {"text": "ok"}) or getattr(response, "status", None) == 200
    print("PASS: completed duplicate acknowledged even at full capacity")
    C._seen_set.discard(987654)
    try:
        await C.internal_update(Request())
    except Exception as e:
        assert type(e).__name__ == "HTTPServiceUnavailable"
        print("PASS: new update rejected when capacity full")
    else:
        raise AssertionError("full server accepted new update")
    assert C.live.reserved == C.config.NODE_MAX_TASKS

asyncio.run(main())
