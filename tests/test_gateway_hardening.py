"""Gateway demotion waits for cancellation; internal payload cap rejects large bodies."""
import asyncio
from pathlib import Path
source = Path(__file__).with_name("test_multiserver.py").read_text()
exec(compile(source.split("# ================= config")[0], "test_multiserver.py", "exec"))
C.web.HTTPRequestEntityTooLarge = type("HTTPRequestEntityTooLarge", (Exception,), {"__init__": lambda self, **kwargs: Exception.__init__(self, "too large")})
async def main():
    C._gw_tasks.clear()
    C.IS_GATEWAY = True
    finished = asyncio.Event()
    async def loop():
        try:
            await asyncio.sleep(3600)
        finally:
            finished.set()
    task = asyncio.create_task(loop())
    await asyncio.sleep(0)
    C._gw_tasks.append(task)
    C._hooks["demote"] = lambda: None
    await C._demote()
    assert finished.is_set() and task.done() and not C.IS_GATEWAY
    print("PASS: gateway demotion waits for background task cleanup")

    class Request:
        content_length = 2 * 1024 * 1024
        headers = {}
        async def read(self):
            raise AssertionError("oversized payload must not be read")
    try:
        await C.internal_update(Request())
    except C.web.HTTPRequestEntityTooLarge:
        print("PASS: oversized internal request rejected before reading")
    else:
        raise AssertionError("oversized request was accepted")
asyncio.run(main())
