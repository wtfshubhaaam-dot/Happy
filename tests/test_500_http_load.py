"""Local synthetic HTTP concurrency benchmark; NOT a Telegram/production load test.

Run: python tests/test_500_http_load.py
Uses 50 in-process simulated nodes, each with 10 active slots.
"""
import asyncio
import json
import time
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout

NODES = 50
USERS = 500
PER_NODE = 10

async def main():
    active = [0] * NODES
    peak = [0] * NODES
    accepted = set()
    rejected = 0
    lock = asyncio.Lock()
    release = asyncio.Event()
    all_entered = asyncio.Event()

    async def handle(request):
        nonlocal rejected
        data = await request.json()
        user = data["user"]
        node = user // PER_NODE
        if not (0 <= user < USERS):
            return web.Response(status=400)
        async with lock:
            if active[node] >= PER_NODE:
                rejected += 1
                return web.Response(status=503)
            active[node] += 1
            peak[node] = max(peak[node], active[node])
            accepted.add(user)
            if len(accepted) == USERS:
                all_entered.set()
        try:
            await asyncio.wait_for(release.wait(), timeout=20)
            return web.json_response({"user": user, "node": node})
        finally:
            async with lock:
                active[node] -= 1

    app = web.Application()
    app.router.add_post("/synthetic", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, backlog=1024)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    started = time.monotonic()
    try:
        connector = TCPConnector(limit=0)
        async with ClientSession(connector=connector, timeout=ClientTimeout(total=30)) as session:
            async def send(user):
                async with session.post(f"http://127.0.0.1:{port}/synthetic", json={"user": user}) as r:
                    return r.status, await r.json() if r.status == 200 else None
            tasks = [asyncio.create_task(send(user)) for user in range(USERS)]
            await asyncio.wait_for(all_entered.wait(), timeout=15)
            assert len(accepted) == USERS, f"admitted {len(accepted)}"
            assert all(x == PER_NODE for x in peak), peak
            release.set()
            results = await asyncio.gather(*tasks)
            assert all(status == 200 for status, _ in results)
            assert len({d["user"] for _, d in results}) == USERS
            assert not any(active)
            duration = round(time.monotonic() - started, 3)
            report = {
                "kind": "synthetic_local_http_not_production",
                "concurrent_requests": USERS,
                "simulated_nodes": NODES,
                "per_node_peak": max(peak),
                "successful": sum(status == 200 for status, _ in results),
                "rejected": rejected,
                "elapsed_seconds": duration,
            }
            print(json.dumps(report, indent=2))
    finally:
        release.set()
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
