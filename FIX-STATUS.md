# Reliability patch status

Implemented: worker-side orphan inbox recovery. A worker checks MongoDB heartbeats periodically and can atomically claim queued work assigned to a node whose heartbeat is older than the configured threshold. Processing updates are reclaimable only after their processing lease expires. If health checks fail, the worker will not steal work. Existing node ID / emoji names, scaling advice, alerts, and Cloudflare Worker are retained. README default-capacity text corrected.

Limits: This is not an exactly-once delivery guarantee. Telegram API side effects may repeat after crash. MongoDB clock/heartbeat race and processing lease edge cases require real integration testing. Gateway failover architecture is unchanged and not independently load-tested. Cloudflare Worker retains the three-origin request-attempt limit to avoid amplifying traffic. Single-process capacity guard remains; multi-host distributed strict enforcement is not guaranteed.

Tests: Python compilation PASS; durable inbox targeted PASS; orphan selection targeted PASS; MongoDB router 38 PASS; node status, scaling, gateway hardening targeted PASS. Existing multi-server test suite fails due to outdated MagicMock expectation for async durable write; not counted as passing. Live 5-server and 500-user testing NOT performed.

Do not claim fully fixed or production ready.
