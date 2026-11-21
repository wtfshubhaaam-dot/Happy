# Phase 1 reliability status — 2026-11-21

This is a source patch, not a verified production-ready release.

## Implemented in this patch
- Added a MongoDB CORE `cluster_update_inbox` collection and indexes.
- Gateway-local and internal worker routes persist Telegram update payloads before returning HTTP 200; MongoDB failures return 503 instead of acknowledging.
- Global update_id is the unique inbox key; MongoDB `$setOnInsert` prevents duplicate inbox documents across nodes.
- Worker background poller atomically claims queued or lease-expired updates; local admission still limits to 10 tasks/users.
- On handler success, mark done; on failure, retry with backoff up to 8 attempts and mark failed for operator inspection. Claims are renewed during long-running handlers.
- Existing gateway registration fail-closed and single-process checks from the partial patch are retained.

## Verified locally
- Python `compileall` passed.
- New `tests/test_durable_inbox.py` passed with mocked MongoDB.
- `tests/test_mongo_router.py` passed (38 assertions).
- `tests/test_gateway_hardening.py`, `tests/test_node_names.py`, `tests/test_scaling_advisor.py` passed.

## Still NOT fully fixed / not verified
1. **Exactly-once Telegram side effects**: a crash after sending a file but before marking the inbox record done can send the file twice on retry. Handler-specific idempotency or transactional outbox is still needed. At-least-once processing only.
2. **Permanently dead nodes**: inbox documents are pinned to the original node; there is no safe automatic reassignment of abandoned updates. This is a known availability gap.
3. **Strict 10-user cap across duplicate NODE_ID deployments**: single-process-per-instance check exists, but two separate hosts using the same NODE_ID are not prevented by a distributed ownership lease.
4. **Live gateway failover**: not tested against real Telegram webhooks and MongoDB failover.
5. Existing `tests/test_multiserver.py` and `tests/test_capacity_guard.py` include expectations from the old immediate-processing contract; they currently fail or need refactoring to cover the new durable-inbox contract. Do not report the old 74/74 pass for this patch.
6. No live 5-server or real 500-user deployment test.

Do not deploy as production-ready without resolving these gaps and running integration tests.
