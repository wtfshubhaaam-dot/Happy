# Reliability progress — 2026-11-21

Based on File-Save-Reliability-Recovery-Partial.zip. Existing features retained.

## Verified locally
- Python compileall: PASS
- Durable inbox targeted tests: PASS
- Orphan-node selection targeted tests: PASS
- MongoDB routing assertions: 38 PASS, 0 FAIL
- Added a defensive check for failed durable inbox lease-renewal after handler completion.

## Not verified / blockers
- Multi-server regression fails because the old test uses a synchronous MagicMock for a new async MongoDB update_one operation; regression suite is not green. Do not assume this is the only failure.
- Telegram sendMessage/sendDocument/sendVideo cannot guarantee exactly-once external side effects after an uncertain acknowledgement. A delivery ledger with state and reconciliation can reduce but cannot eliminate duplicates.
- Gateway leader failover has not been tested with live MongoDB and Telegram webhook transitions.
- Per-process capacity limits are not equivalent to distributed per-host guarantees if more than one process runs per server.
- No real five-server load/failover benchmark has been performed.

## Next actions
1. Update async regression fixtures and run entire suite.
2. Add handler-level delivery ledger and explicit duplicate tradeoff policy; test crash at each send boundary.
3. Test worker death, delayed lease renewal, DB partition, gateway lease transition, and webhook registration using real staging services.
4. Enforce one process per node or add shared distributed semaphore and validate with multi-process tests.

This archive is a tested incremental hardening patch, NOT a fully fixed production release.
