# Automatic server recommendation (starts with 5 servers)

Deploy only five servers now, with unique NODE_ID=n1..n5. Set NODE_ROLE=gateway on gateway-capable node(s), and worker on others, using existing deployment instructions. Do not set NODE_NAME: automatic emoji names remain supported.

The gateway samples every 15 seconds and privately notifies OWNER_ID when pressure persists for at least 4 samples (~60 seconds). It recommends a calculated number of additional servers based on active pinned users and 25% desired headroom, or one server for sustained gateway-to-worker acknowledgement latency (>=1.5s p95, >=20 samples in last 120s) or explicit 503 overload (>=3 in 120s). Notices are rate-limited to about 30 minutes. Existing helper also rate-limits by key. No automatic provisioning or public user messages.

Sample: '⚡ Bot par load badh raha hai. 2 aur server add karo. Abhi 5 online hain.'

No new MongoDB writes per request; a bounded 240-element in-memory rolling deque stores handoff outcomes. No extra MongoDB URL required. The existing heartbeat and stats mechanisms are reused.

IMPORTANT: Gateway-to-worker handoff time is not end-to-end Telegram response time. A slow MongoDB query, media transfer, Telegram API delay or ad challenge may not be detected by this metric. Network outages alone intentionally do not trigger a server recommendation. Suggestions are estimates, not capacity guarantees.

Verified: syntax compilation, scaling-advisor targeted tests, multi-server regression 74/74, MongoDB regression passing. Real deployed 5-server testing not run. No server auto-deployment implemented.
