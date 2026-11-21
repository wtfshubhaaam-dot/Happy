# Owner-only offline and recovery alerts

- Automatically detects stale heartbeat (35 seconds), with two consecutive 15-second samples to avoid false alarms.
- Sends a short Telegram owner-only message on offline and recovery transitions, including auto-assigned emoji name, node ID, and online count.
- Startup initializes baseline without spamming notifications. Sleep vs crash cannot be reliably distinguished.
- Uses existing MongoDB heartbeats; no additional URL or per-request DB writes.
- Only active gateway can send these alerts. If every server/gateway is down, the bot cannot notify you; use independent external monitoring for that case.
- Requires configured OWNER_ID, active gateway, and bot Telegram access.
- Notifications may fail if Telegram is unavailable; logs capture the failure.
- Existing smart scaling recommendations remain enabled.
