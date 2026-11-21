# Forward-only build

Source: File-Save-ImageVideo-Feature-Merged.zip (local uploaded project artifact).

- Removed Pyrogram MTProto streaming engine, its startup/shutdown, dependencies and /dl endpoint.
- Removed stream/download buttons, delivery selection and server-side streaming settings.
- Removed Autoposter range-import mode (depended on MTProto); manual send/forward collection remains.
- Removed user-account transfer feature (depended on Pyrogram). Existing backup and restore remain (they can fetch small backup archives via Telegram Bot API, not public media streaming).
- Preserved Telegram Bot API file delivery, MongoDB, ad gate, autoposter manual collection, cluster, Cloudflare Worker permanent links, and webhook.
- Removed incompatible POST /_register call; the Worker proxies GET permanent links to origin, which redirects to current bot.
- The Worker rejects legacy /dl paths.

Deploy updated `cloudflare-worker/hub-router.js` to Cloudflare as well as updated Python app to Koyeb.
Existing database `delivery_mode=stream` values are ignored by forward-only delivery.
The Telegram Bot API still has its own rate limits. Do not repeatedly restart on FloodWait.
