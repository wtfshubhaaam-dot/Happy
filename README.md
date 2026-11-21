# File Sharing Telegram Bot (button-only)

Async Python bot (python-telegram-bot v21 + MongoDB/Motor). The **only typed command is `/start`**;
everything else is inline buttons and conversational prompts. Settings live in MongoDB and are cached in memory.

```
main.py              entry point (polling + health server + auto-delete worker)
config.py            env vars (BOT_TOKEN, API_ID, API_HASH, MONGO_URI, OWNER_ID)
database/db.py      Motor collections + settings cache
handlers/start.py   /start, force-sub gate, "I've Joined", file delivery
handlers/admin.py   the whole admin panel (buttons + state machine)
handlers/upload.py  admin uploads, batch mode, link creation
handlers/broadcast.py  broadcast with progress + flood control
handlers/usage.py   VPS Usage + Contabo plan advice screens (owner only)
utils/usage.py      CPU/RAM/bandwidth sampler, hourly stats in MongoDB, plan recommender
utils/              helpers, keyboards, membership check, scheduler, web server (/health, /dl, /ad)
```

## 1. Get the credentials
| Variable | Where |
|---|---|
| `BOT_TOKEN` | Telegram **@BotFather** -> /newbot |
| `API_ID`, `API_HASH` | https://my.telegram.org -> API development tools (optional for this PTB build) |
| `MONGO_URI` | MongoDB Atlas free cluster -> Connect -> Drivers (allow your host IP / 0.0.0.0/0) |
| `OWNER_ID` | Message **@userinfobot** to see your numeric ID |

## 2. Deploy on a VPS (recommended: Docker + automatic HTTPS)
1. Point a domain / subdomain (A record) to your VPS IP. Open ports **80** and **443** in the VPS firewall.
2. In MongoDB Atlas -> Network Access, allow the VPS IP (or 0.0.0.0/0).
3. Upload this folder to the VPS, then:
```bash
cp .env.example .env && nano .env     # BOT_TOKEN, OWNER_ID, MONGO_URI, API_ID, API_HASH, DOMAIN
bash deploy.sh                        # installs Docker if needed, builds, starts bot + Caddy (HTTPS)
```
Update later: upload the new files and run `bash deploy.sh` again. Logs: `docker compose logs -f bot`.

`BASE_URL` is set automatically to `https://$DOMAIN` (needed for Monetag ads and Stream/Download links).
Only **one** instance may run per `BOT_TOKEN` (stop old deployments on Koyeb/Replit/Render first).

**Without Docker:**
```bash
python3 -m venv venv && . venv/bin/activate && pip install -r requirements.txt tgcrypto && python main.py
```
Put your own reverse proxy (nginx/Caddy, `proxy_buffering off`) in front of port 8080 and set `BASE_URL`.

**Koyeb / Render / Heroku:** deploy as a Docker service, add the env vars, health check path `/healthz` (liveness; `/health` also pings MongoDB).

## 2b. Several free Koyeb servers behind ONE bot (multi-server mode)
One bot token can receive updates in one place only, so one server is the active **gateway** and the others are **workers**.
Put `NODE_ROLE=gateway` on **two** servers: they compete for a lease in MongoDB, the winner is active, the other is a hot standby
that takes over automatically. Full checklist: `FIX-MULTISERVER.md`.
Telegram -> gateway -> the gateway sends each user to the least-loaded server (active users, running tasks,
Stream/Download viewers, CPU, RAM) and keeps him there until he is inactive for `ACTIVE_WINDOW` seconds (default 300).
That server handles the whole request; Stream/Download links use **its own** domain, so file traffic is spread.
All servers share the same MongoDB.

| Variable | Gateway (account 1) | Worker (account 2, 3, ...) |
|---|---|---|
| `BOT_TOKEN`, `MONGO_URI`, `OWNER_ID`, `API_ID`, `API_HASH` | same | same |
| `CLUSTER_SECRET` | min 24 random characters (`openssl rand -hex 32`) | **same text** |
| `NODE_ID` | `n1` | `n2`, `n3`, ... (unique) |
| `NODE_ROLE` | `gateway` (a 2nd server may also be `gateway` = standby) | `worker` |
| `BASE_URL` | its own `https://...koyeb.app` | its own `https://...koyeb.app` |
| `NODE_CAPACITY` (optional) | users per server, default 10 | same |

**Server names:** every server has a name users can see (`NODE_NAME`, e.g. `🔥 Phoenix`; empty = automatic: n1 ⚡ Falcon,
n2 🔥 Phoenix, n3 🌩 Thunder, n4 🐉 Dragon...). When a user connects (or comes back after being inactive) he sees a short
"Connecting… → Connected: <server>" animation, and the daily-limit message shows which server he is on.

**Platforms:** Koyeb and Render free plans can be mixed. Render sets `PORT` and `RENDER_EXTERNAL_URL` itself (leave `BASE_URL`
empty there; `render.yaml` is included). Render free sleeps after 15 min without traffic: the gateway pings every worker
every 4 min, so only the gateway needs the UptimeRobot monitor. Put the gateway on the most stable platform.

Koyeb: Docker service, port 8080, health check path `/healthz`. Deploy the workers first, the active gateway last.
A new worker shows up in Panel -> **Servers** after about 10 seconds; no other server is restarted.
Add a free UptimeRobot monitor on `<gateway BASE_URL>/health` (every 5 min): the gateway then keeps the workers awake too.
Panel -> **Servers** shows load per server, today's hourly peak, 7-day peaks and how many servers you need.
Notes: admin panel, auto-delete, autoposter and backups always run on the active gateway. If it dies, the standby gateway takes
over (~40 s; Telegram keeps the updates and delivers them). With only ONE gateway the bot is down until it is back.
If a worker dies, its users move to another server. A normal redeploy drains running requests first; a hard crash can lose the
requests being processed at that moment.

## 3. First-time setup (inside Telegram)
1. Create a **private channel** for storage and add the bot as **admin** (Post Messages).
2. Send `/start` as the owner -> Admin Panel -> **Database Channel** -> forward any message from that channel.
3. Add your force-sub channels (bot must be admin in each). Done - send any file to get a link.

## 4. Panel buttons
- **Create Batch** - *Send files* then **Done** = one link for all; or forward the **first + last** post from the DB channel.
- **My Links** - list, view views/count, **Revoke** any link (owner sees all links).
- **Database Channel** - forward a message from it / send ID / @username; bot verifies admin rights.
- **Force-Sub** - add/remove channels (public or private), reorder with up/down, enable/disable.
- **Start Image / Caption** - photo + HTML text; placeholders `{first_name}`, `{mention}`.
- **Join Buttons** - channel buttons are automatic; add custom ones as `Text - URL` (`|` for same row).
- **Auto-Delete** - Off / 5 / 10 / 30 min (survives restarts). **Protect Content** - blocks forwarding/saving.
- **File Caption** - custom caption, or strip the original caption.
- **Broadcast** - send/forward a message, confirm, watch live progress (flood-safe).
- **Stats** - users, files, links, banned, blocked. **Ban/Unban** - forward a message or send an ID.
- **Admins** (owner only) - add/remove by forwarded message or ID. **Reset** - defaults (keeps admins + DB channel).

## 5. Behaviour notes
- **Daily limit = 10 links per user** (1 link = 1 count, every file inside the link is delivered). 5 second cooldown between two link opens. Admins are exempt.
- Users get files **only** via `t.me/<bot>?start=<token>`; tokens are random, stored in MongoDB.
- Non-admin uploads/messages are ignored. Admins skip force-sub.
- If the bot loses admin rights in a channel, users are not blocked and the owner gets a warning message.
- Hidden-forward users/channels can't be read from forwards: send the numeric ID instead.

## 🔄 Transfer controls (Panel → Transfer)
- **⏸ Pause / ▶️ Resume** – freeze the running transfer and continue exactly where it stopped.
- **⏹ Stop** – ends the job, progress is saved (start the same source → destination again to resume).
- **❌ Cancel** – ends the job and throws the saved progress away.
- **🐢 / ⚖️ / ⚡ Speed** – change the pause between requests live.
- **Live counts** – videos, photos, files, audio, text … found in the source vs. transferred, plus *Not forwarded* (with reason, message links and **🔁 Retry failed**).
- **🗑 Delete copied files** – removes exactly the messages the bot copied into a destination in one go (ids are stored in MongoDB, so it also works after a restart). **🧹 Wipe a whole chat** deletes *everything* in a chat (e.g. Saved Messages) after a confirmation.

## 🖥 VPS Usage & Contabo plan advice (Panel → VPS Usage, owner only)
- The bot samples CPU, RAM, network speed and live streaming viewers every 15 s and saves one small record per hour (kept ~13 months, IST days).
- **Daily** (7 days) · **Weekly** (8 weeks) · **Monthly** (6 months) tables: CPU/RAM average + peak, data sent, highest Mbps. Monthly also shows the busiest hours and weekdays.
- **💡 Plan advice** picks the cheapest Contabo plan that fits your real peaks (70 % CPU target, +40 % RAM, +30 % disk/speed headroom), tells which plan your VPS looks like and how much you would save. It needs ~2 weeks of data to be reliable.
- Plan prices are approximate: check https://contabo.com/en/pricing/ and edit `DEFAULT_PLANS` in `utils/usage.py`, or set `CONTABO_PLANS_JSON` in `.env`.
- Files sent inside Telegram chats use Telegram's servers, not your VPS. Only Stream/Download links, ads page and uploads use VPS bandwidth.
