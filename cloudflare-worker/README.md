# One domain (hubpa.com) for all servers

Links become:  https://hubpa.com/<filecode>  and  https://hubpa.com/dl/<token>/<msg_id>

## A. Domain -> Cloudflare (domain stays at Hostinger)
1. Cloudflare dashboard -> Add a site -> hubpa.com -> Free plan.
2. Cloudflare shows 2 nameservers. Hostinger -> Domains -> hubpa.com -> DNS / Nameservers -> Change nameservers -> paste them.
3. Wait until Cloudflare says "Active" (5 min - a few hours).
4. Cloudflare -> DNS -> Add record: Type A, Name @, IPv4 192.0.2.1, Proxy ON (orange cloud).
   Same for Name www (or a CNAME to hubpa.com), Proxy ON. (The IP is a dummy, the Worker answers before it is used.)

## B. Create the Worker
1. Cloudflare -> Workers & Pages -> Create -> Worker -> name "hub-router" -> Deploy -> Edit code.
2. Delete everything, paste the content of hub-router.js, Deploy.
3. Worker -> Settings -> Variables and Secrets -> add:
   ORIGINS = https://s1.koyeb.app,https://s2.koyeb.app,https://s3.koyeb.app,https://xxx.onrender.com
   DL_MODE = proxy
4. Worker -> Settings -> Domains & Routes -> Add -> Route: hubpa.com/*  (zone hubpa.com). Add www.hubpa.com/* too.

## C. Bot servers (env, then redeploy)
All servers:      PUBLIC_URL=https://hubpa.com
Each server keeps its OWN BASE_URL (its koyeb/render URL) - the gateway needs it for the Telegram webhook.
Render: BASE_URL empty is fine (auto).
Rules: NODE_ROLE=gateway on two servers (active + standby), unique NODE_ID on every server (see ../FIX-MULTISERVER.md).

### Optional but recommended: lock the servers to this Worker
1. Make a random text: `openssl rand -hex 32`.
2. Worker -> Settings -> Variables and Secrets -> add a **Secret** `ORIGIN_SECRET` = that text (CLI: `wrangler secret put ORIGIN_SECRET`).
3. Same text as `ORIGIN_SECRET` env on every bot server (PUBLIC_URL must be set there too), redeploy them.
   Now `/dl/...` and `/<token>` on a koyeb / render address answer 403 unless the request came through the Worker.
   `DL_MODE=redirect` cannot be combined with this (the Worker then uses proxy mode by itself).

## D. Test
- https://hubpa.com/health  -> JSON {"status":"ok"...} (answered by one of your servers; reload: "node" changes)
- Create a link in the bot -> must start with https://hubpa.com/
- Open it -> Telegram opens the bot -> files / Stream button; Stream URL must start with https://hubpa.com/dl/
- Switch one server off -> links keep working (Worker tries the next server).
- Download a file longer than 30 seconds (big video) -> it must finish (the Worker no longer cuts the body after 25 s).
- With ORIGIN_SECRET: `https://<server>.koyeb.app/<filecode>` -> 403, `https://hubpa.com/<filecode>` -> works.

Notes: DL_MODE=proxy sends video bytes through Cloudflare (user only sees hubpa.com). If you ever see Cloudflare
limits/complaints, set DL_MODE=redirect: share links stay on hubpa.com, only the Stream/Download request jumps to a server URL.
Old links created before this change keep their old address.
