/**
 * hub-router.js - ONE domain in front of ALL bot servers (forward-only).
 *
 *   yourdomain.com/<filecode>            -> any healthy server -> redirects to t.me/<bot>?start=<filecode>
 *
 * Settings (Worker -> Settings -> Variables):
 *   ORIGINS   comma separated server URLs, no trailing slash:
 *             https://a.koyeb.app,https://b.koyeb.app,https://c.koyeb.app,https://d.onrender.com
 *   ORIGIN_SECRET  (optional, a Secret) same random text as ORIGIN_SECRET on every bot server + PUBLIC_URL set there.
 *             The Worker adds it to every request, the servers then refuse direct /<token> from anybody else, so the
 *             koyeb / render addresses cannot be used to bypass this domain. 
 */
// /monetag/postback is the ONE exception: Monetag's server-to-server call (secret in the URL) must survive any single server dying.
const BLOCKED = [/^\/dl(\/|$)/, /^\/tg(\/|$)/, /^\/internal(\/|$)/, /^\/api(\/|$)/, /^\/monetag(?!\/postback$)(\/|$)/];   // never expose these through the domain

// Test the path the way the server will read it: percent-decoded, slashes collapsed, lower case
// (otherwise /%6Donetag/postback or //internal/update would slip past the list above).
function isBlocked(pathname) {
  let p;
  try { p = decodeURIComponent(pathname); } catch (e) { return true; }
  p = p.replace(/\/{2,}/g, "/").toLowerCase();
  return BLOCKED.some(r => r.test(p));
}

function parseOrigins(env) {
  return String(env.ORIGINS || "").split(",").map(s => s.trim().replace(/\/+$/, "")).filter(Boolean);
}

function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

// servers that failed recently are tried last (this isolate only, resets by itself)
const down = new Map();
function order(origins) {
  const now = Date.now();
  const ok = origins.filter(o => (down.get(o) || 0) < now);
  const bad = origins.filter(o => (down.get(o) || 0) >= now);
  return shuffle(ok).concat(shuffle(bad));
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (isBlocked(url.pathname)) return new Response("Not found", { status: 404 });
    if (request.method !== "GET" && request.method !== "HEAD") return new Response("Method not allowed", { status: 405 });

    const origins = parseOrigins(env);
    if (!origins.length) return new Response("ORIGINS is not set in the Worker", { status: 500 });

    let lastStatus = 502;

    // Bound upstream attempts: 50 origins must never mean 50 sequential DB/stream requests.
    // Avoid excessive upstream attempts.
    const candidates = order(origins).slice(0, 3);
    for (const origin of candidates) {
      const target = origin + url.pathname + url.search;
      try {
        const headers = new Headers();
        for (const h of ["accept", "accept-encoding", "user-agent"]) {
          const v = request.headers.get(h);
          if (v) headers.set(h, v);
        }
        if (env.ORIGIN_SECRET) headers.set("X-Origin-Secret", env.ORIGIN_SECRET);          // see ORIGIN_SECRET above
        const ip = request.headers.get("cf-connecting-ip");
        if (ip) headers.set("X-Client-IP", ip);                                             // real visitor, for the per-IP limit
        // The timeout covers ONLY "until the server starts answering". An AbortSignal.timeout() would also cut the
        // body in the middle of a long download / video (everything longer than 25 s), so the timer is cleared as
        // soon as the response headers are there.
        const ctl = new AbortController();
        const timer = setTimeout(() => ctl.abort(), 15000);
        let res;
        try {
          res = await fetch(target, { method: request.method, headers, redirect: "manual", signal: ctl.signal });
        } finally {
          clearTimeout(timer);
        }
        if (res.status === 503 || res.status === 429) {
          // Explicit capacity rejection: safe to try another origin for idempotent GET/HEAD.
          lastStatus = 503;
          down.set(origin, Date.now() + 5000);
          continue;
        }
        if (res.status >= 500) {
          // Ambiguous failure: an origin might have already done work. Do not
          // amplify load by replaying this request against more databases.
          lastStatus = 503;
          break;
        }
        const out = new Response(res.body, res);          // passes 200/206/302/404 + Range headers unchanged
        out.headers.set("X-Served-By", new URL(origin).hostname.split(".")[0]);
        return out;
      } catch (e) {
        down.set(origin, Date.now() + 30000);
        // A timeout can happen after the origin started a DB
        // operation; avoid replaying the same request to another origin.
        lastStatus = 503;
        break;
      }
    }
    return new Response("All servers are busy or waking up, please retry in a few seconds.", {
      status: lastStatus === 502 ? 503 : lastStatus, headers: { "Retry-After": "10" } });
  },
};
