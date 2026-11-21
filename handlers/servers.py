"""
handlers/servers.py
Panel -> 🌐 Servers (owner only, multi-server mode).

  summary    every server right now: active users / capacity, tasks, streams, CPU, RAM, state
  📈 Today    hour by hour: average / peak active users (peak hour marked)
  📆 7 Days   one line per day: peak users, peak hour, overflow events
  💡 Scaling  how many servers the real peak needs + how to add one

Numbers come from utils/cluster.py (the gateway samples the cluster every 15 s, hourly documents in MongoDB, IST days).
"""
import datetime as dt
import html
import logging

from telegram import InlineKeyboardMarkup as M

import config
from database import db as dbm
from handlers.admin import B, action, show
from utils import cluster as C
from utils.helpers import owner_only

log = logging.getLogger(__name__)
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _table(rows, widths) -> str:
    out = []
    for r in rows:
        out.append(str(r[0]).ljust(widths[0]) + "".join(str(c).rjust(w) for c, w in zip(r[1:], widths[1:])))
    return "<pre>" + html.escape("\n".join(out)) + "</pre>"


def _kb(active: str = ""):
    return M([
        [B("📈 Today", "adm:srv:t"), B("📆 7 Days", "adm:srv:w")],
        [B("💡 Scaling", "adm:srv:s")],
        [B("🔄 Refresh", f"adm:srv:{active}" if active else "adm:srv"), B("🔙 Back", "adm:home")],
    ])


def _off():
    return ("🌐 <b>Servers</b>\n\nMulti-server mode is <b>OFF</b> (single server).\n\n"
            "To turn it on set these on every server: <code>CLUSTER_SECRET</code>, <code>NODE_ID</code> "
            "(n1, n2, ...), <code>NODE_ROLE</code> (<code>gateway</code> on two servers: one active + one standby, <code>worker</code> on the others) "
            "and each server's own <code>BASE_URL</code>.")


def _need_gateway():
    return ("🌐 <b>Servers</b>\n\nThis screen is only live on the <b>gateway</b> server. "
            "Set <code>NODE_ROLE=gateway</code> on one or two servers (two = automatic failover). "
            "Right now no server holds the gateway lease - check the logs.")


@action("srv")
@owner_only
async def a_srv(update, context, args):
    if not C.ENABLED:
        await show(update, _off(), M([[B("🔙 Back", "adm:home")]]))
        return
    if not C.IS_GATEWAY:
        await show(update, _need_gateway(), M([[B("🔙 Back", "adm:home")]]))
        return
    view = args[0] if args else ""
    if view == "t":
        await _today(update)
    elif view == "w":
        await _week(update)
    elif view == "s":
        await _scaling(update)
    else:
        await _summary(update)


# ------------------------------------------------------------------ helpers
def _ist_today() -> dt.date:
    return dt.datetime.now(dbm.LIMIT_TZ).date()


def _hour_label(h: int) -> str:
    return f"{h:02d}-{(h + 1) % 24:02d}"


def _peak_doc(docs):
    best = None
    for d in docs:
        if best is None or d.get("peak_active", 0) > best.get("peak_active", 0):
            best = d
    return best


def _fmt_peak(d) -> str:
    if not d or not d.get("peak_active"):
        return "—"
    day = dt.date.fromisoformat(d["_id"][:10])
    return f"<b>{d['peak_active']}</b> users ({DAYS[day.weekday()]} {day.strftime('%d %b')}, {_hour_label(int(d['_id'][11:13]))} IST)"


# ------------------------------------------------------------------ screens
async def _summary(update):
    snap = C.router.snapshot()
    rows = [["Node", "Users", "Task", "CPU", "RAM", "State"]]
    for r in snap["nodes"]:
        name = C.short_name(r["name"])[:8] + ("*" if r["gateway"] else ("~" if r.get("standby") else ""))
        rows.append([name, f"{r['users']}/{r['cap']}", r["tasks"], f"{r['cpu']:.0f}%",
                     f"{r['ram']:.0f}%", r["state"]])
    t = _ist_today()
    docs = await C.load_hours(t - dt.timedelta(days=6))
    today_docs = [d for d in docs if d["_id"][:10] == t.isoformat()]
    pct = round(100 * snap["active"] / snap["capacity"]) if snap["capacity"] else 0
    text = (f"🌐 <b>Servers</b> · {snap['up']}/{len(snap['nodes'])} online\n"
            f"<i>Active = used the bot in the last {config.ACTIVE_WINDOW // 60} min</i>\n\n"
            f"⚡ <b>Now:</b> {snap['active']} active / {snap['capacity']} capacity ({pct}%)\n\n"
            + _table(rows, [10, 8, 5, 5, 5, 6])
            + "\n".join(f"{html.escape(r['name'])} = {r['id']}" + (f" · {r['host']}" if r["host"] else "")
                         for r in snap["nodes"]) + "\n"
            + "<i>* = active gateway · ~ = standby gateway · Task = requests being processed · Str = live Stream/Download viewers</i>\n\n"
            f"📈 <b>Peak today:</b> {_fmt_peak(_peak_doc(today_docs))}\n"
            f"📆 <b>Peak 7 days:</b> {_fmt_peak(_peak_doc(docs))}")
    down = [r["id"] for r in snap["nodes"] if r["state"] == "DOWN"]
    if down:
        text += f"\n\n🔴 Down: {', '.join(down)} (users are moved to other servers automatically)"
    await show(update, text, _kb())


def _today_screen_rows(docs):
    rows = [["Hour", "Avg", "Peak", "Ovf"]]
    best = _peak_doc(docs)
    for d in docs:
        n = d.get("samples", 0) or 1
        mark = " ◄" if best and d is best and d.get("peak_active") else ""
        rows.append([_hour_label(int(d["_id"][11:13])), f"{d.get('active_sum', 0) / n:.0f}", d.get("peak_active", 0),
                     d.get("overflow", 0)])
        rows[-1][3] = f"{rows[-1][3]}{mark}"
    return rows


async def _today(update):
    t = _ist_today()
    docs = [d for d in await C.load_hours(t) if d["_id"][:10] == t.isoformat()]
    if not docs:
        await show(update, "📈 <b>Today</b>\n\nNo data yet. The gateway samples every 15 seconds and saves every minute.",
                   _kb("t"))
        return
    rows = _today_screen_rows(docs)
    best = _peak_doc(docs)
    node_ids = sorted({k for d in docs for k in (d.get("n") or {})})
    per_node = ""
    if best and best.get("n"):
        per_node = "\n".join(f"• {nid}: {v.get('users', 0)} users · CPU {v.get('cpu', 0):.0f}% · RAM {v.get('ram', 0):.0f}%"
                             for nid, v in sorted(best["n"].items()))
    await show(update,
               f"📈 <b>Today</b> ({t.strftime('%d %b')}, IST)\n\n" + _table(rows, [7, 6, 6, 7])
               + "<i>Avg / Peak = active users in that hour · Ovf = users that arrived when every server was full</i>\n\n"
               + f"🔥 <b>Peak hour:</b> {_fmt_peak(best)}\n" + (per_node and f"{per_node}\n") +
               f"\nServers seen today: {len(node_ids)}",
               _kb("t"))


async def _week(update):
    t = _ist_today()
    docs = await C.load_hours(t - dt.timedelta(days=6))
    if not docs:
        await show(update, "📆 <b>7 Days</b>\n\nNo data yet.", _kb("w"))
        return
    by_day = {}
    for d in docs:
        by_day.setdefault(d["_id"][:10], []).append(d)
    rows = [["Day", "Peak", "At", "Avg", "Ovf"]]
    for i in range(6, -1, -1):
        day = t - dt.timedelta(days=i)
        ds = by_day.get(day.isoformat(), [])
        if not ds:
            rows.append([f"{DAYS[day.weekday()]} {day.day:02d}", "-", "-", "-", "-"])
            continue
        best = _peak_doc(ds)
        samples = sum(d.get("samples", 0) for d in ds) or 1
        rows.append([f"{DAYS[day.weekday()]} {day.day:02d}", best.get("peak_active", 0),
                     _hour_label(int(best["_id"][11:13])), f"{sum(d.get('active_sum', 0) for d in ds) / samples:.0f}",
                     sum(d.get("overflow", 0) for d in ds)])
    await show(update,
               "📆 <b>Last 7 days</b> (IST)\n\n" + _table(rows, [8, 6, 8, 6, 5])
               + "<i>Peak = most users active together · At = that hour · Avg = average active users · "
                 "Ovf = arrivals while all servers were full</i>",
               _kb("w"))


async def _scaling(update):
    t = _ist_today()
    docs = await C.load_hours(t - dt.timedelta(days=13))
    snap = C.router.snapshot()
    live_nodes = [r for r in snap["nodes"] if r["state"] != "DOWN"]
    cap = round(sum(r["cap"] for r in live_nodes) / len(live_nodes)) if live_nodes else config.NODE_CAPACITY
    have = len(snap["nodes"])
    best = _peak_doc(docs)
    if not best or not best.get("peak_active"):
        await show(update,
                   "💡 <b>Scaling</b>\n\nNot enough data yet. Check again after a busy day.\n\n" + _how_to_add(have),
                   _kb("s"))
        return
    peak = best["peak_active"]
    need = C.servers_needed(peak, cap)
    overflow = sum(d.get("overflow", 0) for d in docs)
    failover = sum(d.get("failover", 0) for d in docs)
    cpu_hi = max((v.get("cpu", 0) for d in docs for v in (d.get("n") or {}).values()), default=0)
    ram_hi = max((v.get("ram", 0) for d in docs for v in (d.get("n") or {}).values()), default=0)

    lines = [f"💡 <b>Scaling</b> (last 14 days)\n",
             f"🔥 Highest load: {_fmt_peak(best)}",
             f"🖥 Servers now: <b>{have}</b> × {cap} users = {have * cap} users",
             f"📐 Needed for that peak (+25% spare): <b>{need}</b> server(s)"]
    if need > have:
        lines.append(f"\n➕ <b>Add {need - have} more server(s)</b> before the next busy hour.")
    else:
        lines.append("\n✅ Enough servers for your busiest hour so far.")
    lines.append(f"\n🔁 Overflow arrivals: {overflow} · Failovers: {failover}")
    lines.append(f"🌡 Hottest server: CPU {cpu_hi:.0f}% · RAM {ram_hi:.0f}%")
    if overflow and cpu_hi < 60 and ram_hi < 70:
        lines.append(f"\n💡 Servers still had spare CPU/RAM when they were full. Try <code>NODE_CAPACITY={int(cap * 1.3)}</code> "
                     "before adding a server.")
    elif cpu_hi >= 85 or ram_hi >= 88:
        lines.append(f"\n⚠️ Servers run hot at peak. Lower <code>NODE_CAPACITY</code> (e.g. {max(5, int(cap * 0.8))}) "
                     "or add a server.")
    lines.append("\n" + _how_to_add(have))
    await show(update, "\n".join(lines), _kb("s"))


def _how_to_add(have: int) -> str:
    return ("<b>Add a server</b> (Koyeb or Render free)\n"
            "1. New free account → deploy this same repo (Dockerfile).\n"
            f"2. Same env as the others, only change: <code>NODE_ID=n{have + 1}</code>, <code>NODE_ROLE=worker</code> "
            "(optional <code>NODE_NAME</code>). On Render the URL is detected automatically; elsewhere set "
            "<code>BASE_URL</code> to the app's own https URL.\n"
            "3. Done. It appears here after its first heartbeat (10 s). Nothing else is restarted.")
