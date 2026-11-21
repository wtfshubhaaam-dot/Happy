"""
handlers/autoposter.py
Autoposter screens of the admin panel (registered into handlers.admin's ACTIONS / STATES).

  Panel -> 📅 Autoposter -> channel list -> pick a channel:
      ⏰ Timings        several times a day, posts per time (each time has its own count)
      📚 All Posts      every post of this channel (✅ posted / ⏳ pending), open one -> Post now / Delete
      ➕ Create Post    (a) range: first + last message of a source channel  (b) send / forward files, then Done
      🧩 Post type      Classic (caption file = thumbnail, one link)  /  Image + Video (image 1, video 1, image 2 ... =
                        one post with a free image link + an ad-gated video link, one fixed thumbnail 🖼)
      🗂 Queue          shuffled order of the posts (cycle, cursor, up next)
      🔀 Reshuffle      new random order, all posts pending again
      🔁 Mode           loop (reshuffle every cycle) / one pass (stop after every post is posted once)
      ▶️ ON / OFF, 🗑 remove channel
"""
import html
import logging
import re
import time

from telegram import InlineKeyboardMarkup as M
from telegram.constants import ParseMode
from telegram.error import TelegramError

from database import db
from database.db import settings as S
from handlers.admin import B, PAGE, action, ask, ask_chat, resolve_channel, show, state
from handlers.upload import media_info
from utils import autoposter as AP
from utils import keyboards as K
from utils.helpers import call
from utils.links import share_link

log = logging.getLogger(__name__)
MAX_RANGE = 10000


async def _job(coro, bot, chat_id):
    try:
        await coro
    except Exception as e:  # never let a background job die silently
        log.exception("Autoposter job failed")
        try:
            await bot.send_message(chat_id, f"⚠️ Create Post failed: <code>{html.escape(str(e))}</code>")
        except Exception:
            pass


# ===================================================================== channel list
@action("ap")
async def a_ap(update, context, args):
    chans = await db.ap_list_channels()
    rows = [[B(f"📺 {c['title'][:30]}", f"adm:apc:{c['_id']}")] for c in chans]
    rows.append([B("➕ Add Channel / Group", "adm:apadd")])
    rows.append([B("🔙 Back", "adm:home")])
    await show(update,
               "📅 <b>Autoposter</b>\n\nAdd a channel or group, set the timings and the bot posts "
               "your saved posts there automatically.\n\n"
               + (f"{len(chans)} channel(s) added." if chans else "No channel added yet."),
               M(rows))


@action("apadd")
async def a_apadd(update, context, args):
    await ask_chat(update, context, "ap_add",
                   "➕ <b>Add Channel / Group</b>\n\nChoose where the bot should post.\n"
                   "If the bot is not an admin there, Telegram will offer to add it.",
                   channel=True, group=True)


@state("ap_add")
async def st_ap_add(update, context):
    chat = await resolve_channel(update, context, need_post=True)
    if not chat:
        return
    await db.ap_add_channel(chat.id, chat.title or str(chat.id))
    context.user_data.pop("state", None)
    await update.effective_message.reply_text(
        f"✅ Added <b>{html.escape(chat.title or '')}</b>.",
        reply_markup=M([[B("📺 Open", f"adm:apc:{chat.id}")], [B("🔙 Back", "adm:ap")]]))


# ===================================================================== one channel
async def show_channel(update, cid: int, note: str = ""):
    ch = await db.ap_get_channel(cid)
    if not ch:
        await show(update, "⚠️ Channel not found.", K.back("ap"))
        return
    total, posted = await db.ap_counts(cid)
    on = ch.get("enabled", True)
    loop = ch.get("loop", True)
    iv = ch.get("ptype") == "iv"
    text = (f"{note}📺 <b>{html.escape(ch['title'])}</b>\n\n"
            f"Autoposter: {'ON ✅' if on else 'OFF ⏸'}\n"
            f"🧩 New posts: <b>{'🖼🎬 Image + Video' if iv else '📄 Classic'}</b>"
            + (f" · fixed thumbnail {'✅' if ch.get('iv_thumb') else '— (each post shows its own image)'}" if iv else "") + "\n"
            f"⏰ Timings (IST): {AP.fmt_slots(ch.get('slots', []))}\n"
            f"🔀 Order: shuffled · Cycle <b>{(ch.get('queue_state') or {}).get('cycle', 0)}</b> · "
            f"{'🔁 loop' if loop else '➡️ one pass'}\n"
            f"📚 Posts: <b>{total}</b> · ✅ posted {posted} · ⏳ pending {total - posted}")
    await show(update, text, M([
        [B("⏰ Timings", f"adm:aptm:{cid}"), B(f"📚 All Posts ({total})", f"adm:apall:{cid}:0")],
        [B("➕ Create Post", f"adm:apnew:{cid}"), B("🎨 Caption & Button", f"adm:apcb:{cid}")],
        [B(f"🧩 Type: {'Image + Video' if iv else 'Classic'}", f"adm:aptype:{cid}")]
        + ([B("🖼 Fixed Thumbnail", f"adm:apthumb:{cid}")] if iv else []),
        [B("🗂 Queue", f"adm:apq:{cid}"), B("🔀 Reshuffle", f"adm:apresh:{cid}")],
        [B(f"🔁 Mode: {'loop' if loop else 'one pass'}", f"adm:apmode:{cid}"),
         B("⏸ Turn OFF" if on else "▶️ Turn ON", f"adm:apon:{cid}")],
        [B("🧹 Wipe posts", f"adm:apwipe:{cid}")],
        [B("🗑 Remove channel", f"adm:aprm:{cid}")],
        [B("🔙 Back", "adm:ap")],
    ]))


@action("apc")
async def a_apc(update, context, args):
    await show_channel(update, int(args[0]))


@action("apon")
async def a_apon(update, context, args):
    cid = int(args[0])
    ch = await db.ap_get_channel(cid)
    if ch:
        await db.ap_set(cid, enabled=not ch.get("enabled", True))
    await show_channel(update, cid, "✅ Updated.\n\n")


@action("apreset")
async def a_apreset(update, context, args):
    await a_apresh(update, context, args)          # old "mark all pending" button = a fresh reshuffle


# ===================================================================== post type (Classic / Image + Video)
@action("aptype")
async def a_aptype(update, context, args):
    """Switch what ➕ Create Post makes from now on. Posts already created keep their own type, the queue publishes both."""
    cid = int(args[0])
    ch = await db.ap_get_channel(cid)
    if not ch:
        await show(update, "⚠️ Channel not found.", K.back("ap"))
        return
    iv = ch.get("ptype") != "iv"
    await db.ap_set(cid, ptype="iv" if iv else "classic")
    await show_channel(
        update, cid,
        ("🖼🎬 <b>Image + Video</b> posts: send image 1, video 1, image 2, video 2 … in this order (➕ Create Post).\n"
         "Each pair = ONE post with a free image link and an ad-gated video link. Set the 🖼 Fixed Thumbnail once.\n\n"
         if iv else
         "📄 <b>Classic</b> posts: a captioned file = thumbnail, the captionless files below it = the post's link.\n\n"))


@action("apthumb")
async def a_apthumb(update, context, args):
    cid = int(args[0])
    if not S.get("db_channel"):
        await show(update, "⚠️ Set the Database Channel first.", M([[B("📡 Set Database Channel", "adm:dbch")]]))
        return
    context.user_data["ap_cid"] = cid
    await ask(update, context, "ap_thumb",
              "🖼 <b>Fixed thumbnail</b>\n\nSend ONE image (as a photo). Every Image + Video post of this channel is published "
              "with this picture, the caption and the two buttons.\n"
              "Send <code>-</code> to remove it (each post then shows its own image).")


@state("ap_thumb")
async def st_ap_thumb(update, context):
    msg = update.effective_message
    cid = context.user_data.get("ap_cid")
    if cid is None:
        return
    if msg.text and msg.text.strip() == "-":
        await db.ap_set(cid, iv_thumb="")
        context.user_data.pop("state", None)
        await msg.reply_text("✅ Fixed thumbnail removed.", reply_markup=K.back(f"apc:{cid}"))
        return
    channel = S.get("db_channel")
    if not msg.photo or not channel:
        await msg.reply_text("⚠️ Send the thumbnail as a <b>photo</b> (not as a file).", reply_markup=K.cancel())
        return
    token = db.new_token()
    code = db.make_code(token, 0)
    try:    # stored in the Database Channel under a backup code, like every other file (survives a channel restore)
        copied = await call(context.bot.copy_message, channel["id"], msg.chat_id, msg.message_id,
                            caption=f"<code>{code}</code>", parse_mode=ParseMode.HTML)
    except TelegramError as e:
        await msg.reply_text(f"⚠️ Could not save it: <code>{html.escape(str(e))}</code>", reply_markup=K.cancel())
        return
    await db.add_file(copied.message_id, "photo", "post thumbnail", update.effective_user.id, code=code, cap="")
    await db.ap_set(cid, iv_thumb=token)
    context.user_data.pop("state", None)
    await msg.reply_text("✅ Fixed thumbnail saved.", reply_markup=K.back(f"apc:{cid}"))


# ===================================================================== shuffled queue
def _ago(ts) -> str:
    if not ts:
        return "never"
    mins = int((time.time() - ts) // 60)
    return "just now" if mins < 1 else f"{mins} min ago" if mins < 60 else f"{mins // 60} h ago" if mins < 1440 else f"{mins // 1440} d ago"


async def show_queue(update, cid: int, note: str = ""):
    ch = await db.ap_get_channel(cid)
    if not ch:
        await show(update, "⚠️ Channel not found.", K.back("ap"))
        return
    total, posted = await db.ap_counts(cid)
    qs = ch.get("queue_state") or {}
    queue, cursor = qs.get("queue") or [], int(qs.get("cursor", 0))
    nxt = AP.next_slot(ch.get("slots", []))
    batch = int(nxt[1]["n"]) if nxt else 1
    up = await db.ap_upcoming(cid, 10)
    lines = [f"{'▶️' if i < batch else '•'} #{p['seq']} {html.escape(p['title'][:30])}" for i, p in enumerate(up)]
    when = (f"{nxt[0].strftime('%H:%M')} IST ×{nxt[1]['n']}" if nxt else "no timing set")
    await show(update,
               f"{note}🗂 <b>Queue</b> — {html.escape(ch['title'])}\n\n"
               f"⏳ Remaining: <b>{total - posted}</b> · ✅ Posted: <b>{posted}</b>\n"
               f"🔄 Cycle: <b>{qs.get('cycle', 0)}</b> · Cursor: <b>{cursor}/{len(queue)}</b>\n"
               f"Mode: <b>{'🔁 loop' if ch.get('loop', True) else '➡️ one pass'}</b>\n"
               f"Status: <b>{html.escape(str(qs.get('shuffle_status', 'idle')))}</b>\n"
               f"Last posted: <b>{_ago(qs.get('last_posted_at'))}</b>\n"
               f"Next time: <b>{when}</b>\n\n"
               f"<b>Up next</b> (▶️ = goes out at the next time)\n"
               + ("\n".join(lines) if lines else "<i>empty</i>"),
               M([[B("🔀 Reshuffle", f"adm:apresh:{cid}"), B("🔄 Refresh", f"adm:apq:{cid}")],
                  [B("🔙 Back", f"adm:apc:{cid}")]]))


@action("apq")
async def a_apq(update, context, args):
    await show_queue(update, int(args[0]))


@action("apresh")
async def a_apresh(update, context, args):
    cid = int(args[0])
    await db.ap_rebuild_cycle(cid)
    await show_queue(update, cid, "🔀 Queue reshuffled.\n\n")


@action("apmode")
async def a_apmode(update, context, args):
    cid = int(args[0])
    ch = await db.ap_get_channel(cid)
    if ch:
        await db.ap_set(cid, loop=not ch.get("loop", True))
    ch = await db.ap_get_channel(cid) or {}
    await show_channel(update, cid,
                       ("🔁 Loop: when every post is posted, the queue is reshuffled and starts again.\n\n"
                        if ch.get("loop", True) else
                        "➡️ One pass: the Autoposter turns OFF after every post is posted once.\n\n"))


@action("aprm")
async def a_aprm(update, context, args):
    cid = int(args[0])
    await show(update,
               "🗑 <b>Remove this channel from Autoposter?</b>\n\nIts post list is deleted from the Autoposter. "
               "Share links and the files in the Database Channel stay.",
               M([[B("✅ Yes, remove", f"adm:aprmyes:{cid}")], [B("❌ No", f"adm:apc:{cid}")]]))


@action("aprmyes")
async def a_aprmyes(update, context, args):
    await db.ap_remove_channel(int(args[0]))
    await show(update, "🗑 Channel removed.", K.back("ap"))


# ===================================================================== caption & button
async def show_cb(update, cid: int, note: str = ""):
    ch = await db.ap_get_channel(cid)
    if not ch:
        await show(update, "⚠️ Channel not found.", K.back("ap"))
        return
    tpl = (ch.get("cap_tpl") or "").replace("{caption}", "").strip()
    btn = ch.get("btn") or AP.DEFAULT_BTN
    bi = ch.get("btn_img") or AP.DEFAULT_BTN_IMG
    bv = ch.get("btn_vid") or AP.DEFAULT_BTN_VID
    await show(update,
               f"{note}🎨 <b>Caption &amp; Button</b> — {html.escape(ch['title'])}\n\n"
               f"📝 Fixed caption:\n{tpl or '— not set (each post uses its own caption) —'}\n\n"
               f"🔘 Button (classic posts): <b>{html.escape(btn)}</b>\n"
               f"🖼 Image button (Image + Video): <b>{html.escape(bi)}</b>\n"
               f"🎬 Video button (Image + Video): <b>{html.escape(bv)}</b>\n\n"
               "This one fixed caption is used on every post of this channel (also the ones already created).",
               M([[B("📝 Edit Caption", f"adm:apcap:{cid}"), B("🔘 Edit Button", f"adm:apbtn:{cid}")],
                  [B("🖼 Image Button", f"adm:apbtnx:{cid}:i"), B("🎬 Video Button", f"adm:apbtnx:{cid}:v")],
                  [B("♻️ Reset to default", f"adm:apcbr:{cid}")],
                  [B("🔙 Back", f"adm:apc:{cid}")]]))


@action("apcb")
async def a_apcb(update, context, args):
    await show_cb(update, int(args[0]))


@action("apcbr")
async def a_apcbr(update, context, args):
    cid = int(args[0])
    await db.ap_set(cid, cap_tpl="", btn="", btn_img="", btn_vid="")
    await show_cb(update, cid, "♻️ Reset.\n\n")


@action("apcap")
async def a_apcap(update, context, args):
    context.user_data["ap_cid"] = int(args[0])
    await ask(update, context, "ap_cap",
              "📝 <b>Fixed post caption</b>\n\nSend the full caption once (bold / italic / links work).\n"
              "Every post of this channel will get exactly this text - nothing else is added.\n"
              "Send <code>-</code> to remove it (then each post uses its own caption).")


@state("ap_cap")
async def st_ap_cap(update, context):
    msg = update.effective_message
    cid = context.user_data.get("ap_cid")
    if not msg.text or cid is None:
        await msg.reply_text("⚠️ Send the caption as text.", reply_markup=K.cancel())
        return
    tpl = "" if msg.text.strip() == "-" else msg.text_html.strip()
    if AP._plain_len(tpl) > AP.CAPTION_MAX:
        await msg.reply_text("⚠️ Too long (Telegram caption limit is 1024).", reply_markup=K.cancel())
        return
    await db.ap_set(cid, cap_tpl=tpl)
    context.user_data.pop("state", None)
    await msg.reply_text("✅ Caption saved.", reply_markup=K.back(f"apcb:{cid}"))


@action("apbtn")
async def a_apbtn(update, context, args):
    context.user_data["ap_cid"] = int(args[0])
    await ask(update, context, "ap_btn",
              f"🔘 <b>Button text</b>\n\nSend the text for the button under every post "
              f"(max {AP.BTN_MAX} characters, emoji ok), e.g. <code>⬇️ Download Now</code>\n"
              f"Send <code>-</code> for the default ({AP.DEFAULT_BTN}).")


@state("ap_btn")
async def st_ap_btn(update, context):
    msg = update.effective_message
    cid = context.user_data.get("ap_cid")
    txt = (msg.text or "").strip()
    if not txt or cid is None or len(txt) > AP.BTN_MAX:
        await msg.reply_text(f"⚠️ Send text up to {AP.BTN_MAX} characters.", reply_markup=K.cancel())
        return
    await db.ap_set(cid, btn="" if txt == "-" else txt)
    context.user_data.pop("state", None)
    await msg.reply_text("✅ Button saved.", reply_markup=K.back(f"apcb:{cid}"))


@action("apbtnx")
async def a_apbtnx(update, context, args):
    """Button label of the Image + Video type: i = image (free preview) button, v = video button."""
    context.user_data["ap_cid"] = int(args[0])
    context.user_data["ap_btn_which"] = "btn_img" if args[1] == "i" else "btn_vid"
    default = AP.DEFAULT_BTN_IMG if args[1] == "i" else AP.DEFAULT_BTN_VID
    await ask(update, context, "ap_btnx",
              f"🔘 <b>{'Image' if args[1] == 'i' else 'Video'} button text</b>\n\nTwo buttons sit side by side, so keep it short "
              f"(max {AP.BTN_MAX_IV} characters, emoji ok).\nSend <code>-</code> for the default ({default}).")


@state("ap_btnx")
async def st_ap_btnx(update, context):
    msg = update.effective_message
    cid, field = context.user_data.get("ap_cid"), context.user_data.get("ap_btn_which")
    txt = (msg.text or "").strip()
    if not txt or cid is None or field not in ("btn_img", "btn_vid") or len(txt) > AP.BTN_MAX_IV:
        await msg.reply_text(f"⚠️ Send text up to {AP.BTN_MAX_IV} characters.", reply_markup=K.cancel())
        return
    await db.ap_set(cid, **{field: "" if txt == "-" else txt})
    context.user_data.pop("state", None)
    await msg.reply_text("✅ Button saved.", reply_markup=K.back(f"apcb:{cid}"))


# ===================================================================== timings
async def show_timings(update, cid: int, note: str = ""):
    ch = await db.ap_get_channel(cid)
    if not ch:
        await show(update, "⚠️ Channel not found.", K.back("ap"))
        return
    slots = sorted(ch.get("slots", []), key=lambda s: s["t"])
    rows = []
    for s in slots:
        k = s["t"].replace(":", "")       # ":" is the callback separator
        rows.append([B(f"🕘 {s['t']}", "adm:noop"), B("➖", f"adm:apsn:{cid}:{k}:m"),
                     B(str(s["n"]), "adm:noop"), B("➕", f"adm:apsn:{cid}:{k}:p"),
                     B("❌", f"adm:apsd:{cid}:{k}")])
    rows.append([B("➕ Add Time", f"adm:apsadd:{cid}")])
    rows.append([B("🔙 Back", f"adm:apc:{cid}")])
    await show(update,
               f"{note}⏰ <b>Timings</b> — {html.escape(ch['title'])}\n\n"
               "Time is IST. Each row = a time and how many posts go out at that time.\n"
               "➖ ➕ change the count, ❌ removes the time.",
               M(rows))


@action("aptm")
async def a_aptm(update, context, args):
    await show_timings(update, int(args[0]))


def _hhmm(k: str) -> str:
    return f"{k[:2]}:{k[2:]}"


@action("apsn")
async def a_apsn(update, context, args):
    cid, t, d = int(args[0]), _hhmm(args[1]), args[2]
    ch = await db.ap_get_channel(cid)
    slots = list((ch or {}).get("slots", []))
    for s in slots:
        if s["t"] == t:
            s["n"] = max(1, min(AP.MAX_PER_SLOT, s["n"] + (1 if d == "p" else -1)))
    await db.ap_set(cid, slots=slots)
    await show_timings(update, cid)


@action("apsd")
async def a_apsd(update, context, args):
    cid, t = int(args[0]), _hhmm(args[1])
    ch = await db.ap_get_channel(cid)
    await db.ap_set(cid, slots=[s for s in (ch or {}).get("slots", []) if s["t"] != t])
    await show_timings(update, cid, "✅ Time removed.\n\n")


@action("apsadd")
async def a_apsadd(update, context, args):
    context.user_data["ap_cid"] = int(args[0])
    await ask(update, context, "ap_slot",
              "➕ <b>Add Time</b> (IST, 24-hour)\n\n"
              "Send one or more lines: <code>HH:MM  posts</code>\n"
              "<code>09:00 2</code>\n<code>14:30 1</code>\n<code>21:00 3</code>\n\n"
              f"Posts is optional (default 1, max {AP.MAX_PER_SLOT}). Sending an existing time updates its count.")


@state("ap_slot")
async def st_ap_slot(update, context):
    msg = update.effective_message
    cid = context.user_data.get("ap_cid")
    try:
        new = AP.parse_slots(msg.text or "")
    except ValueError as e:
        await msg.reply_text(f"⚠️ {html.escape(str(e))}\nExample: <code>09:00 2</code>", reply_markup=K.cancel())
        return
    ch = await db.ap_get_channel(cid)
    if not ch:
        context.user_data.pop("state", None)
        return
    merged = {s["t"]: s["n"] for s in ch.get("slots", [])}
    merged.update({s["t"]: s["n"] for s in new})
    await db.ap_set(cid, slots=[{"t": t, "n": n} for t, n in sorted(merged.items())])
    context.user_data.pop("state", None)
    await msg.reply_text(f"✅ Saved. Timings: {AP.fmt_slots([{'t': t, 'n': n} for t, n in merged.items()])}",
                         reply_markup=K.back(f"aptm:{cid}"))


# ===================================================================== all posts
@action("apall")
async def a_apall(update, context, args):
    cid, page = int(args[0]), (int(args[1]) if len(args) > 1 else 0)
    total, posted = await db.ap_counts(cid)
    posts = await db.ap_list_posts(cid, page * PAGE, PAGE)
    rows = [[B(f"{'✅' if p['posted'] else '⏳'} #{p['seq']}{' 🖼🎬' if p.get('ptype') == 'iv' else ''} {p['title'][:22]} · {p['n_files']}",
               f"adm:app:{p['_id']}")] for p in posts]
    nav = []
    if page > 0:
        nav.append(B("⬅️ Prev", f"adm:apall:{cid}:{page - 1}"))
    if (page + 1) * PAGE < total:
        nav.append(B("Next ➡️", f"adm:apall:{cid}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([B("🔙 Back", f"adm:apc:{cid}")])
    await show(update,
               f"📚 <b>All Posts</b> ({total})\n✅ posted {posted} · ⏳ pending {total - posted}\n\n"
               + ("Tap a post to open it." if total else "No posts yet. Use ➕ Create Post."),
               M(rows))


async def show_post(update, bot, token: str, note: str = ""):
    p = await db.ap_get_post(token)
    if not p:
        await show(update, "⚠️ Post not found.", K.back("ap"))
        return
    if p.get("ptype") == "iv":
        links = (f"🖼 Free image link:\n<code>{share_link(bot.username, p['img_token'])}</code>\n"
                 f"🎬 Video link (ad gate):\n<code>{share_link(bot.username, token)}</code>")
    else:
        links = f"🔗 <code>{share_link(bot.username, token)}</code>"
    await show(update,
               f"{note}📝 <b>#{p['seq']}</b> · {'✅ posted' if p['posted'] else '⏳ pending'}"
               f"{' · 🖼🎬 Image + Video' if p.get('ptype') == 'iv' else ''}\n"
               f"📁 Files: {p['n_files']}\n\n{html.escape(p['title'])}\n\n{links}",
               M([[B("📤 Post now", f"adm:appnow:{token}")],
                  [B("🗑 Delete post", f"adm:apdel:{token}:0"), B("🗑 Delete post + link", f"adm:apdel:{token}:1")],
                  [B("🔙 Back", f"adm:apall:{p['chan']}:0")]]))


@action("app")
async def a_app(update, context, args):
    await show_post(update, context.bot, args[0])


@action("appnow")
async def a_appnow(update, context, args):
    p = await db.ap_get_post(args[0])
    if not p:
        await show(update, "⚠️ Post not found.", K.back("ap"))
        return
    err = await AP.send_post(context.bot, p["chan"], p)
    await show_post(update, context.bot, args[0], "✅ Posted.\n\n" if not err else f"⚠️ <code>{html.escape(err)}</code>\n\n")


@action("apdel")
async def a_apdel(update, context, args):
    p = await db.ap_get_post(args[0])
    with_link = len(args) > 1 and args[1] == "1"
    await db.ap_delete_post(args[0])
    if with_link:
        await db.delete_link(args[0])
        if p and p.get("img_token"):                       # Image + Video: the free image link too
            await db.delete_link(p["img_token"])
    await show(update,
               "🗑 Post and its share link deleted." if with_link
               else "🗑 Post deleted from the Autoposter (its share link still works).",
               K.back(f"apall:{p['chan']}:0" if p else "ap"))


@action("apwipe")
async def a_apwipe(update, context, args):
    cid = int(args[0])
    total, posted = await db.ap_counts(cid)
    await show(update,
               f"🧹 <b>Wipe posts</b>\n\n📚 {total} post(s) · ✅ posted {posted} · ⏳ pending {total - posted}\n\n"
               "<b>Posts only</b>: posts leave the Autoposter, old share links keep working.\n"
               "<b>Posts + links</b>: the share links are deleted too (files stay in the Database Channel).",
               M([[B("🧹 Wipe ALL posts", f"adm:apwipey:{cid}:a0")],
                  [B("🧹 Wipe ALL posts + links", f"adm:apwipey:{cid}:a1")],
                  [B("✅ Wipe only posted ones", f"adm:apwipey:{cid}:p0")],
                  [B("🔙 Back", f"adm:apc:{cid}")]]))


@action("apwipey")
async def a_apwipey(update, context, args):
    cid, mode = int(args[0]), args[1]
    n = await db.ap_wipe(cid, only_posted=mode[0] == "p", links=mode[1] == "1")
    await show_channel(update, cid, f"🧹 {n} post(s) deleted.\n\n")


# ===================================================================== create post
@action("apnew")
async def a_apnew(update, context, args):
    cid = int(args[0])
    if not S.get("db_channel"):
        await show(update, "⚠️ Set the Database Channel first.", M([[B("📡 Set Database Channel", "adm:dbch")]]))
        return
    ch = await db.ap_get_channel(cid) or {}
    if ch.get("ptype") == "iv":
        how = ("🖼🎬 <b>Image + Video</b> mode\n\nSend / forward in this order: <b>image 1, video 1, image 2, video 2 …</b>\n"
               "Every (image + video) pair = ONE post with two links: the image link is a free preview (no ad), "
               "the video link shows the ad first. Captions are kept (with their backup codes).\n"
               "A photo without a video after it, or a video without a photo before it, is skipped.\n\nChoose how:")
    else:
        how = ("A file <b>with a caption</b> = the post's thumbnail. All files <b>without a caption</b> right below it "
               "are that post's files (one share link). The next captioned file starts the next post.\n"
               "📷 A captionless <b>album</b> below a captioned file goes into that post's link.\n\nChoose how:")
    await show(update,
               "➕ <b>Create Post</b>\n\n" + how,
               M([[B("📥 Send / forward files, then Done", f"adm:apcollect:{cid}")],
                  [B("🔙 Back", f"adm:apc:{cid}")]]))


# ---- (b) collect mode: the admin sends / forwards the files to the bot
def _collect_text(n: int) -> str:
    return (f"📥 <b>Collecting files</b>\n\n{n} file(s) received.\n"
            "Send / forward them <b>in order</b> (caption file first, its captionless files below), then tap <b>Done</b>.")


def _collect_kb():
    return M([[B("✅ Done", "adm:apdone")], [B("❌ Cancel", "adm:cancel")]])


@action("apcollect")
async def a_apcollect(update, context, args):
    q = update.callback_query
    context.user_data.update(state="ap_collect", ap_cid=int(args[0]), ap_items=[],
                             ap_status=(q.message.chat_id, q.message.message_id), ap_ts=0.0)
    await show(update, _collect_text(0), _collect_kb())


@state("ap_collect")
async def st_ap_collect(update, context):
    msg = update.effective_message
    kind, name = media_info(msg)
    if not kind:
        await msg.reply_text("📎 Send files only, or tap <b>Done</b>.", reply_markup=_collect_kb())
        return
    items = context.user_data.setdefault("ap_items", [])
    items.append(AP.item_from_ptb(msg, kind, name))
    if time.time() - context.user_data.get("ap_ts", 0) > 2.5 and context.user_data.get("ap_status"):
        context.user_data["ap_ts"] = time.time()
        chat_id, mid = context.user_data["ap_status"]
        try:
            await context.bot.edit_message_text(_collect_text(len(items)), chat_id=chat_id, message_id=mid,
                                                reply_markup=_collect_kb())
        except Exception:
            pass


@action("apdone")
async def a_apdone(update, context, args):
    ud = context.user_data
    items, cid = ud.get("ap_items") or [], ud.get("ap_cid")
    if not items or cid is None:
        if cid is not None:
            ud["state"] = "ap_collect"       # the router cleared it; keep collecting
        await show(update, "⚠️ Send at least one file first.\n\n" + _collect_text(0), _collect_kb())
        return
    for k in ("state", "ap_items", "ap_cid", "ap_status", "ap_ts"):
        ud.pop(k, None)
    status = await update.effective_chat.send_message(f"⏳ Starting… {len(items)} file(s)")
    AP.spawn(_job(AP.create_posts(context.bot, items, cid, update.effective_user.id,
                                  status.chat_id, status.message_id),
                  context.bot, status.chat_id))
