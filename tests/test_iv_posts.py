"""Autoposter post type "Image + Video": pairing, storing, publishing, free image link.
No MongoDB / Telegram needed (third-party modules are stubbed).   Run:  python3 tests/test_iv_posts.py"""
import asyncio, datetime as dt, os, sys, types
from unittest.mock import MagicMock, AsyncMock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

os.environ.update(BOT_TOKEN="123:abc", MONGO_URI="mongodb://x", OWNER_ID="1", API_ID="1", API_HASH="h",
                  BASE_URL="https://n1.example.com")
sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *a, **k: None)


class TelegramError(Exception): pass


tg = MagicMock(); tge = types.ModuleType("telegram.error")
for n in ("TelegramError", "BadRequest", "Forbidden", "RetryAfter"):
    setattr(tge, n, type(n, (TelegramError,), {}) if n != "TelegramError" else TelegramError)
sys.modules.update({"telegram": tg, "telegram.constants": MagicMock(), "telegram.error": tge, "telegram.ext": MagicMock(),
                    "pymongo": MagicMock(), "pymongo.errors": MagicMock(), "aiohttp": MagicMock()})

database = types.ModuleType("database"); dbmod = MagicMock(); dbmod.LIMIT_TZ = dt.timezone.utc
database.db = dbmod; sys.modules.update({"database": database, "database.db": dbmod})
# the pure helpers of the real db module are needed for backup codes
dbmod.make_code = lambda token, idx: f"FSB:{token}:{idx}"
_tok = iter(f"tok{i:02d}" for i in range(1000))
dbmod.new_token = lambda: next(_tok)

import config
from utils import autoposter as AP

ok = fail = 0


def check(name, cond):
    global ok, fail
    if cond: ok += 1; print("  ok  ", name)
    else: fail += 1; print("  FAIL", name)


def item(mid, kind, cap="", name=None):
    return {"chat": 1, "mid": mid, "kind": kind, "name": name or kind, "cap": cap, "cap_html": cap, "gid": None}


# ================= pairing
print("group_iv")
P, V, D = "photo", "video", "document"
pairs, skipped = AP.group_iv([item(1, P), item(2, V), item(3, P), item(4, V)])
check("image 1, video 1, image 2, video 2 = two pairs", len(pairs) == 2 and skipped == 0)
check("pair order is kept", [(x["img"]["mid"], x["vid"]["mid"]) for x in pairs] == [(1, 2), (3, 4)])
pairs, skipped = AP.group_iv([item(1, V), item(2, P), item(3, P), item(4, V), item(5, V), item(6, P)])
check("video first, image without video, second video, trailing image are skipped",
      [(x["img"]["mid"], x["vid"]["mid"]) for x in pairs] == [(3, 4)] and skipped == 4)
pairs, skipped = AP.group_iv([item(1, P), item(2, D), item(3, P), item(4, "animation"), item(5, "audio")])
check("a video sent as a file / gif completes a pair, other types are skipped", len(pairs) == 2 and skipped == 1)
check("nothing in -> nothing out", AP.group_iv([]) == ([], 0))


# ================= create_iv_posts
async def make_posts():
    dbmod.reset_mock()
    config_ch = {"id": -100500, "title": "DB"}
    AP.S.get = MagicMock(side_effect=lambda k: config_ch if k == "db_channel" else None)
    dbmod.ap_get_channel = AsyncMock(return_value={"_id": 7, "ptype": "iv"})
    dbmod.ap_last_seq = AsyncMock(return_value=4)
    dbmod.create_link = AsyncMock()
    dbmod.ap_add_post = AsyncMock()
    dbmod.ap_enqueue_new = AsyncMock()
    dbmod.add_file = AsyncMock()
    bot = MagicMock(); bot.edit_message_text = AsyncMock()
    bot.copy_message = AsyncMock(side_effect=[types.SimpleNamespace(message_id=m) for m in (901, 902, 903, 904)])
    AP.COPY_DELAY = 0
    items = [item(10, P, "<b>Cap one</b>"), item(11, V, ""), item(12, P, ""), item(13, V, "Video two cap")]
    await AP.create_posts(bot, items, 7, 99, 1, 2)          # classic entry point must hand over to the iv engine
    return bot


print("create_iv_posts")
bot = asyncio.run(make_posts())
check("4 files copied into the Database Channel", bot.copy_message.await_count == 4)
caps = [c.kwargs.get("caption") for c in bot.copy_message.await_args_list]
check("every file carries its backup code (captions are kept)",
      caps[0].startswith("<b>Cap one</b>") and "FSB:tok00:0" in caps[0] and caps[1] == "<code>FSB:tok01:0</code>"
      and "Video two cap" in caps[3] and "FSB:tok03:0" in caps[3])
links = dbmod.create_link.await_args_list
check("image link is FREE, video link is a normal link",
      links[0].kwargs.get("free") is True and links[0].args[0] == [901] and links[0].kwargs["token"] == "tok00"
      and not links[1].kwargs.get("free") and links[1].args[0] == [902] and links[1].kwargs["token"] == "tok01")
post = dbmod.ap_add_post.await_args_list[0]
check("ONE post per pair: id = video token, img_token = image token, type iv, seq continues",
      dbmod.ap_add_post.await_count == 2 and post.args[1] == "tok01" and post.args[3] == 5
      and post.kwargs == {"ptype": "iv", "img_token": "tok00"} and post.args[7] == 2)
check("post caption = the image caption, else the video caption",
      post.args[5] == "<b>Cap one</b>" and dbmod.ap_add_post.await_args_list[1].args[5] == "Video two cap")
check("new posts are mixed into the running queue", dbmod.ap_enqueue_new.await_args.args == (7, ["tok01", "tok03"]))


# ================= publishing
async def publish(post, ch, thumbs):
    dbmod.reset_mock()
    AP.S.get = MagicMock(side_effect=lambda k: {"id": -100500, "title": "DB"} if k == "db_channel" else None)
    dbmod.ap_get_channel = AsyncMock(return_value=ch)
    dbmod.thumb_msg_id = AsyncMock(side_effect=lambda code: thumbs.get(code))
    dbmod.ap_mark_posted = AsyncMock()
    tg.InlineKeyboardButton.reset_mock()
    bot = MagicMock(); bot.username = "mybot"; bot.copy_message = AsyncMock()
    err = await AP.send_post(bot, 7, post)
    return err, bot


print("send_post")
ivpost = {"_id": "vtok", "img_token": "itok", "ptype": "iv", "cap": "Post cap", "thumb_token": ""}
err, bot = asyncio.run(publish(ivpost, {"_id": 7, "iv_thumb": "fixed", "btn_img": "", "btn_vid": "Play"},
                               {"FSB:fixed:0": 555, "FSB:itok:0": 666}))
kw = bot.copy_message.await_args
check("fixed thumbnail is used (not the post's own image)", err is None and kw.args == (7, -100500, 555))
btns = [c for c in tg.InlineKeyboardButton.call_args_list]
check("two buttons: free image link | video link, custom label + default label",
      [(c.args[0], c.kwargs["url"].rsplit("/", 1)[1]) for c in btns] == [(AP.DEFAULT_BTN_IMG, "itok"), ("Play", "vtok")])
check("post is marked posted", dbmod.ap_mark_posted.await_args.args == ("vtok",))

err, bot = asyncio.run(publish(ivpost, {"_id": 7}, {"FSB:itok:0": 666}))
check("no fixed thumbnail -> the post's own image is the thumbnail", err is None and bot.copy_message.await_args.args[2] == 666)
err, bot = asyncio.run(publish(ivpost, {"_id": 7, "iv_thumb": "gone"}, {"FSB:itok:0": 666}))
check("fixed thumbnail missing from the DB channel -> falls back to the post's image", err is None)
err, bot = asyncio.run(publish(ivpost, {"_id": 7}, {}))
check("nothing found -> 'thumbnail not found' (the queue skips such a post)", err and err.startswith("thumbnail not found"))

classic = {"_id": "ctok", "thumb_token": "cth", "cap": "c"}
err, bot = asyncio.run(publish(classic, {"_id": 7, "btn": "Get"}, {"FSB:cth:0": 777}))
check("classic post is untouched: thumbnail by its own token, one button",
      err is None and bot.copy_message.await_args.args == (7, -100500, 777)
      and len(tg.InlineKeyboardButton.call_args_list) == 1)

# ================= free link: no cooldown, no daily limit, no ad gate
print("free link (handlers/start.py)")
sys.modules["handlers.admin"] = MagicMock()
for n in ("ads", "cluster", "membership", "streamer"):
    sys.modules[f"utils.{n}"] = MagicMock()
sys.modules["utils.streamer"].human_size = str
import utils
for n in ("ads", "cluster", "membership", "streamer"):
    setattr(utils, n, sys.modules[f"utils.{n}"])
from handlers import start as ST


async def open_link(link):
    ST.is_admin = lambda uid: False
    ST._last_open.clear()
    ST.S.get = MagicMock(side_effect=lambda k: {"ads_enabled": True}.get(k))
    ST.cluster.ENABLED = False
    ST.cluster.connect_animation = AsyncMock()
    ST.ads.gate = AsyncMock(return_value=False)
    ST.db.used_today = AsyncMock(return_value=0)
    ST.db.limit_value = MagicMock(return_value=10)
    ST.db.claim_quota = AsyncMock(return_value=(1, 1))
    ST._deliver = AsyncMock(return_value=1)
    ST.call = AsyncMock()
    bot = MagicMock()
    await ST.request_access(bot, 5, link)
    first = (ST.ads.gate.await_count, ST.db.claim_quota.await_count, ST._deliver.await_count, 5 in ST._last_open)
    await ST.request_access(bot, 5, link)             # immediately again (cooldown applies only to normal links)
    return first, ST._deliver.await_count


first, again = asyncio.run(open_link({"_id": "itok", "ids": [901], "free": True}))
check("free link: no ad gate, no daily count, no cooldown, files delivered both times", first == (0, 0, 1, False) and again == 2)
first, again = asyncio.run(open_link({"_id": "vtok", "ids": [902]}))
check("normal link: ad gate asked, daily count claimed, cooldown set and enforced", first == (1, 1, 1, True) and again == 1)

print(f"\n{'PASS' if not fail else 'FAIL'}: {ok} ok, {fail} failed")
sys.exit(1 if fail else 0)
