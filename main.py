"""
main.py - entry point.
Starts: MongoDB + settings cache, health endpoint (/health), auto-delete worker, Telegram polling.
"""
import asyncio
import logging
import signal

try:                                   # uvloop = much faster event loop (Linux/VPS). Optional.
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except Exception:                      # not installed / not supported -> normal asyncio
    pass

from telegram.constants import ParseMode
from telegram.error import Conflict, NetworkError
from telegram.ext import (ApplicationBuilder, CallbackQueryHandler, ChatMemberHandler, CommandHandler,
                          ContextTypes, Defaults, MessageHandler, filters)

import config
from database import db, mongo
from handlers import admin, start, upload
from handlers import autoposter as _autoposter  # noqa: F401  (registers the Autoposter panel screens)
from handlers import backup as _backup          # registers the Backup / Restore screens
from handlers import analytics as _analytics    # registers the Earnings / Analytics screens
from handlers import usage as _usage            # registers the VPS Usage / Plan advice screens
from handlers import servers as _servers        # registers the Servers screens (multi-server mode)
from utils.backup import backup_loop
from utils import padding
from utils import chats as _chats
from utils import cluster
from utils.web import start_web_server
from utils.scheduler import autodelete_loop
from utils.autoposter import autopost_loop
from utils.usage import usage_loop

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s", level=config.LOG_LEVEL)
logging.getLogger("httpx").setLevel(logging.WARNING)  # don't log every Bot API call (leaks the token)
log = logging.getLogger("bot")


def start_gateway_jobs(app):
    """Background jobs that must run on ONE server only: auto-delete, autoposter, daily backup, link registration."""
    app.bot_data["gc"] = asyncio.create_task(autodelete_loop(app.bot))
    app.bot_data["ap"] = asyncio.create_task(autopost_loop(app.bot))     # Autoposter scheduler
    app.bot_data["bk"] = asyncio.create_task(backup_loop(app.bot))      # daily auto-backup to the owner


def stop_gateway_jobs(app):
    for key in ("gc", "ap", "bk"):
        task = app.bot_data.pop(key, None)
        if task:
            task.cancel()


async def post_init(app):
    if app.bot_data.get("_started"):                     # never run twice
        return
    app.bot_data["_started"] = True
    await db.init_db()                                   # connect, indexes on every cluster, load cached settings
    app.bot_data["stf"] = asyncio.create_task(db.stats_flush_loop())    # buffered counters (views / daily stats) -> MongoDB every few seconds
    if len(mongo.clusters) > 1:
        app.bot_data["mm"] = asyncio.create_task(mongo.monitor())       # pings the extra MongoDB clusters, switches to core when one is down
    cluster.attach(app)
    app.bot_data["health"] = await start_web_server(config.PORT, app.bot)   # health and permanent links
    # Multi-server mode: ONLY the active gateway runs the background jobs (otherwise every server would delete / post twice).
    # The active gateway is decided by a lease in MongoDB (utils/cluster.py) and can change: the jobs follow it.
    if not cluster.ENABLED:
        start_gateway_jobs(app)
    else:
        cluster.set_hooks(lambda: start_gateway_jobs(app), lambda: stop_gateway_jobs(app))
    app.bot_data["us"] = asyncio.create_task(usage_loop())              # VPS usage sampler (CPU / RAM / bandwidth)
    if cluster.ENABLED:
        app.bot_data["cl"] = await cluster.start(app)    # heartbeat (+ router, stats, Telegram webhook on the gateway)
    log.info("Bot @%s started (web on :%s, forwarding only%s)", app.bot.username, config.PORT,
             f", node {config.NODE_ID} {'gateway-capable' if cluster.CAN_GATEWAY else 'worker'}" if cluster.ENABLED else "")
    if not config.BASE_URL.startswith("https://"):
        log.warning("BASE_URL is not a public https URL -> Monetag ads (Mini App) and permanent links will not work.")
    if cluster.ENABLED and config.NODE_ROLE not in ("gateway", "worker"):
        log.warning("NODE_ROLE must be 'gateway' or 'worker' (got %r) - running as a worker.", config.NODE_ROLE)


async def post_shutdown(app):
    if cluster.ENABLED:
        # FIRST: stop taking new updates, hand the gateway lease over, let the running updates finish (DRAIN_SECONDS).
        await cluster.shutdown()
    stop_gateway_jobs(app)
    for key in ("us", "mm"):
        task = app.bot_data.get(key)
        if task:
            task.cancel()
    task = app.bot_data.get("stf")
    if task:
        task.cancel()                                  # its CancelledError handler writes the last buffered counters
        await asyncio.gather(task, return_exceptions=True)
    for task in app.bot_data.get("cl") or []:
        task.cancel()
    runner = app.bot_data.get("health")
    if runner:
        await runner.cleanup()


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, Conflict):
        # Another instance is polling with the same token (old deploy still shutting down,
        # a local run, or another server). Polling keeps retrying and recovers once it stops.
        log.warning("Conflict: another instance is using this BOT_TOKEN - stop it (only one may run)")
        return
    if isinstance(context.error, NetworkError):
        log.warning("Network error: %s", context.error)  # transient, polling retries by itself
        return
    log.error("Unhandled error", exc_info=context.error)


def build_app():
    padding.install()           # every message with inline buttons gets as wide as its keyboard
    app = (ApplicationBuilder()
           .token(config.BOT_TOKEN)
           .defaults(Defaults(parse_mode=ParseMode.HTML))
           .concurrent_updates(config.NODE_MAX_TASKS)    # many users can receive files at the same time
           # Bot API connection pool: must be big, otherwise concurrent users queue up behind each other
           .connection_pool_size(max(16, config.NODE_MAX_TASKS * 2))
           .pool_timeout(15)
           .connect_timeout(10)
           .read_timeout(30)
           .write_timeout(30)
           .get_updates_connection_pool_size(4)
           .get_updates_read_timeout(40)
           .get_updates_connect_timeout(10)
           .get_updates_pool_timeout(15)
           .post_init(post_init)
           .post_shutdown(post_shutdown)
           .build())

    private = filters.ChatType.PRIVATE
    app.add_handler(CommandHandler("start", start.start_cmd, filters=private))        # the ONLY command
    app.add_handler(CallbackQueryHandler(admin.callback_router, pattern=r"^adm:"))
    app.add_handler(CallbackQueryHandler(start.check_callback, pattern=r"^chk:"))
    # a backup file (fsb-backup-*.json.gz) sent by the owner -> restore flow (must come BEFORE the generic router)
    app.add_handler(MessageHandler(private & filters.Document.ALL & _backup.BackupDocFilter(), _backup.ingest))
    app.add_handler(MessageHandler(private & ~filters.COMMAND, admin.message_router))
    # Files forwarded into the Database Channel are re-linked from their caption code (disaster recovery)
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST & ~filters.COMMAND, upload.channel_restore))
    # remember where the bot is admin -> shown as inline buttons in the panel (utils/chats.py)
    app.add_handler(ChatMemberHandler(_chats.on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, _chats.on_channel_post), group=1)
    app.add_error_handler(on_error)
    return app


async def run_cluster(app):
    """Multi-server mode: no polling. Telegram calls the gateway's webhook, the gateway routes (utils/cluster.py)."""
    await app.initialize()
    await post_init(app)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    await stop.wait()
    log.info("Shutting down...")
    await post_shutdown(app)
    await app.shutdown()


def main():
    app = build_app()
    if cluster.ENABLED:
        asyncio.run(run_cluster(app))
        return
    asyncio.set_event_loop(asyncio.new_event_loop())   # Python 3.12 + uvloop: run_polling needs a current loop
    # only the update types this bot really uses (less traffic, less CPU)
    app.run_polling(allowed_updates=cluster.ALLOWED_UPDATES, drop_pending_updates=True, timeout=30)


if __name__ == "__main__":
    main()
