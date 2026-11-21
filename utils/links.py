"""
utils/links.py - permanent share links.

With WORKER_URL set, every link is  <WORKER_URL>/<token>  (a Cloudflare Worker that instantly redirects to the
CURRENT bot). The Worker proxies permanent links to an active origin, which redirects using its current bot username.
Without WORKER_URL but with BASE_URL (https), links are  <BASE_URL>/<token>  and the bot's own web server
redirects them to the current bot (no Cloudflare needed; same permanence after a bot change).
Otherwise the plain t.me link is used.
"""
import logging


import config

log = logging.getLogger(__name__)


def share_link(bot_username: str, token: str) -> str:
    if config.WORKER_URL:
        return f"{config.WORKER_URL}/{token}"
    if config.PUBLIC_URL.startswith("https://"):  # one domain for all servers (Cloudflare Worker in front)
        return f"{config.PUBLIC_URL}/{token}"
    if config.BASE_URL.startswith("https://"):   # own domain: https://yourdomain/<token> -> redirects to the CURRENT bot
        return f"{config.BASE_URL}/{token}"
    return f"https://t.me/{bot_username}?start={token}"


