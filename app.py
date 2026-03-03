import asyncio
import os
import secrets
import time
from datetime import datetime
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from flask import Flask, abort, jsonify, redirect, render_template, request, session, url_for

import database as db
import scanner
import state

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "dev-secret-change-me")
app.config.update(
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=False,
    PERMANENT_SESSION_LIFETIME=600,
)

db.init_db()

DISCORD_CLIENT_ID     = os.getenv("DISCORD_CLIENT_ID")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
DISCORD_REDIRECT_URI  = os.getenv("DISCORD_REDIRECT_URI", "http://localhost:5000/callback")
DISCORD_BOT_TOKEN     = os.getenv("DISCORD_TOKEN")

_API      = "https://discord.com/api/v10"
_AUTH_URL = "https://discord.com/oauth2/authorize"
_TOKEN_URL = f"{_API}/oauth2/token"
_SCOPES   = "identify guilds"

_MANAGE_SERVER = 0x20
_ADMINISTRATOR = 0x8

# In-process guild list cache: {user_id: (guilds_list, fetched_at_timestamp)}
# Avoids hammering /users/@me/guilds on every page load (Discord rate-limits it aggressively).
_guilds_cache: dict[str, tuple[list, float]] = {}
_GUILDS_CACHE_TTL = 120  # seconds

_EVENT_LABELS = {
    "eggs":        "Egg Collection",
    "milk":        "Milk Collection",
    "deposit":     "Deposit",
    "withdrawal":  "Withdrawal",
    "cattle_buy":  "Cattle Buy",
    "cattle_sell": "Cattle Sell",
    "materials":   "Materials",
    "supplies":    "Supplies",
    "stock_sale":  "Stock Sale",
}

_EVENT_COLORS = {
    "eggs":        "#f59e0b",
    "milk":        "#3b82f6",
    "deposit":     "#22c55e",
    "withdrawal":  "#ef4444",
    "cattle_buy":  "#f97316",
    "cattle_sell": "#14b8a6",
    "materials":   "#a855f7",
    "supplies":    "#06b6d4",
    "stock_sale":  "#6366f1",
}


# ---------------------------------------------------------------------------
# Template filters
# ---------------------------------------------------------------------------

@app.template_filter("fmt_num")
def fmt_num(value):
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


@app.template_filter("fmt_money")
def fmt_money(value):
    try:
        return f"${int(value):,}"
    except (TypeError, ValueError):
        return "$0"


@app.template_filter("time_ago")
def time_ago(timestamp_str):
    if not timestamp_str:
        return ""
    try:
        dt = datetime.fromisoformat(timestamp_str)
        delta = datetime.utcnow() - dt
        if delta.days >= 1:
            return f"{delta.days}d ago"
        hours = delta.seconds // 3600
        if hours >= 1:
            return f"{hours}h ago"
        minutes = delta.seconds // 60
        return f"{minutes}m ago"
    except Exception:
        return timestamp_str


@app.template_filter("event_label")
def event_label_filter(etype):
    return _EVENT_LABELS.get(etype, etype)


@app.template_filter("event_color")
def event_color_filter(etype):
    return _EVENT_COLORS.get(etype, "#71717a")


# ---------------------------------------------------------------------------
# Discord helpers
# ---------------------------------------------------------------------------

def _avatar_url(user: dict) -> str:
    """Return the user's Discord avatar URL, or a default if they have none."""
    if user.get("avatar"):
        return f"https://cdn.discordapp.com/avatars/{user['id']}/{user['avatar']}.png"
    default_index = (int(user["id"]) >> 22) % 6
    return f"https://cdn.discordapp.com/embed/avatars/{default_index}.png"


def _guild_icon_url(guild: dict) -> str | None:
    """Return the guild's Discord icon URL, or None if unset."""
    if guild.get("icon"):
        return f"https://cdn.discordapp.com/icons/{guild['id']}/{guild['icon']}.png"
    return None


def _has_manage_server(guild: dict) -> bool:
    """Return True if the guild permissions include Manage Server or Administrator."""
    perms = int(guild.get("permissions", 0))
    return bool((perms & _MANAGE_SERVER) or (perms & _ADMINISTRATOR))


def _get_user_guilds(access_token: str) -> list:
    """Fetch the authenticated user's guild list, with a 2-minute in-process cache."""
    user_id = session.get("user", {}).get("id")
    if user_id:
        cached = _guilds_cache.get(user_id)
        if cached and (time.time() - cached[1]) < _GUILDS_CACHE_TTL:
            return cached[0]

    resp = requests.get(
        f"{_API}/users/@me/guilds",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    resp.raise_for_status()
    guilds = resp.json()

    if user_id:
        _guilds_cache[user_id] = (guilds, time.time())

    return guilds


def _get_guild_channels(guild_id: str) -> list:
    """Fetch text channels for a guild using the bot token, sorted by position."""
    resp = requests.get(
        f"{_API}/guilds/{guild_id}/channels",
        headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
        timeout=10,
    )
    resp.raise_for_status()
    channels = resp.json()
    text_channels = [c for c in channels if c.get("type") in (0, 5)]
    text_channels.sort(key=lambda c: c.get("position", 0))
    return text_channels


def _require_auth():
    """Return a redirect response if the user is not logged in, else None."""
    if "user" not in session:
        return redirect(url_for("login"))
    return None


def _require_guild_access(guild_id: str) -> dict:
    """Verify the current user is a member of the guild. Returns the guild dict."""
    access_token = session.get("access_token")
    user_guilds  = _get_user_guilds(access_token)
    guild = next((g for g in user_guilds if g["id"] == guild_id), None)
    if not guild:
        abort(403)
    guild["icon_url"]   = _guild_icon_url(guild)
    guild["can_manage"] = _has_manage_server(guild)
    return guild


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html", user=session.get("user"))


@app.route("/login")
def login():
    state = secrets.token_urlsafe(16)
    session.permanent = True
    session["oauth_state"] = state
    params = {
        "client_id":     DISCORD_CLIENT_ID,
        "redirect_uri":  DISCORD_REDIRECT_URI,
        "response_type": "code",
        "scope":         _SCOPES,
        "state":         state,
    }
    return redirect(f"{_AUTH_URL}?{urlencode(params)}")


@app.route("/callback")
def callback():
    received_state = request.args.get("state")
    stored_state   = session.pop("oauth_state", None)

    if received_state != stored_state:
        abort(403)

    code = request.args.get("code")
    if not code:
        abort(400)

    token_resp = requests.post(
        _TOKEN_URL,
        data={
            "client_id":     DISCORD_CLIENT_ID,
            "client_secret": DISCORD_CLIENT_SECRET,
            "grant_type":    "authorization_code",
            "code":          code,
            "redirect_uri":  DISCORD_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=10,
    )
    token_resp.raise_for_status()
    access_token = token_resp.json()["access_token"]

    user_resp = requests.get(
        f"{_API}/users/@me",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    user_resp.raise_for_status()
    user = user_resp.json()

    session["user"] = {
        "id":         user["id"],
        "username":   user["username"],
        "avatar_url": _avatar_url(user),
    }
    session["access_token"] = access_token

    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    redir = _require_auth()
    if redir:
        return redir

    access_token   = session.get("access_token")
    user_guilds    = _get_user_guilds(access_token)
    configured_ids = set(db.get_configured_guild_ids())

    for g in user_guilds:
        g["icon_url"] = _guild_icon_url(g)

    bot_guilds = [g for g in user_guilds if g["id"] in configured_ids]
    other_guilds = [
        g for g in user_guilds
        if g["id"] not in configured_ids and _has_manage_server(g)
    ]

    return render_template(
        "dashboard.html",
        user=session["user"],
        bot_guilds=bot_guilds,
        other_guilds=other_guilds,
        client_id=DISCORD_CLIENT_ID,
    )


@app.route("/guild/<guild_id>")
def guild_page(guild_id):
    redir = _require_auth()
    if redir:
        return redir

    guild  = _require_guild_access(guild_id)
    tab    = request.args.get("tab", "analytics")
    period = request.args.get("period", "alltime")

    if tab == "settings":
        config        = db.get_guild_config(guild_id)
        channels      = []
        bot_connected = bool(state.bot_loop and not state.bot_loop.is_closed())
        try:
            channels = _get_guild_channels(guild_id)
        except Exception:
            pass
        return render_template(
            "guild.html",
            user=session["user"],
            guild=guild,
            tab="settings",
            period=period,
            config=config,
            channels=channels,
            saved=request.args.get("saved"),
            scanning=request.args.get("scanning"),
            bot_connected=bot_connected,
        )

    # Analytics tab
    config        = db.get_guild_config(guild_id)
    summary       = db.get_summary_stats(period=period, player=None, guild_id=guild_id)
    top_eggs      = db.get_collection_stats("eggs",      period, None, guild_id)[:5]
    top_milk      = db.get_collection_stats("milk",      period, None, guild_id)[:5]
    top_materials = db.get_collection_stats("materials", period, None, guild_id)[:5]
    top_supplies  = db.get_collection_stats("supplies",  period, None, guild_id)[:5]
    top_deposits  = db.get_ledger_stats(period, None, guild_id)["deposit"][:5]
    recent        = db.get_recent_events(guild_id, limit=20)

    return render_template(
        "guild.html",
        user=session["user"],
        guild=guild,
        tab="analytics",
        period=period,
        config=config,
        summary=summary,
        top_eggs=top_eggs,
        top_milk=top_milk,
        top_deposits=top_deposits,
        top_materials=top_materials,
        top_supplies=top_supplies,
        recent=recent,
    )


@app.route("/guild/<guild_id>/settings", methods=["POST"])
def guild_settings_save(guild_id):
    redir = _require_auth()
    if redir:
        return redir

    guild = _require_guild_access(guild_id)
    if not guild["can_manage"]:
        abort(403)

    old_config = db.get_guild_config(guild_id)
    old_ranch  = old_config["ranch_channel_id"] if old_config else None
    old_camp   = old_config["camp_channel_id"]  if old_config else None

    ranch_channel_id = request.form.get("ranch_channel_id") or None
    camp_channel_id  = request.form.get("camp_channel_id") or None
    db.upsert_guild_config(guild_id, ranch_channel_id, camp_channel_id)

    scanning = False
    if state.bot_loop and not state.bot_loop.is_closed():
        if ranch_channel_id and ranch_channel_id != old_ranch:
            asyncio.run_coroutine_threadsafe(
                scanner.scan_channel(state.bot_instance, int(ranch_channel_id), guild_id, is_camp=False),
                state.bot_loop,
            )
            scanning = True
        if camp_channel_id and camp_channel_id != old_camp:
            asyncio.run_coroutine_threadsafe(
                scanner.scan_channel(state.bot_instance, int(camp_channel_id), guild_id, is_camp=True),
                state.bot_loop,
            )
            scanning = True

    redirect_kwargs = {"guild_id": guild_id, "tab": "settings", "saved": "1"}
    if scanning:
        redirect_kwargs["scanning"] = "1"
    return redirect(url_for("guild_page", **redirect_kwargs))


@app.route("/guild/<guild_id>/scan", methods=["POST"])
def guild_scan(guild_id):
    redir = _require_auth()
    if redir:
        return redir

    guild = _require_guild_access(guild_id)
    if not guild["can_manage"]:
        abort(403)

    config = db.get_guild_config(guild_id)
    if not config:
        return redirect(url_for("guild_page", guild_id=guild_id, tab="settings"))

    scanning = False
    if state.bot_loop and not state.bot_loop.is_closed():
        if config["ranch_channel_id"]:
            asyncio.run_coroutine_threadsafe(
                scanner.scan_channel(state.bot_instance, int(config["ranch_channel_id"]), guild_id, is_camp=False),
                state.bot_loop,
            )
            scanning = True
        if config["camp_channel_id"]:
            asyncio.run_coroutine_threadsafe(
                scanner.scan_channel(state.bot_instance, int(config["camp_channel_id"]), guild_id, is_camp=True),
                state.bot_loop,
            )
            scanning = True

    redirect_kwargs = {"guild_id": guild_id, "tab": "settings"}
    if scanning:
        redirect_kwargs["scanning"] = "1"
    return redirect(url_for("guild_page", **redirect_kwargs))


@app.route("/guild/<guild_id>/scan-status")
def guild_scan_status(guild_id):
    if "user" not in session:
        return jsonify({"scanning": False, "processed": 0, "added": 0, "bot_connected": False})
    bot_connected = bool(state.bot_loop and not state.bot_loop.is_closed())
    status = state.scan_status.get(guild_id, {"scanning": False, "processed": 0, "added": 0})
    status["bot_connected"] = bot_connected
    return jsonify(status)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


# Run via main.py (starts the bot thread alongside Flask).
# Do NOT run this file directly — state.bot_loop will be None
# and web-triggered scans will silently do nothing.
