"""
TikTok Live Gift/Powerup Tracker
---------------------------------
Polls a TikTok username every 60s. When live, connects to the stream and
logs every gift/powerup event to SQLite. Broadcasts events + expiry info
to the browser over WebSockets (Flask-SocketIO). Serves index.html.
"""

# eventlet.monkey_patch() MUST run before anything else imports socket/ssl/threading
# (including psycopg2, asyncio internals pulled in by TikTokLive, etc.) or you get
# "RLock(s) were not greened" errors and unreliable async behavior.
import eventlet
eventlet.monkey_patch()

import os
import sqlite3
import asyncio
import threading
import time
import logging
import traceback
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tracker")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
USERNAME = os.environ.get("TIKTOK_USERNAME", "your_username_here")
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC", "60"))
POWERUP_TTL_DAYS = int(os.environ.get("POWERUP_TTL_DAYS", "5"))
DB_PATH = os.environ.get("DB_PATH", "powerups.db")
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "5000"))

DATABASE_URL = os.environ.get("DATABASE_URL")
USE_POSTGRES = bool(DATABASE_URL)

# Import everything else AFTER monkey_patch() has run
from flask import Flask, jsonify, send_from_directory
from flask_socketio import SocketIO

from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, DisconnectEvent, GiftEvent

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras

# ---------------------------------------------------------------------------
# Flask + Socket.IO setup
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder="templates", template_folder="templates")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_LOCK = threading.Lock()


def get_conn():
    if USE_POSTGRES:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        conn.autocommit = False
        return conn
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with DB_LOCK:
        conn = get_conn()
        try:
            if USE_POSTGRES:
                cur = conn.cursor()
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gift_events (
                        id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL,
                        nickname TEXT,
                        powerup_type TEXT NOT NULL,
                        gift_count INTEGER DEFAULT 1,
                        received_at TIMESTAMPTZ NOT NULL,
                        expires_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_gift_events_expires ON gift_events(expires_at)"
                )
                conn.commit()
            else:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS gift_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        nickname TEXT,
                        powerup_type TEXT NOT NULL,
                        gift_count INTEGER DEFAULT 1,
                        received_at TEXT NOT NULL,
                        expires_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_gift_events_expires ON gift_events(expires_at)"
                )
                conn.commit()
        finally:
            conn.close()
    log.info("Database ready (%s)", "Postgres" if USE_POSTGRES else f"SQLite at {DB_PATH}")


def record_gift(username: str, nickname: str, powerup_type: str, gift_count: int = 1):
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=POWERUP_TTL_DAYS)
    row = {
        "username": username,
        "nickname": nickname,
        "powerup_type": powerup_type,
        "gift_count": gift_count,
        "received_at": now.isoformat(),
        "expires_at": expires.isoformat(),
    }
    with DB_LOCK:
        conn = get_conn()
        try:
            if USE_POSTGRES:
                cur = conn.cursor()
                cur.execute(
                    """
                    INSERT INTO gift_events (username, nickname, powerup_type, gift_count, received_at, expires_at)
                    VALUES (%(username)s, %(nickname)s, %(powerup_type)s, %(gift_count)s, %(received_at)s, %(expires_at)s)
                    RETURNING id
                    """,
                    row,
                )
                row["id"] = cur.fetchone()[0]
                conn.commit()
            else:
                cur = conn.execute(
                    """
                    INSERT INTO gift_events (username, nickname, powerup_type, gift_count, received_at, expires_at)
                    VALUES (:username, :nickname, :powerup_type, :gift_count, :received_at, :expires_at)
                    """,
                    row,
                )
                conn.commit()
                row["id"] = cur.lastrowid
        finally:
            conn.close()
    return row


def _row_to_dict(cols, row):
    d = dict(zip(cols, row))
    # Normalize datetime objects (Postgres) to ISO strings so the frontend
    # gets the same shape regardless of backend.
    for k in ("received_at", "expires_at"):
        if hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d


def fetch_active_powerups():
    """Return all powerups that have not yet expired, newest first."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with DB_LOCK:
        conn = get_conn()
        try:
            if USE_POSTGRES:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, username, nickname, powerup_type, gift_count, received_at, expires_at "
                    "FROM gift_events WHERE expires_at > %s ORDER BY received_at DESC",
                    (now_iso,),
                )
                cols = [d[0] for d in cur.description]
                return [_row_to_dict(cols, r) for r in cur.fetchall()]
            else:
                cur = conn.execute(
                    "SELECT * FROM gift_events WHERE expires_at > ? ORDER BY received_at DESC",
                    (now_iso,),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()


def fetch_all_history(limit=500):
    with DB_LOCK:
        conn = get_conn()
        try:
            if USE_POSTGRES:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id, username, nickname, powerup_type, gift_count, received_at, expires_at "
                    "FROM gift_events ORDER BY received_at DESC LIMIT %s",
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                return [_row_to_dict(cols, r) for r in cur.fetchall()]
            else:
                cur = conn.execute(
                    "SELECT * FROM gift_events ORDER BY received_at DESC LIMIT ?",
                    (limit,),
                )
                return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------
@app.route("/")
def index():
    return send_from_directory("templates", "index.html")


@app.route("/api/history")
def api_history():
    """Full history (for reference / debugging)."""
    return jsonify(fetch_all_history())


@app.route("/api/active")
def api_active():
    """Only currently-active (non-expired) powerups — used to populate the table on load."""
    return jsonify(fetch_active_powerups())


@app.route("/api/status")
def api_status():
    return jsonify({"live": STATE["is_live"], "username": USERNAME})


# ---------------------------------------------------------------------------
# TikTokLive client logic
# ---------------------------------------------------------------------------
STATE = {"is_live": False}


def build_client() -> TikTokLiveClient:
    client = TikTokLiveClient(unique_id=USERNAME)

    @client.on(ConnectEvent)
    async def on_connect(_: ConnectEvent):
        STATE["is_live"] = True
        log.info("Connected to @%s's live stream.", USERNAME)
        socketio.emit("stream_status", {"live": True})

    @client.on(DisconnectEvent)
    async def on_disconnect(_: DisconnectEvent):
        STATE["is_live"] = False
        log.info("Disconnected from @%s's live stream.", USERNAME)
        socketio.emit("stream_status", {"live": False})

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        # Some gifts are "streakable" and fire repeatedly while the combo builds;
        # only record once the streak has ended (or if it's not streakable at all).
        if event.gift.streakable and not event.streaking:
            handle_gift(event)
        elif not event.gift.streakable:
            handle_gift(event)

    return client


def handle_gift(event: GiftEvent):
    user = event.user.unique_id or "unknown"
    nickname = event.user.nickname or user
    powerup_type = event.gift.name or "Unknown Gift"
    count = event.gift.count or event.repeat_count or 1

    row = record_gift(user, nickname, powerup_type, count)
    log.info("Recorded gift: %s x%s from %s (@%s)", powerup_type, count, nickname, user)

    # Push to any connected browsers immediately
    socketio.emit("new_powerup", row)


async def run_live_session(client: TikTokLiveClient):
    """Run the client until the stream ends or an error occurs."""
    try:
        await client.start(fetch_room_info=True)
    except Exception as e:
        log.warning("Live session ended/error: %s", e)
    finally:
        STATE["is_live"] = False
        socketio.emit("stream_status", {"live": False})


def polling_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        try:
            client = build_client()
            is_live = loop.run_until_complete(client.is_live())

            if is_live:
                log.info("@%s is LIVE. Connecting...", USERNAME)
                loop.run_until_complete(run_live_session(client))
                log.info("Stream ended. Resuming polling.")
            else:
                log.info("@%s is not live. Checking again in %ss.", USERNAME, POLL_INTERVAL_SEC)
        except Exception as e:
            log.error("Polling loop error: %s", e)
            log.error(traceback.format_exc())

        time.sleep(POLL_INTERVAL_SEC)


def expiry_sweeper():
    """Periodically tell the frontend to refresh expiry state (cheap heartbeat)."""
    while True:
        time.sleep(30)
        socketio.emit("heartbeat", {"server_time": datetime.now(timezone.utc).isoformat()})


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if USERNAME == "your_username_here":
        log.warning("Set TIKTOK_USERNAME env var to your actual TikTok username!")

    init_db()

    threading.Thread(target=polling_loop, daemon=True).start()
    threading.Thread(target=expiry_sweeper, daemon=True).start()

    log.info("Starting web server on http://%s:%s", HOST, PORT)
    socketio.run(app, host=HOST, port=PORT, allow_unsafe_werkzeug=True)
