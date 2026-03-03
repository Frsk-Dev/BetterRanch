import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Optional


DB_PATH = "betterranch.db"


@contextmanager
def _conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables and indexes on first run, and migrate existing schemas."""
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id         TEXT PRIMARY KEY,
                ranch_channel_id TEXT,
                camp_channel_id  TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    TEXT,
                event_type  TEXT    NOT NULL,
                player_name TEXT    NOT NULL,
                value       REAL    NOT NULL,
                quantity    INTEGER DEFAULT 1,
                timestamp   TEXT    DEFAULT (datetime('now')),
                message_id  TEXT    UNIQUE
            )
        """)
        # Migrate existing schema: add guild_id column if it is missing.
        # This must run before creating the index that references it.
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(events)")}
        if "guild_id" not in existing_cols:
            conn.execute("ALTER TABLE events ADD COLUMN guild_id TEXT")

        conn.execute("CREATE INDEX IF NOT EXISTS idx_guild     ON events (guild_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_type      ON events (event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_player    ON events (LOWER(player_name))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON events (timestamp)")


# ---------------------------------------------------------------------------
# Guild config
# ---------------------------------------------------------------------------

def get_guild_config(guild_id: str) -> Optional[sqlite3.Row]:
    """Return the config row for a guild, or None if not configured."""
    with _conn() as conn:
        return conn.execute(
            "SELECT * FROM guild_config WHERE guild_id = ?", (guild_id,)
        ).fetchone()


def upsert_guild_config(
    guild_id: str,
    ranch_channel_id: Optional[str],
    camp_channel_id: Optional[str],
) -> None:
    """Insert or update the channel config for a guild."""
    with _conn() as conn:
        conn.execute(
            """INSERT INTO guild_config (guild_id, ranch_channel_id, camp_channel_id)
               VALUES (?, ?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET
                   ranch_channel_id = excluded.ranch_channel_id,
                   camp_channel_id  = excluded.camp_channel_id""",
            (guild_id, ranch_channel_id, camp_channel_id),
        )


def migrate_null_events(guild_id: str) -> int:
    """Assign guild_id to events imported before multi-guild support.
    Returns the number of rows updated."""
    with _conn() as conn:
        cursor = conn.execute(
            "UPDATE events SET guild_id = ? WHERE guild_id IS NULL",
            (guild_id,),
        )
        return cursor.rowcount


# ---------------------------------------------------------------------------
# Event insertion
# ---------------------------------------------------------------------------

def insert_event(
    event_type: str,
    player_name: str,
    value: float,
    quantity: int = 1,
    message_id: str = None,
    guild_id: str = None,
) -> bool:
    """Insert one event row. Returns False if message_id already exists (duplicate)."""
    with _conn() as conn:
        try:
            conn.execute(
                """INSERT INTO events (guild_id, event_type, player_name, value, quantity, message_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (guild_id, event_type, player_name, value, quantity, message_id),
            )
            return True
        except sqlite3.IntegrityError:
            return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _cutoff(period: str) -> Optional[str]:
    if period == "day":
        return (datetime.utcnow() - timedelta(days=1)).isoformat()
    if period == "week":
        return (datetime.utcnow() - timedelta(weeks=1)).isoformat()
    return None


def _where(
    event_types: list[str],
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> tuple[str, list]:
    """Build a WHERE clause and matching params list."""
    placeholders = ",".join("?" * len(event_types))
    params: list = list(event_types)
    clause = f"WHERE event_type IN ({placeholders})"

    if guild_id:
        clause += " AND guild_id = ?"
        params.append(guild_id)

    cutoff = _cutoff(period)
    if cutoff:
        clause += " AND timestamp >= ?"
        params.append(cutoff)

    if player:
        clause += " AND LOWER(player_name) = LOWER(?)"
        params.append(player)

    return clause, params


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def get_collection_stats(
    event_type: str,
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> list:
    """Eggs or milk totals, grouped by player."""
    where, params = _where([event_type], period, player, guild_id)
    with _conn() as conn:
        return conn.execute(
            f"""SELECT player_name,
                       SUM(value)  AS total,
                       COUNT(*)    AS collections
                FROM events {where}
                GROUP BY LOWER(player_name)
                ORDER BY total DESC""",
            params,
        ).fetchall()


def get_ledger_stats(
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> dict:
    """Deposits and withdrawals, each grouped by player."""
    result = {}
    with _conn() as conn:
        for etype in ("deposit", "withdrawal"):
            where, params = _where([etype], period, player, guild_id)
            result[etype] = conn.execute(
                f"""SELECT player_name,
                           SUM(value) AS total,
                           COUNT(*)   AS count
                    FROM events {where}
                    GROUP BY LOWER(player_name)
                    ORDER BY total DESC""",
                params,
            ).fetchall()
    return result


def get_cattle_stats(
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> dict:
    """Cattle buys and sells, each grouped by player."""
    result = {}
    with _conn() as conn:
        for etype in ("cattle_buy", "cattle_sell"):
            where, params = _where([etype], period, player, guild_id)
            result[etype] = conn.execute(
                f"""SELECT player_name,
                           SUM(value)    AS total_value,
                           SUM(quantity) AS total_qty,
                           COUNT(*)      AS transactions
                    FROM events {where}
                    GROUP BY LOWER(player_name)
                    ORDER BY total_value DESC""",
                params,
            ).fetchall()
    return result


def get_summary_stats(
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> dict:
    """Single aggregate row per event type — used by /summary."""
    result = {}
    with _conn() as conn:
        for etype in ("eggs", "milk", "deposit", "withdrawal", "cattle_buy", "cattle_sell", "materials", "supplies", "stock_sale"):
            where, params = _where([etype], period, player, guild_id)
            result[etype] = conn.execute(
                f"""SELECT SUM(value)    AS total,
                           SUM(quantity) AS total_qty,
                           COUNT(*)      AS count
                    FROM events {where}""",
                params,
            ).fetchone()
    return result


def get_sales_stats(
    event_types: list[str],
    period: str,
    player: Optional[str],
    guild_id: Optional[str] = None,
) -> list:
    """Generic qty+value query — used for cattle and stock sales."""
    where, params = _where(event_types, period, player, guild_id)
    with _conn() as conn:
        return conn.execute(
            f"""SELECT player_name,
                       SUM(value)    AS total_value,
                       SUM(quantity) AS total_qty,
                       COUNT(*)      AS transactions
                FROM events {where}
                GROUP BY LOWER(player_name)
                ORDER BY total_value DESC""",
            params,
        ).fetchall()


def get_configured_guild_ids() -> list[str]:
    """Return all guild IDs that have a row in guild_config."""
    with _conn() as conn:
        rows = conn.execute("SELECT guild_id FROM guild_config").fetchall()
        return [r["guild_id"] for r in rows]


def get_recent_events(guild_id: str, limit: int = 20) -> list:
    """Return the most recent events for a guild, newest first."""
    with _conn() as conn:
        return conn.execute(
            """SELECT event_type, player_name, value, quantity, timestamp
               FROM events
               WHERE guild_id = ?
               ORDER BY timestamp DESC
               LIMIT ?""",
            (guild_id, limit),
        ).fetchall()


def delete_player_events(player_name: str, guild_id: str) -> int:
    """Delete all events for a player in a guild. Returns number of rows deleted."""
    with _conn() as conn:
        cursor = conn.execute(
            "DELETE FROM events WHERE LOWER(player_name) = LOWER(?) AND guild_id = ?",
            (player_name, guild_id),
        )
        return cursor.rowcount


def get_player_names(
    event_types: list[str] = None,
    guild_id: Optional[str] = None,
) -> list[str]:
    """Return distinct player names, optionally filtered to specific event types and guild."""
    conditions = []
    params = []

    if event_types:
        placeholders = ",".join("?" * len(event_types))
        conditions.append(f"event_type IN ({placeholders})")
        params.extend(event_types)

    if guild_id:
        conditions.append("guild_id = ?")
        params.append(guild_id)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    with _conn() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT player_name FROM events {where} ORDER BY LOWER(player_name)",
            params,
        ).fetchall()
        return [r["player_name"] for r in rows]
