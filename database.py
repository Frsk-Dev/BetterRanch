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
    """Create tables and indexes on first run."""
    with _conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type  TEXT    NOT NULL,
                player_name TEXT    NOT NULL,
                value       REAL    NOT NULL,
                quantity    INTEGER DEFAULT 1,
                timestamp   TEXT    DEFAULT (datetime('now')),
                message_id  TEXT    UNIQUE
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_type      ON events (event_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_player    ON events (LOWER(player_name))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON events (timestamp)")


def insert_event(
    event_type: str,
    player_name: str,
    value: float,
    quantity: int = 1,
    message_id: str = None,
) -> bool:
    """Insert one event row. Returns False if message_id already exists (duplicate)."""
    with _conn() as conn:
        try:
            conn.execute(
                """INSERT INTO events (event_type, player_name, value, quantity, message_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_type, player_name, value, quantity, message_id),
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


def _where(event_types: list[str], period: str, player: Optional[str]) -> tuple[str, list]:
    """Build a WHERE clause and matching params list."""
    placeholders = ",".join("?" * len(event_types))
    params: list = list(event_types)
    clause = f"WHERE event_type IN ({placeholders})"

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

def get_collection_stats(event_type: str, period: str, player: Optional[str]) -> list:
    """Eggs or milk totals, grouped by player."""
    where, params = _where([event_type], period, player)
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


def get_ledger_stats(period: str, player: Optional[str]) -> dict:
    """Deposits and withdrawals, each grouped by player."""
    result = {}
    with _conn() as conn:
        for etype in ("deposit", "withdrawal"):
            where, params = _where([etype], period, player)
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


def get_cattle_stats(period: str, player: Optional[str]) -> dict:
    """Cattle buys and sells, each grouped by player."""
    result = {}
    with _conn() as conn:
        for etype in ("cattle_buy", "cattle_sell"):
            where, params = _where([etype], period, player)
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


def get_summary_stats(period: str, player: Optional[str]) -> dict:
    """Single aggregate row per event type — used by /summary."""
    result = {}
    with _conn() as conn:
        for etype in ("eggs", "milk", "deposit", "withdrawal", "cattle_buy", "cattle_sell"):
            where, params = _where([etype], period, player)
            result[etype] = conn.execute(
                f"""SELECT SUM(value)    AS total,
                           SUM(quantity) AS total_qty,
                           COUNT(*)      AS count
                    FROM events {where}""",
                params,
            ).fetchone()
    return result


def get_sales_stats(event_types: list[str], period: str, player: Optional[str]) -> list:
    """Generic qty+value query — used for cattle and stock sales."""
    where, params = _where(event_types, period, player)
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


def get_player_names(event_types: list[str] = None) -> list[str]:
    """Return distinct player names, optionally filtered to specific event types."""
    with _conn() as conn:
        if event_types:
            placeholders = ",".join("?" * len(event_types))
            rows = conn.execute(
                f"""SELECT DISTINCT player_name FROM events
                    WHERE event_type IN ({placeholders})
                    ORDER BY LOWER(player_name)""",
                event_types,
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT player_name FROM events ORDER BY LOWER(player_name)"
            ).fetchall()
        return [r["player_name"] for r in rows]
