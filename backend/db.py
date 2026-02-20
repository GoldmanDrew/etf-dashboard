"""
SQLite-backed storage for borrow rate history and dashboard state.

Uses synchronous SQLite (sqlite3) — fast enough for a single-user
internal dashboard. Swap to aiosqlite if needed.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS borrow_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    borrow_net  REAL,
    borrow_fee  REAL,
    borrow_rebate REAL,
    shares_available INTEGER,
    UNIQUE(symbol, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_borrow_sym_ts
    ON borrow_history(symbol, timestamp);

CREATE TABLE IF NOT EXISTS system_state (
    key   TEXT PRIMARY KEY,
    value TEXT
);
"""


class DashboardDB:
    """Simple SQLite wrapper for the dashboard."""

    def __init__(self, db_path: str | Path = "data/dashboard.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()
        logger.info(f"DB connected: {self.db_path}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        return self._conn

    # ── Borrow History ─────────────────────────

    def insert_borrow_snapshot(
        self,
        borrow_map: dict[str, float],
        fee_map: dict[str, float],
        rebate_map: dict[str, float],
        available_map: dict[str, int],
        timestamp: Optional[dt.datetime] = None,
    ):
        """Store a snapshot of borrow rates."""
        ts = (timestamp or dt.datetime.utcnow()).isoformat()
        rows = []
        for sym in borrow_map:
            rows.append((
                sym, ts,
                borrow_map.get(sym),
                fee_map.get(sym),
                rebate_map.get(sym),
                available_map.get(sym),
            ))
        if rows:
            self.conn.executemany(
                "INSERT OR REPLACE INTO borrow_history "
                "(symbol, timestamp, borrow_net, borrow_fee, borrow_rebate, shares_available) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                rows,
            )
            self.conn.commit()
            logger.info(f"Stored {len(rows)} borrow records at {ts}")

    def get_borrow_history(
        self,
        symbol: str,
        limit: int = 100,
    ) -> list[dict]:
        """Get recent borrow history for a symbol."""
        cur = self.conn.execute(
            "SELECT timestamp, borrow_net, shares_available "
            "FROM borrow_history WHERE symbol = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (symbol.upper(), limit),
        )
        rows = cur.fetchall()
        return [
            {"timestamp": r[0], "borrow_net": r[1], "shares_available": r[2]}
            for r in reversed(rows)
        ]

    # ── System State ───────────────────────────

    def set_state(self, key: str, value):
        self.conn.execute(
            "INSERT OR REPLACE INTO system_state (key, value) VALUES (?, ?)",
            (key, json.dumps(value)),
        )
        self.conn.commit()

    def get_state(self, key: str, default=None):
        cur = self.conn.execute(
            "SELECT value FROM system_state WHERE key = ?", (key,)
        )
        row = cur.fetchone()
        if row:
            return json.loads(row[0])
        return default

    def prune_old_history(self, days: int = 30):
        """Remove borrow history older than N days."""
        cutoff = (dt.datetime.utcnow() - dt.timedelta(days=days)).isoformat()
        self.conn.execute(
            "DELETE FROM borrow_history WHERE timestamp < ?", (cutoff,)
        )
        self.conn.commit()
