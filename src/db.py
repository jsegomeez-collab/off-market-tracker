from __future__ import annotations

import sqlite3
import json
from pathlib import Path
from datetime import datetime
from typing import Iterable
from .models import Property

DB_PATH = Path(__file__).parent.parent / "data" / "deals.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS properties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    parcel_id TEXT,
    address TEXT NOT NULL,
    city TEXT,
    county TEXT,
    state TEXT DEFAULT 'PA',
    zip TEXT,
    owner_name TEXT,
    owner_address TEXT,
    listing_price REAL,
    property_type TEXT,
    description TEXT,
    url TEXT,
    raw TEXT,
    score INTEGER DEFAULT 0,
    score_reasons TEXT,
    notified INTEGER DEFAULT 0,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, source_id)
);

CREATE INDEX IF NOT EXISTS idx_score ON properties(score DESC);
CREATE INDEX IF NOT EXISTS idx_first_seen ON properties(first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_notified ON properties(notified);

CREATE TABLE IF NOT EXISTS skip_trace (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_name TEXT NOT NULL,
    city TEXT,
    state TEXT DEFAULT 'PA',
    phones TEXT,
    age TEXT,
    current_address TEXT,
    relatives TEXT,
    source TEXT DEFAULT 'truepeoplesearch',
    raw TEXT,
    looked_up_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(owner_name, city)
);

CREATE INDEX IF NOT EXISTS idx_skip_owner ON skip_trace(owner_name);

CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    items_found INTEGER DEFAULT 0,
    items_new INTEGER DEFAULT 0,
    error TEXT
);
"""


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def upsert_properties(conn: sqlite3.Connection, props: Iterable[Property]) -> tuple[int, int]:
    found = 0
    new = 0
    cur = conn.cursor()
    for p in props:
        found += 1
        row = p.to_db_row()
        cur.execute(
            """
            INSERT INTO properties
                (source, source_id, parcel_id, address, city, county, state, zip,
                 owner_name, owner_address, listing_price, property_type, description, url, raw)
            VALUES (:source, :source_id, :parcel_id, :address, :city, :county, :state, :zip,
                    :owner_name, :owner_address, :listing_price, :property_type, :description, :url, :raw)
            ON CONFLICT(source, source_id) DO UPDATE SET
                last_seen = CURRENT_TIMESTAMP,
                listing_price = COALESCE(excluded.listing_price, properties.listing_price),
                description = COALESCE(excluded.description, properties.description)
            """,
            row,
        )
        if cur.rowcount == 1 and cur.lastrowid:
            check = conn.execute(
                "SELECT first_seen, last_seen FROM properties WHERE id = ?", (cur.lastrowid,)
            ).fetchone()
            if check and check["first_seen"] == check["last_seen"]:
                new += 1
    conn.commit()
    return found, new


def update_score(conn: sqlite3.Connection, prop_id: int, score: int, reasons: list[str]) -> None:
    conn.execute(
        "UPDATE properties SET score = ?, score_reasons = ? WHERE id = ?",
        (score, json.dumps(reasons), prop_id),
    )
    conn.commit()


def unscored_properties(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM properties WHERE score = 0 AND score_reasons IS NULL"
    ).fetchall()


def top_unnotified(conn: sqlite3.Connection, min_score: int = 40, limit: int = 20) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT p.*,
               st.phones AS st_phones,
               st.age AS st_age,
               st.current_address AS st_current_address
        FROM properties p
        LEFT JOIN skip_trace st
          ON st.owner_name = p.owner_name
         AND COALESCE(st.city,'') = COALESCE(p.city,'')
        WHERE p.notified = 0 AND p.score >= ?
        ORDER BY p.score DESC
        LIMIT ?
        """,
        (min_score, limit),
    ).fetchall()


def mark_notified(conn: sqlite3.Connection, ids: list[int]) -> None:
    conn.executemany("UPDATE properties SET notified = 1 WHERE id = ?", [(i,) for i in ids])
    conn.commit()


def log_run(
    conn: sqlite3.Connection,
    source: str,
    started: datetime,
    found: int,
    new: int,
    error: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO scrape_runs (source, started_at, finished_at, items_found, items_new, error)
           VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?)""",
        (source, started.isoformat(), found, new, error),
    )
    conn.commit()
