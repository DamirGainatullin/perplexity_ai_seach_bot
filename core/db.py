import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional


def db_init(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
                chat_id INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_cache (
                prompt_name TEXT NOT NULL,
                cache_date TEXT NOT NULL,
                response TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (prompt_name, cache_date)
            )
            """
        )


def db_add_chat(db_path: Path, chat_id: int) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO chats (chat_id, created_at)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO NOTHING
            """,
            (chat_id, datetime.utcnow().isoformat(timespec="seconds")),
        )


def db_get_cached_response(db_path: Path, prompt_name: str, cache_date: str) -> Optional[str]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT response
            FROM prompt_cache
            WHERE prompt_name = ? AND cache_date = ?
            """,
            (prompt_name, cache_date),
        ).fetchone()
    if row is None:
        return None
    return str(row[0])


def db_save_cached_response(db_path: Path, prompt_name: str, cache_date: str, response: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO prompt_cache (prompt_name, cache_date, response, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(prompt_name, cache_date) DO UPDATE SET
                response = excluded.response,
                created_at = excluded.created_at
            """,
            (prompt_name, cache_date, response, datetime.utcnow().isoformat(timespec="seconds")),
        )

