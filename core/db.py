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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_subscriptions (
                chat_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_sent_at TEXT
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


def db_toggle_schedule_subscription(db_path: Path, chat_id: int) -> bool:
    now_iso = datetime.utcnow().isoformat(timespec="seconds")
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT enabled
            FROM schedule_subscriptions
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
        if row is None:
            conn.execute(
                """
                INSERT INTO schedule_subscriptions (chat_id, enabled, created_at, updated_at, last_sent_at)
                VALUES (?, 1, ?, ?, NULL)
                """,
                (chat_id, now_iso, now_iso),
            )
            return True

        enabled = 0 if int(row[0] or 0) else 1
        conn.execute(
            """
            UPDATE schedule_subscriptions
            SET enabled = ?, updated_at = ?
            WHERE chat_id = ?
            """,
            (enabled, now_iso, chat_id),
        )
        return bool(enabled)


def db_list_due_schedule_chat_ids(db_path: Path, schedule_slot: str) -> list[int]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT chat_id
            FROM schedule_subscriptions
            WHERE enabled = 1
              AND (last_sent_at IS NULL OR last_sent_at <> ?)
            ORDER BY chat_id
            """,
            (schedule_slot,),
        ).fetchall()
    return [int(row[0]) for row in rows]


def db_mark_schedule_sent(db_path: Path, chat_id: int, schedule_slot: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE schedule_subscriptions
            SET last_sent_at = ?, updated_at = ?
            WHERE chat_id = ?
            """,
            (schedule_slot, datetime.utcnow().isoformat(timespec="seconds"), chat_id),
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


def db_delete_cached_response(db_path: Path, prompt_name: str, cache_date: str) -> int:
    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute(
            """
            DELETE FROM prompt_cache
            WHERE prompt_name = ? AND cache_date = ?
            """,
            (prompt_name, cache_date),
        )
    return int(cursor.rowcount or 0)
