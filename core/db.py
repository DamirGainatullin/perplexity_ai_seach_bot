import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    columns = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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
        _ensure_column(conn, "prompt_cache", "item_count", "INTEGER NOT NULL DEFAULT -1")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digest_history (
                prompt_name TEXT NOT NULL,
                url TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                published_date TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                PRIMARY KEY (prompt_name, url)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_digest_history_prompt_seen
            ON digest_history (prompt_name, last_seen_at DESC)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tavily_daily_usage (
                usage_date TEXT PRIMARY KEY,
                credits REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_state (
                state_key TEXT PRIMARY KEY,
                state_value TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
            (chat_id, _utcnow_iso()),
        )


def db_toggle_schedule_subscription(
    db_path: Path,
    chat_id: int,
    *,
    initial_last_sent_at: str | None = None,
) -> bool:
    now_iso = _utcnow_iso()
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
                VALUES (?, 1, ?, ?, ?)
                """,
                (chat_id, now_iso, now_iso, initial_last_sent_at),
            )
            return True

        enabled = 0 if int(row[0] or 0) else 1
        conn.execute(
            """
            UPDATE schedule_subscriptions
            SET enabled = ?, updated_at = ?,
                last_sent_at = CASE WHEN ? = 1 AND ? IS NOT NULL THEN ? ELSE last_sent_at END
            WHERE chat_id = ?
            """,
            (enabled, now_iso, enabled, initial_last_sent_at, initial_last_sent_at, chat_id),
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
            (schedule_slot, _utcnow_iso(), chat_id),
        )


def db_prepare_schedule_mode(
    db_path: Path,
    digest_mode: str,
    *,
    elapsed_schedule_slot: str | None = None,
) -> tuple[str, int]:
    now_iso = _utcnow_iso()
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT state_value FROM app_state WHERE state_key = 'digest_mode'"
        ).fetchone()
        previous_mode = str(row[0]) if row else ""
        skipped_chats = 0
        if previous_mode != digest_mode and elapsed_schedule_slot:
            cursor = conn.execute(
                """
                UPDATE schedule_subscriptions
                SET last_sent_at = ?, updated_at = ?
                WHERE enabled = 1
                """,
                (elapsed_schedule_slot, now_iso),
            )
            skipped_chats = int(cursor.rowcount or 0)
        conn.execute(
            """
            INSERT INTO app_state (state_key, state_value, updated_at)
            VALUES ('digest_mode', ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                state_value = excluded.state_value,
                updated_at = excluded.updated_at
            """,
            (digest_mode, now_iso),
        )
        conn.commit()
    return previous_mode, skipped_chats


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


def db_get_cached_result(db_path: Path, prompt_name: str, cache_date: str) -> Optional[tuple[str, int]]:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT response, item_count
            FROM prompt_cache
            WHERE prompt_name = ? AND cache_date = ?
            """,
            (prompt_name, cache_date),
        ).fetchone()
    if row is None:
        return None
    return str(row[0]), int(row[1] if row[1] is not None else -1)


def db_save_cached_response(
    db_path: Path,
    prompt_name: str,
    cache_date: str,
    response: str,
    item_count: int = -1,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO prompt_cache (prompt_name, cache_date, response, created_at, item_count)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(prompt_name, cache_date) DO UPDATE SET
                response = excluded.response,
                created_at = excluded.created_at,
                item_count = excluded.item_count
            """,
            (
                prompt_name,
                cache_date,
                response,
                _utcnow_iso(),
                int(item_count),
            ),
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


def db_get_recent_digest_history(
    db_path: Path,
    prompt_name: str,
    *,
    limit: int = 80,
) -> list[dict[str, str]]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT url, title, summary, published_date, last_seen_at
            FROM digest_history
            WHERE prompt_name = ?
            ORDER BY last_seen_at DESC
            LIMIT ?
            """,
            (prompt_name, max(1, int(limit))),
        ).fetchall()
    return [
        {
            "url": str(row[0]),
            "title": str(row[1]),
            "summary": str(row[2]),
            "date": str(row[3]),
            "last_seen_at": str(row[4]),
        }
        for row in rows
    ]


def db_save_digest_history(db_path: Path, prompt_name: str, rows: list[dict[str, Any]]) -> None:
    now_iso = _utcnow_iso()
    values: list[tuple[str, str, str, str, str, str, str]] = []
    for row in rows:
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        values.append(
            (
                prompt_name,
                url,
                str(row.get("title", "")).strip(),
                str(row.get("summary", "")).strip(),
                str(row.get("date", "")).strip(),
                now_iso,
                now_iso,
            )
        )
    if not values:
        return
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO digest_history (
                prompt_name, url, title, summary, published_date, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(prompt_name, url) DO UPDATE SET
                title = excluded.title,
                summary = excluded.summary,
                published_date = excluded.published_date,
                last_seen_at = excluded.last_seen_at
            """,
            values,
        )


def db_get_tavily_daily_usage(db_path: Path, usage_date: str) -> float:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT credits FROM tavily_daily_usage WHERE usage_date = ?",
            (usage_date,),
        ).fetchone()
    return float(row[0]) if row else 0.0


def db_reserve_tavily_credits(
    db_path: Path,
    usage_date: str,
    requested_credits: float,
    daily_limit: float,
) -> bool:
    requested = max(0.0, float(requested_credits))
    limit = max(0.0, float(daily_limit))
    now_iso = _utcnow_iso()
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT credits FROM tavily_daily_usage WHERE usage_date = ?",
            (usage_date,),
        ).fetchone()
        current = float(row[0]) if row else 0.0
        if current + requested > limit + 1e-9:
            conn.rollback()
            return False
        conn.execute(
            """
            INSERT INTO tavily_daily_usage (usage_date, credits, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(usage_date) DO UPDATE SET
                credits = excluded.credits,
                updated_at = excluded.updated_at
            """,
            (usage_date, current + requested, now_iso),
        )
        conn.commit()
    return True


def db_reconcile_tavily_credits(
    db_path: Path,
    usage_date: str,
    reserved_credits: float,
    actual_credits: float,
) -> None:
    reserved = max(0.0, float(reserved_credits))
    actual = max(0.0, float(actual_credits))
    now_iso = _utcnow_iso()
    with sqlite3.connect(db_path, timeout=30) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT credits FROM tavily_daily_usage WHERE usage_date = ?",
            (usage_date,),
        ).fetchone()
        current = float(row[0]) if row else 0.0
        adjusted = max(0.0, current - reserved + actual)
        conn.execute(
            """
            INSERT INTO tavily_daily_usage (usage_date, credits, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(usage_date) DO UPDATE SET
                credits = excluded.credits,
                updated_at = excluded.updated_at
            """,
            (usage_date, adjusted, now_iso),
        )
        conn.commit()
