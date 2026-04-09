import asyncio
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
PROMPTS_DIR = BASE_DIR / "prompts"
DB_PATH = BASE_DIR / "weekly_bot.sqlite3"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"
MAX_TELEGRAM_MESSAGE_LENGTH = 4000
DEFAULT_MODEL = "perplexity/sonar-pro"
DEFAULT_SEARCH_TYPE = "auto"
DEFAULT_DAY = 0
DEFAULT_TIME = "09:00"
DEFAULT_TZ = "Europe/Moscow"
SYSTEM_PROMPT = "Ты готовишь аккуратную еженедельную новостную сводку на русском языке."


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().lstrip("=")
    return values


def read_prompt(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "cp866"):
        try:
            return path.read_text(encoding=encoding).strip()
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Cannot decode prompt file: {path}")


def resolve_timezone(tz_name: str) -> timezone:
    normalized = (tz_name or "").strip()
    if normalized in {"Europe/Moscow", "MSK", "UTC+3", "+03:00", "+03"}:
        return timezone(timedelta(hours=3))
    if normalized in {"UTC", "Etc/UTC", "+00:00", "+00"}:
        return timezone.utc

    sign = 1
    raw = normalized
    if raw.startswith("UTC"):
        raw = raw[3:]
    if raw.startswith("+"):
        raw = raw[1:]
    elif raw.startswith("-"):
        sign = -1
        raw = raw[1:]

    if ":" in raw:
        hours_text, minutes_text = raw.split(":", 1)
    elif raw.isdigit():
        hours_text, minutes_text = raw, "0"
    else:
        return timezone.utc

    try:
        hours = int(hours_text)
        minutes = int(minutes_text)
    except ValueError:
        return timezone.utc

    offset = timedelta(hours=hours, minutes=minutes) * sign
    return timezone(offset)


def get_prompt_commands() -> dict[str, Path]:
    commands: dict[str, Path] = {}
    for path in sorted(PROMPTS_DIR.glob("*.txt")):
        command = f"/{path.stem.lower()}"
        commands[command] = path
    return commands


def split_message(text: str, limit: int = MAX_TELEGRAM_MESSAGE_LENGTH) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        while len(block) > limit:
            split_at = block.rfind("\n", 0, limit)
            if split_at <= 0:
                split_at = limit
            chunks.append(block[:split_at].strip())
            block = block[split_at:].strip()
        current = block

    if current:
        chunks.append(current)
    return chunks


def db_init() -> None:
    with sqlite3.connect(DB_PATH) as conn:
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


def db_add_chat(chat_id: int) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO chats (chat_id, created_at)
            VALUES (?, ?)
            ON CONFLICT(chat_id) DO NOTHING
            """,
            (chat_id, datetime.utcnow().isoformat(timespec="seconds")),
        )


def db_get_chat_ids() -> list[int]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT chat_id FROM chats ORDER BY created_at ASC").fetchall()
    return [int(row[0]) for row in rows]


def db_get_cached_response(prompt_name: str, cache_date: str) -> str | None:
    with sqlite3.connect(DB_PATH) as conn:
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


def db_save_cached_response(prompt_name: str, cache_date: str, response: str) -> None:
    with sqlite3.connect(DB_PATH) as conn:
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


def tg_request(bot_token: str, method: str, payload: dict | None = None) -> dict:
    url = TELEGRAM_API_URL.format(token=bot_token, method=method)
    data = None
    headers = {}
    http_method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        http_method = "POST"

    req = request.Request(url, data=data, headers=headers, method=http_method)
    try:
        with request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API error {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Telegram network error: {exc}") from exc

    if not body.get("ok"):
        raise RuntimeError(f"Telegram API returned error: {body}")
    return body["result"]


async def tg_get_updates(bot_token: str, offset: int | None, timeout: int = 30) -> list[dict]:
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    query = parse.urlencode(params)
    method = f"getUpdates?{query}"
    return await asyncio.to_thread(tg_request, bot_token, method, None)


async def tg_send_text(bot_token: str, chat_id: int, text: str) -> None:
    for chunk in split_message(text):
        payload = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}
        await asyncio.to_thread(tg_request, bot_token, "sendMessage", payload)


def openrouter_request(
    api_key: str,
    model: str,
    search_type: str,
    user_prompt: str,
) -> str:
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "web_search_options": {"search_type": search_type},
    }
    req = request.Request(
        OPENROUTER_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/",
            "X-Title": "weekly_agents_news",
        },
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=180) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenRouter API error {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"OpenRouter network error: {exc}") from exc

    try:
        return body["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected OpenRouter response: {body}") from exc


async def generate_by_prompt(
    api_key: str,
    model: str,
    search_type: str,
    prompt_file: Path,
    tz: timezone,
) -> str:
    prompt_name = prompt_file.stem.lower()
    cache_date = datetime.now(tz).date().isoformat()
    cached = db_get_cached_response(prompt_name, cache_date)
    if cached is not None:
        return f"{prompt_file.stem.upper()}\n\n{cached}"

    prompt_text = read_prompt(prompt_file)
    answer = await asyncio.to_thread(
        openrouter_request,
        api_key,
        model,
        search_type,
        prompt_text,
    )
    db_save_cached_response(prompt_name, cache_date, answer)
    return f"{prompt_file.stem.upper()}\n\n{answer}"


def next_run_at(schedule_day: int, schedule_time: datetime.time, tz: timezone) -> datetime:
    now = datetime.now(tz)
    target = datetime.combine(now.date(), schedule_time, tzinfo=tz)
    days_ahead = (schedule_day - now.weekday()) % 7
    if days_ahead == 0 and target <= now:
        days_ahead = 7
    return target + timedelta(days=days_ahead)


async def handle_start(bot_token: str, chat_id: int, commands: dict[str, Path]) -> None:
    db_add_chat(chat_id)
    cmd_text = ", ".join(sorted(commands.keys())) if commands else "(нет .txt в prompts)"
    text = (
        "Чат зарегистрирован для еженедельной рассылки.\n"
        "Доступные команды по промптам:\n"
        f"{cmd_text}"
    )
    await tg_send_text(bot_token, chat_id, text)


async def handle_prompt_command(
    bot_token: str,
    chat_id: int,
    command: str,
    commands: dict[str, Path],
    api_key: str,
    model: str,
    search_type: str,
    tz: timezone,
    request_lock: asyncio.Lock,
) -> None:
    prompt_file = commands.get(command)
    if prompt_file is None:
        return

    await tg_send_text(bot_token, chat_id, f"Собираю сводку для команды {command}...")
    try:
        async with request_lock:
            result = await generate_by_prompt(api_key, model, search_type, prompt_file, tz)
        await tg_send_text(bot_token, chat_id, result)
    except Exception as exc:
        await tg_send_text(bot_token, chat_id, f"Ошибка при запросе {command}: {exc}")


async def poll_loop(
    bot_token: str,
    commands: dict[str, Path],
    api_key: str,
    model: str,
    search_type: str,
    tz: timezone,
    request_lock: asyncio.Lock,
) -> None:
    offset: int | None = None
    while True:
        try:
            updates = await tg_get_updates(bot_token, offset=offset, timeout=30)
            for update in updates:
                offset = update["update_id"] + 1
                message = update.get("message") or update.get("edited_message")
                if not message:
                    continue
                chat = message.get("chat") or {}
                chat_id = chat.get("id")
                text = (message.get("text") or "").strip()
                if not chat_id or not text:
                    continue

                command = text.split()[0].split("@")[0].lower()
                if command == "/start":
                    await handle_start(bot_token, chat_id, commands)
                elif command in commands:
                    await handle_prompt_command(
                        bot_token,
                        chat_id,
                        command,
                        commands,
                        api_key,
                        model,
                        search_type,
                        tz,
                        request_lock,
                    )
        except Exception as exc:
            print(f"[polling] {exc}", file=sys.stderr)
            await asyncio.sleep(5)


async def weekly_broadcast_loop(
    bot_token: str,
    commands: dict[str, Path],
    api_key: str,
    model: str,
    search_type: str,
    schedule_day: int,
    schedule_time: datetime.time,
    tz: timezone,
    request_lock: asyncio.Lock,
) -> None:
    while True:
        run_at = next_run_at(schedule_day, schedule_time, tz)
        delay = max((run_at - datetime.now(tz)).total_seconds(), 1)
        print(f"[scheduler] Next run at {run_at.isoformat()}")
        await asyncio.sleep(delay)

        chat_ids = db_get_chat_ids()
        if not chat_ids:
            print("[scheduler] No registered chats yet")
            continue

        for command, prompt_file in commands.items():
            try:
                async with request_lock:
                    result = await generate_by_prompt(api_key, model, search_type, prompt_file, tz)
            except Exception as exc:
                print(f"[scheduler] Failed to generate for {command}: {exc}", file=sys.stderr)
                continue

            for chat_id in chat_ids:
                try:
                    await tg_send_text(bot_token, chat_id, result)
                except Exception as exc:
                    print(f"[scheduler] Failed to send to {chat_id}: {exc}", file=sys.stderr)


def load_settings() -> dict:
    env = load_env(ENV_PATH)
    bot_token = env.get("BOT_TOKEN", "")
    openrouter_api_key = env.get("OPENROUTER_API_KEY", "") or env.get("OPENROUTER_API_TOKEN", "")
    if not bot_token or not openrouter_api_key:
        raise RuntimeError("BOT_TOKEN and OPENROUTER_API_KEY must exist in .env")

    model = env.get("OPENROUTER_MODEL", DEFAULT_MODEL)
    search_type = env.get("OPENROUTER_SEARCH_TYPE", DEFAULT_SEARCH_TYPE)
    schedule_day = int(env.get("WEEKLY_SEND_DAY", str(DEFAULT_DAY)))
    schedule_time = datetime.strptime(env.get("WEEKLY_SEND_TIME", DEFAULT_TIME), "%H:%M").time()
    tz = resolve_timezone(env.get("TZ", DEFAULT_TZ))

    return {
        "bot_token": bot_token,
        "openrouter_api_key": openrouter_api_key,
        "model": model,
        "search_type": search_type,
        "schedule_day": schedule_day,
        "schedule_time": schedule_time,
        "tz": tz,
    }


async def main() -> None:
    settings = load_settings()
    commands = get_prompt_commands()
    if not commands:
        raise RuntimeError("No .txt prompts found in prompts/")

    db_init()
    request_lock = asyncio.Lock()

    print(f"[startup] Prompt commands: {', '.join(sorted(commands.keys()))}")
    await asyncio.gather(
        poll_loop(
            settings["bot_token"],
            commands,
            settings["openrouter_api_key"],
            settings["model"],
            settings["search_type"],
            settings["tz"],
            request_lock,
        ),
        weekly_broadcast_loop(
            settings["bot_token"],
            commands,
            settings["openrouter_api_key"],
            settings["model"],
            settings["search_type"],
            settings["schedule_day"],
            settings["schedule_time"],
            settings["tz"],
            request_lock,
        ),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
