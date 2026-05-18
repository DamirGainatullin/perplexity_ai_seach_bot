import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from core.db import db_add_chat, db_get_cached_response, db_init, db_save_cached_response
from core.env import load_env, read_text_with_fallback, resolve_timezone
from core.telegram_api import tg_get_updates, tg_send_text
from pipelines.engine import format_digest_response, run_budget_pipeline
from pipelines.models import PromptProfile
from pipelines.profiles import load_profiles


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
PROMPTS_DIR = BASE_DIR / "prompts"
DB_PATH = BASE_DIR / "weekly_bot.sqlite3"
DEFAULT_TZ = "Europe/Moscow"


def get_prompt_commands(profiles: dict[str, PromptProfile]) -> dict[str, Path]:
    return {command: profile.prompt_path for command, profile in profiles.items()}


async def generate_by_profile(
    tavily_api_key: str,
    profile: PromptProfile,
    tz,
) -> str:
    prompt_name = profile.name
    cache_date = datetime.now(tz).date().isoformat()
    cached = db_get_cached_response(DB_PATH, prompt_name, cache_date)
    if cached is not None:
        return cached

    prompt_text = read_text_with_fallback(profile.prompt_path)
    rows, usage = await asyncio.to_thread(
        run_budget_pipeline,
        profile,
        prompt_text,
        tavily_api_key,
        datetime.now(tz).date(),
    )
    response_text = format_digest_response(profile, rows, usage)
    db_save_cached_response(DB_PATH, prompt_name, cache_date, response_text)
    return response_text


async def handle_start(bot_token: str, chat_id: int, commands: dict[str, Path]) -> None:
    db_add_chat(DB_PATH, chat_id)
    cmd_text = ", ".join(sorted(commands.keys())) if commands else "(нет доступных команд)"
    text = (
        "Чат зарегистрирован для еженедельной рассылки.\n"
        "Доступные команды:\n"
        f"{cmd_text}"
    )
    await tg_send_text(bot_token, chat_id, text)


async def handle_prompt_command(
    bot_token: str,
    chat_id: int,
    command: str,
    profiles: dict[str, PromptProfile],
    tavily_api_key: str,
    tz,
    request_lock: asyncio.Lock,
) -> None:
    profile = profiles.get(command)
    if profile is None:
        return

    await tg_send_text(bot_token, chat_id, f"Собираю сводку для команды {command}...")
    try:
        async with request_lock:
            result = await generate_by_profile(tavily_api_key, profile, tz)
        await tg_send_text(bot_token, chat_id, result)
    except Exception as exc:
        await tg_send_text(bot_token, chat_id, f"Ошибка при запросе {command}: {exc}")


async def poll_loop(
    bot_token: str,
    profiles: dict[str, PromptProfile],
    tavily_api_key: str,
    tz,
    request_lock: asyncio.Lock,
) -> None:
    offset: Optional[int] = None
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
                    await handle_start(bot_token, chat_id, get_prompt_commands(profiles))
                elif command in profiles:
                    await handle_prompt_command(
                        bot_token,
                        chat_id,
                        command,
                        profiles,
                        tavily_api_key,
                        tz,
                        request_lock,
                    )
        except Exception as exc:
            print(f"[polling] {exc}", file=sys.stderr)
            await asyncio.sleep(5)


def load_settings() -> dict[str, Any]:
    env = load_env(ENV_PATH)
    bot_token = env.get("BOT_TOKEN", "")
    tavily_api_key = env.get("TAVILY_API_KEY", "")
    if not bot_token or not tavily_api_key:
        raise RuntimeError("BOT_TOKEN and TAVILY_API_KEY must exist in .env")

    tz = resolve_timezone(env.get("TZ", DEFAULT_TZ))
    return {
        "bot_token": bot_token,
        "tavily_api_key": tavily_api_key,
        "tz": tz,
    }


async def main() -> None:
    settings = load_settings()
    profiles = load_profiles(PROMPTS_DIR)
    if not profiles:
        raise RuntimeError("No supported prompt files found (expected logistics.txt / metanol.txt)")

    db_init(DB_PATH)
    request_lock = asyncio.Lock()

    print(f"[startup] Prompt commands: {', '.join(sorted(profiles.keys()))}")
    await poll_loop(
        settings["bot_token"],
        profiles,
        settings["tavily_api_key"],
        settings["tz"],
        request_lock,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")
