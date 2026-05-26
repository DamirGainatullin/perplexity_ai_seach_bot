import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from core.db import db_add_chat, db_delete_cached_response, db_get_cached_response, db_init, db_save_cached_response
from core.env import load_env, read_text_with_fallback, resolve_timezone
from core.telegram_api import tg_get_updates, tg_send_text
from pipelines.adaptive_followup import (
    DEFAULT_FOLLOWUP_CREDIT_CAP,
    DEFAULT_FOLLOWUP_MAX_QUERIES,
    DEFAULT_FOLLOWUP_MAX_RESULTS,
    merge_digest_rows,
    plan_followup_queries,
    run_followup_queries,
)
from pipelines.engine import format_digest_response, run_budget_pipeline
from pipelines.models import PromptProfile
from pipelines.openrouter_filter import DEFAULT_OPENROUTER_MODEL, run_three_stage_openrouter_pipeline
from pipelines.perplexity_seed import load_latest_perplexity_summary_for_profile
from pipelines.profiles import load_profiles


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
PROMPTS_DIR = BASE_DIR / "prompts"
DB_PATH = BASE_DIR / "weekly_bot.sqlite3"
DEFAULT_TZ = "Europe/Moscow"
CLEAR_TARGET_ALIASES = {
    "logistic": "logistics",
    "logistics": "logistics",
    "metanol": "metanol",
    "methanol": "metanol",
    "precursors": "precursors",
    "rop": "rop",
}


def _safe_bool(raw: str | None, default: bool) -> bool:
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _safe_int(raw: str | None, default: int) -> int:
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _safe_float(raw: str | None, default: float) -> float:
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def get_prompt_commands(profiles: dict[str, PromptProfile]) -> dict[str, Path]:
    return {command: profile.prompt_path for command, profile in profiles.items()}


def _resolve_clear_profile(profiles: dict[str, PromptProfile], command: str) -> PromptProfile | None:
    if not command.startswith("/clear:"):
        return None
    raw_target = command.split(":", 1)[1].strip().strip(",; ")
    if not raw_target:
        return None
    normalized = CLEAR_TARGET_ALIASES.get(raw_target.lower(), raw_target.lower())
    for profile in profiles.values():
        if profile.name.lower() == normalized:
            return profile
    return None


async def generate_by_profile(
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
    profile: PromptProfile,
    tz,
) -> str:
    response_text, _usage = await generate_by_profile_with_usage(
        tavily_api_key,
        openrouter_api_key,
        openrouter_model,
        adaptive_followup_enabled,
        followup_credit_cap,
        followup_max_queries,
        followup_max_results,
        profile,
        tz,
        use_cache=True,
        persist_cache=True,
    )
    return response_text


async def generate_by_profile_with_usage(
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
    profile: PromptProfile,
    tz,
    *,
    use_cache: bool = True,
    persist_cache: bool = True,
) -> tuple[str, dict[str, Any]]:
    prompt_name = profile.name
    now_date = datetime.now(tz).date()
    cache_date = now_date.isoformat()
    if use_cache:
        cached = db_get_cached_response(DB_PATH, prompt_name, cache_date)
        if cached is not None:
            return cached, {
                "cache_hit": True,
                "cache_date": cache_date,
                "prompt_name": prompt_name,
            }

    prompt_text = read_text_with_fallback(profile.prompt_path)
    rows, usage = await asyncio.to_thread(
        run_budget_pipeline,
        profile,
        prompt_text,
        tavily_api_key,
        now_date,
    )

    if adaptive_followup_enabled:
        seed_summary, seed_meta = await asyncio.to_thread(
            load_latest_perplexity_summary_for_profile,
            profile.name,
            PROMPTS_DIR,
            BASE_DIR,
        )
        usage.update(seed_meta)

        queries, plan_meta = await asyncio.to_thread(
            plan_followup_queries,
            prompt_text,
            seed_summary,
            rows,
            openrouter_api_key,
            openrouter_model,
            max(1, followup_max_queries),
        )
        usage.update(plan_meta)

        extra_rows, followup_meta = await asyncio.to_thread(
            run_followup_queries,
            profile,
            tavily_api_key,
            now_date,
            queries,
            max(0.0, followup_credit_cap),
            max(1, followup_max_results),
        )
        usage.update(followup_meta)
        usage["total_credits"] = float(usage.get("total_credits", 0.0)) + float(
            followup_meta.get("followup_search_credits", 0.0)
        )
        if extra_rows:
            rows = merge_digest_rows(rows, extra_rows, limit=30)
        usage["followup_rows_after_merge"] = len(rows)

    rows, filter_usage = await asyncio.to_thread(
        run_three_stage_openrouter_pipeline,
        rows,
        prompt_text,
        openrouter_api_key,
        openrouter_model,
    )
    usage.update(filter_usage)
    usage["final_rows_count"] = len(rows)
    usage["cache_hit"] = False
    usage["cache_date"] = cache_date
    usage["prompt_name"] = prompt_name
    response_text = format_digest_response(profile, rows, usage)
    if persist_cache:
        db_save_cached_response(DB_PATH, prompt_name, cache_date, response_text)
    return response_text, usage


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
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
    tz,
    request_lock: asyncio.Lock,
) -> None:
    profile = profiles.get(command)
    if profile is None:
        return

    await tg_send_text(bot_token, chat_id, f"Собираю сводку для команды {command}...")
    try:
        async with request_lock:
            result = await generate_by_profile(
                tavily_api_key,
                openrouter_api_key,
                openrouter_model,
                adaptive_followup_enabled,
                followup_credit_cap,
                followup_max_queries,
                followup_max_results,
                profile,
                tz,
            )
        await tg_send_text(bot_token, chat_id, result)
    except Exception as exc:
        await tg_send_text(bot_token, chat_id, f"Ошибка при запросе {command}: {exc}")


async def handle_clear_command(
    bot_token: str,
    chat_id: int,
    command: str,
    profiles: dict[str, PromptProfile],
    tz,
    request_lock: asyncio.Lock,
) -> None:
    profile = _resolve_clear_profile(profiles, command)
    if profile is None:
        return
    cache_date = datetime.now(tz).date().isoformat()
    async with request_lock:
        deleted = await asyncio.to_thread(db_delete_cached_response, DB_PATH, profile.name, cache_date)
    if deleted > 0:
        await tg_send_text(bot_token, chat_id, f"Кэш за {cache_date} для {profile.name} очищен.")
    else:
        await tg_send_text(bot_token, chat_id, f"Кэш за {cache_date} для {profile.name} уже пуст.")


async def poll_loop(
    bot_token: str,
    profiles: dict[str, PromptProfile],
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
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
                elif command.startswith("/clear:"):
                    await handle_clear_command(
                        bot_token,
                        chat_id,
                        command,
                        profiles,
                        tz,
                        request_lock,
                    )
                elif command in profiles:
                    await handle_prompt_command(
                        bot_token,
                        chat_id,
                        command,
                        profiles,
                        tavily_api_key,
                        openrouter_api_key,
                        openrouter_model,
                        adaptive_followup_enabled,
                        followup_credit_cap,
                        followup_max_queries,
                        followup_max_results,
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
    openrouter_api_key = env.get("OPENROUTER_API_KEY", "").strip()
    openrouter_model = env.get("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL).strip() or DEFAULT_OPENROUTER_MODEL
    adaptive_followup_enabled = _safe_bool(env.get("ADAPTIVE_FOLLOWUP_ENABLED"), True)
    followup_credit_cap = max(0.0, _safe_float(env.get("FOLLOWUP_CREDIT_CAP"), DEFAULT_FOLLOWUP_CREDIT_CAP))
    followup_max_queries = max(1, _safe_int(env.get("FOLLOWUP_MAX_QUERIES"), DEFAULT_FOLLOWUP_MAX_QUERIES))
    followup_max_results = max(1, _safe_int(env.get("FOLLOWUP_MAX_RESULTS"), DEFAULT_FOLLOWUP_MAX_RESULTS))

    tz = resolve_timezone(env.get("TZ", DEFAULT_TZ))
    return {
        "bot_token": bot_token,
        "tavily_api_key": tavily_api_key,
        "openrouter_api_key": openrouter_api_key,
        "openrouter_model": openrouter_model,
        "adaptive_followup_enabled": adaptive_followup_enabled,
        "followup_credit_cap": followup_credit_cap,
        "followup_max_queries": followup_max_queries,
        "followup_max_results": followup_max_results,
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
    print(
        "[startup] OpenRouter filter: "
        + ("enabled" if settings["openrouter_api_key"] else "disabled (OPENROUTER_API_KEY missing)")
    )
    print(
        "[startup] Adaptive follow-up: "
        f"{'enabled' if settings['adaptive_followup_enabled'] else 'disabled'}, "
        f"credit_cap={settings['followup_credit_cap']}, "
        f"max_queries={settings['followup_max_queries']}, "
        f"max_results={settings['followup_max_results']}"
    )
    await poll_loop(
        settings["bot_token"],
        profiles,
        settings["tavily_api_key"],
        settings["openrouter_api_key"],
        settings["openrouter_model"],
        settings["adaptive_followup_enabled"],
        settings["followup_credit_cap"],
        settings["followup_max_queries"],
        settings["followup_max_results"],
        settings["tz"],
        request_lock,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")


# For local tests - python run_local_profile.py --command /logistics --no-cache 
