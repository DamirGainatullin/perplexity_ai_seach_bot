import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from core.db import (
    db_add_chat,
    db_delete_cached_response,
    db_get_cached_result,
    db_get_recent_digest_history,
    db_init,
    db_list_due_schedule_chat_ids,
    db_mark_schedule_sent,
    db_prepare_schedule_mode,
    db_reconcile_tavily_credits,
    db_reserve_tavily_credits,
    db_save_digest_history,
    db_save_cached_response,
    db_toggle_schedule_subscription,
)
from core.env import load_env, read_text_with_fallback, resolve_timezone
from core.telegram_api import configure_telegram_proxy, mask_proxy_url, tg_get_updates, tg_send_text
from pipelines.adaptive_followup import (
    DEFAULT_FOLLOWUP_CREDIT_CAP,
    DEFAULT_FOLLOWUP_MAX_QUERIES,
    DEFAULT_FOLLOWUP_MAX_RESULTS,
    merge_digest_rows,
    plan_followup_queries,
    run_followup_queries,
)
from pipelines.daily_engine import DAILY_PROFILE_CREDIT_RESERVATION, run_daily_pipeline
from pipelines.engine import format_digest_response, format_digest_response_html, run_budget_pipeline
from pipelines.models import PromptProfile
from pipelines.openrouter_filter import (
    DEFAULT_OPENROUTER_MODEL,
    configure_openrouter_proxy,
    run_three_stage_openrouter_pipeline,
)
from pipelines.perplexity_seed import (
    is_perplexity_followup_enabled_for_profile,
    load_aggregated_perplexity_summary_for_profile,
)
from pipelines.profiles import load_profiles
from pipelines.telegram_feed import configure_telegram_feed_proxy


BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
PROMPTS_DIR = BASE_DIR / "prompts"
DB_PATH = BASE_DIR / "weekly_bot.sqlite3"
DEFAULT_TZ = "Europe/Moscow"
SCHEDULE_COMMAND = "/schedule"
DEFAULT_DIGEST_MODE = "weekly"
DEFAULT_DAILY_LOOKBACK_DAYS = 2
DEFAULT_DAILY_TAVILY_CREDIT_LIMIT = 28.0
DEFAULT_SCHEDULE_HOUR = 8
DEFAULT_SCHEDULE_MINUTE = 30
DEFAULT_SCHEDULE_POLL_INTERVAL_SEC = 30
DEFAULT_SCHEDULE_PROFILE_ROTATION = (
    "/logistics",
    "/rop",
    "/metanol",
    "/precursors",
    "/chesny_znak",
    "/spot",
    "/sales",
)
CLEAR_TARGET_ALIASES = {
    "logistic": "logistics",
    "logistics": "logistics",
    "chesny": "chesny_znak",
    "chesny_znak": "chesny_znak",
    "chesnyznak": "chesny_znak",
    "honestsign": "chesny_znak",
    "marking": "chesny_znak",
    "markirovka": "chesny_znak",
    "chz": "chesny_znak",
    "metanol": "metanol",
    "methanol": "metanol",
    "precursors": "precursors",
    "prodazhi": "sales",
    "rop": "rop",
    "sales": "sales",
    "sale": "sales",
    "spot": "spot",
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


def _safe_schedule_time(raw: str | None, default_hour: int, default_minute: int) -> tuple[int, int]:
    if not raw:
        return default_hour, default_minute
    value = raw.strip()
    if not value or ":" not in value:
        return default_hour, default_minute
    hour_raw, minute_raw = value.split(":", 1)
    try:
        hour = int(hour_raw)
        minute = int(minute_raw)
    except ValueError:
        return default_hour, default_minute
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return default_hour, default_minute
    return hour, minute


def _parse_schedule_profile_rotation(raw: str | None) -> tuple[str, ...]:
    if not raw or not raw.strip():
        return DEFAULT_SCHEDULE_PROFILE_ROTATION
    commands = tuple(
        value if value.startswith("/") else f"/{value}"
        for value in (part.strip().lower() for part in raw.split(","))
        if value
    )
    if len(commands) != 7 or len(set(commands)) != 7:
        raise RuntimeError("SCHEDULE_PROFILE_ROTATION must contain exactly 7 unique comma-separated commands")
    return commands


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


def _is_schedule_due(
    now: datetime,
    schedule_hour: int,
    schedule_minute: int,
) -> bool:
    return (now.hour, now.minute) >= (schedule_hour, schedule_minute)


def _scheduled_command_for_day(now: datetime, rotation: tuple[str, ...]) -> str:
    return rotation[now.weekday()]


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
    *,
    digest_mode: str = DEFAULT_DIGEST_MODE,
    daily_lookback_days: int = DEFAULT_DAILY_LOOKBACK_DAYS,
    daily_tavily_credit_limit: float = DEFAULT_DAILY_TAVILY_CREDIT_LIMIT,
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
        digest_mode=digest_mode,
        daily_lookback_days=daily_lookback_days,
        daily_tavily_credit_limit=daily_tavily_credit_limit,
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
    digest_mode: str = DEFAULT_DIGEST_MODE,
    daily_lookback_days: int = DEFAULT_DAILY_LOOKBACK_DAYS,
    daily_tavily_credit_limit: float = DEFAULT_DAILY_TAVILY_CREDIT_LIMIT,
    use_history: bool = True,
    persist_history: bool = True,
    use_cache: bool = True,
    persist_cache: bool = True,
) -> tuple[str, dict[str, Any]]:
    prompt_name = profile.name
    normalized_mode = "weekly" if digest_mode == "weekly" else "daily"
    cache_prompt_name = prompt_name if normalized_mode == "weekly" else f"{prompt_name}:daily"
    now_date = datetime.now(tz).date()
    cache_date = now_date.isoformat()
    if use_cache:
        cached = db_get_cached_result(DB_PATH, cache_prompt_name, cache_date)
        if cached is not None:
            cached_response, cached_item_count = cached
            return cached_response, {
                "cache_hit": True,
                "cache_date": cache_date,
                "prompt_name": prompt_name,
                "final_rows_count": cached_item_count,
            }

    prompt_text = read_text_with_fallback(profile.prompt_path)
    if normalized_mode == "daily":
        recent_history = (
            await asyncio.to_thread(
                db_get_recent_digest_history,
                DB_PATH,
                prompt_name,
                limit=40,
            )
            if use_history
            else []
        )
        reserved = await asyncio.to_thread(
            db_reserve_tavily_credits,
            DB_PATH,
            cache_date,
            DAILY_PROFILE_CREDIT_RESERVATION,
            daily_tavily_credit_limit,
        )
        allowance = DAILY_PROFILE_CREDIT_RESERVATION if reserved else 0.0
        rows, usage = await asyncio.to_thread(
            run_daily_pipeline,
            profile,
            prompt_text,
            tavily_api_key,
            openrouter_api_key,
            openrouter_model,
            now_date,
            recent_history,
            lookback_days=daily_lookback_days,
            credit_allowance=allowance,
        )
        if reserved:
            await asyncio.to_thread(
                db_reconcile_tavily_credits,
                DB_PATH,
                cache_date,
                DAILY_PROFILE_CREDIT_RESERVATION,
                float(usage.get("total_credits", 0.0) or 0.0),
            )
        usage["daily_credit_reservation_status"] = "reserved" if reserved else "daily_limit_reached"
        usage["empty_message"] = "За прошедшие сутки новых релевантных материалов не найдено."
        if persist_history and rows:
            await asyncio.to_thread(db_save_digest_history, DB_PATH, prompt_name, rows)
    else:
        rows, usage = await asyncio.to_thread(
            run_budget_pipeline,
            profile,
            prompt_text,
            tavily_api_key,
            now_date,
        )
        if adaptive_followup_enabled and is_perplexity_followup_enabled_for_profile(profile.name):
            seed_summary, seed_meta = await asyncio.to_thread(
                load_aggregated_perplexity_summary_for_profile,
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
        elif adaptive_followup_enabled:
            usage.update(
                {
                    "perplexity_seed_status": "skipped_profile_disabled",
                    "followup_plan_status": "skipped_profile_disabled",
                    "followup_search_status": "skipped_profile_disabled",
                    "followup_rows_after_merge": len(rows),
                }
            )
        else:
            usage.update(
                {
                    "perplexity_seed_status": "skipped_globally_disabled",
                    "followup_plan_status": "skipped_globally_disabled",
                    "followup_search_status": "skipped_globally_disabled",
                    "followup_rows_after_merge": len(rows),
                }
            )

        rows, filter_usage = await asyncio.to_thread(
            run_three_stage_openrouter_pipeline,
            rows,
            prompt_text,
            openrouter_api_key,
            openrouter_model,
        )
        usage.update(filter_usage)

    usage["final_rows_count"] = len(rows)
    usage["digest_mode"] = normalized_mode
    usage["cache_hit"] = False
    usage["cache_date"] = cache_date
    usage["prompt_name"] = prompt_name
    response_text = format_digest_response(profile, rows, usage)
    if persist_cache:
        db_save_cached_response(DB_PATH, cache_prompt_name, cache_date, response_text, len(rows))
    return response_text, usage


async def handle_start(bot_token: str, chat_id: int, commands: dict[str, Path], digest_mode: str) -> None:
    db_add_chat(DB_PATH, chat_id)
    visible_commands = sorted(commands.keys())
    visible_commands.append(SCHEDULE_COMMAND)
    cmd_text = ", ".join(visible_commands) if visible_commands else "(нет доступных команд)"
    period = "ежедневную" if digest_mode == "daily" else "ежедневную ротацию недельных сводок"
    text = f"Бот готов. Авторассылку можно включить командой /schedule ({period}).\nДоступные команды:\n{cmd_text}"
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
    digest_mode: str,
    daily_lookback_days: int,
    daily_tavily_credit_limit: float,
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
                digest_mode=digest_mode,
                daily_lookback_days=daily_lookback_days,
                daily_tavily_credit_limit=daily_tavily_credit_limit,
            )
        await tg_send_text(bot_token, chat_id, format_digest_response_html(result), parse_mode="HTML")
    except Exception as exc:
        await tg_send_text(bot_token, chat_id, f"Ошибка при запросе {command}: {exc}")


async def handle_clear_command(
    bot_token: str,
    chat_id: int,
    command: str,
    profiles: dict[str, PromptProfile],
    tz,
    digest_mode: str,
    request_lock: asyncio.Lock,
) -> None:
    profile = _resolve_clear_profile(profiles, command)
    if profile is None:
        return
    cache_date = datetime.now(tz).date().isoformat()
    cache_prompt_name = profile.name if digest_mode == "weekly" else f"{profile.name}:daily"
    async with request_lock:
        deleted = await asyncio.to_thread(db_delete_cached_response, DB_PATH, cache_prompt_name, cache_date)
    if deleted > 0:
        await tg_send_text(bot_token, chat_id, f"Кэш за {cache_date} для {profile.name} очищен.")
    else:
        await tg_send_text(bot_token, chat_id, f"Кэш за {cache_date} для {profile.name} уже пуст.")


async def handle_schedule_command(
    bot_token: str,
    chat_id: int,
    digest_mode: str,
    tz,
    schedule_hour: int,
    schedule_minute: int,
) -> None:
    db_add_chat(DB_PATH, chat_id)
    now = datetime.now(tz)
    elapsed_slot = (
        now.date().isoformat()
        if _is_schedule_due(now, schedule_hour, schedule_minute)
        else None
    )
    enabled = await asyncio.to_thread(
        db_toggle_schedule_subscription,
        DB_PATH,
        chat_id,
        initial_last_sent_at=elapsed_slot,
    )
    if enabled:
        period = (
            "Ежедневные сводки"
            if digest_mode == "daily"
            else "Одна недельная сводка по направлению"
        )
        await tg_send_text(
            bot_token,
            chat_id,
            f"Авторассылка включена. {period} будет приходить сюда ежедневно по расписанию сервера.",
        )
    else:
        await tg_send_text(
            bot_token,
            chat_id,
            "Авторассылка отключена. Плановые сводки в этот чат больше отправляться не будут.",
        )


async def _build_scheduled_reports(
    profiles: dict[str, PromptProfile],
    scheduled_command: str,
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
    tz,
    digest_mode: str,
    daily_lookback_days: int,
    daily_tavily_credit_limit: float,
    request_lock: asyncio.Lock,
) -> list[tuple[str, str, bool, int]]:
    profile = profiles[scheduled_command]
    try:
        async with request_lock:
            result, usage = await generate_by_profile_with_usage(
                tavily_api_key,
                openrouter_api_key,
                openrouter_model,
                adaptive_followup_enabled,
                followup_credit_cap,
                followup_max_queries,
                followup_max_results,
                profile,
                tz,
                digest_mode=digest_mode,
                daily_lookback_days=daily_lookback_days,
                daily_tavily_credit_limit=daily_tavily_credit_limit,
            )
        return [(scheduled_command, result, True, int(usage.get("final_rows_count", -1)))]
    except Exception as exc:
        return [(scheduled_command, f"Ошибка при плановой сводке {scheduled_command}: {exc}", False, -1)]


async def run_scheduled_broadcast(
    bot_token: str,
    chat_ids: list[int],
    profiles: dict[str, PromptProfile],
    scheduled_command: str,
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    adaptive_followup_enabled: bool,
    followup_credit_cap: float,
    followup_max_queries: int,
    followup_max_results: int,
    tz,
    digest_mode: str,
    daily_lookback_days: int,
    daily_tavily_credit_limit: float,
    request_lock: asyncio.Lock,
    schedule_slot: str,
) -> None:
    if not chat_ids:
        return

    reports = await _build_scheduled_reports(
        profiles,
        scheduled_command,
        tavily_api_key,
        openrouter_api_key,
        openrouter_model,
        adaptive_followup_enabled,
        followup_credit_cap,
        followup_max_queries,
        followup_max_results,
        tz,
        digest_mode,
        daily_lookback_days,
        daily_tavily_credit_limit,
        request_lock,
    )
    nonempty_reports = [report for report in reports if (not report[2]) or report[3] != 0]

    for chat_id in chat_ids:
        try:
            if nonempty_reports:
                await tg_send_text(
                    bot_token,
                    chat_id,
                    f"Плановая рассылка за {schedule_slot}: {scheduled_command}.",
                )
                for _command, text, is_digest, _item_count in nonempty_reports:
                    if is_digest:
                        await tg_send_text(bot_token, chat_id, format_digest_response_html(text), parse_mode="HTML")
                    else:
                        await tg_send_text(bot_token, chat_id, text)
            else:
                empty_text = (
                    f"За прошедшие сутки новых релевантных материалов по направлению {scheduled_command} не найдено."
                    if digest_mode == "daily"
                    else f"За последние семь дней новых релевантных материалов по направлению {scheduled_command} не найдено."
                )
                await tg_send_text(
                    bot_token,
                    chat_id,
                    empty_text,
                )
            await asyncio.to_thread(db_mark_schedule_sent, DB_PATH, chat_id, schedule_slot)
        except Exception as exc:
            print(f"[schedule] failed chat_id={chat_id}: {exc}", file=sys.stderr)


async def schedule_loop(
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
    digest_mode: str,
    daily_lookback_days: int,
    daily_tavily_credit_limit: float,
    schedule_profile_rotation: tuple[str, ...],
    schedule_hour: int,
    schedule_minute: int,
    schedule_poll_interval_sec: int,
    request_lock: asyncio.Lock,
) -> None:
    while True:
        try:
            now = datetime.now(tz)
            if _is_schedule_due(now, schedule_hour, schedule_minute):
                schedule_slot = now.date().isoformat()
                scheduled_command = _scheduled_command_for_day(now, schedule_profile_rotation)
                chat_ids = await asyncio.to_thread(db_list_due_schedule_chat_ids, DB_PATH, schedule_slot)
                if chat_ids:
                    print(
                        f"[schedule] sending {digest_mode} digest {scheduled_command} "
                        f"to {len(chat_ids)} chat(s) for {schedule_slot}"
                    )
                    await run_scheduled_broadcast(
                        bot_token,
                        chat_ids,
                        profiles,
                        scheduled_command,
                        tavily_api_key,
                        openrouter_api_key,
                        openrouter_model,
                        adaptive_followup_enabled,
                        followup_credit_cap,
                        followup_max_queries,
                        followup_max_results,
                        tz,
                        digest_mode,
                        daily_lookback_days,
                        daily_tavily_credit_limit,
                        request_lock,
                        schedule_slot,
                    )
            await asyncio.sleep(schedule_poll_interval_sec)
        except Exception as exc:
            print(f"[schedule] {exc}", file=sys.stderr)
            await asyncio.sleep(schedule_poll_interval_sec)


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
    digest_mode: str,
    daily_lookback_days: int,
    daily_tavily_credit_limit: float,
    schedule_hour: int,
    schedule_minute: int,
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
                    await handle_start(bot_token, chat_id, get_prompt_commands(profiles), digest_mode)
                elif command == SCHEDULE_COMMAND:
                    await handle_schedule_command(
                        bot_token,
                        chat_id,
                        digest_mode,
                        tz,
                        schedule_hour,
                        schedule_minute,
                    )
                elif command.startswith("/clear:"):
                    await handle_clear_command(
                        bot_token,
                        chat_id,
                        command,
                        profiles,
                        tz,
                        digest_mode,
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
                        digest_mode,
                        daily_lookback_days,
                        daily_tavily_credit_limit,
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
    digest_mode = env.get("DIGEST_MODE", DEFAULT_DIGEST_MODE).strip().lower()
    if digest_mode not in {"daily", "weekly"}:
        digest_mode = DEFAULT_DIGEST_MODE
    openrouter_api_key = env.get("OPENROUTER_API_KEY", "").strip()
    if digest_mode == "daily" and not openrouter_api_key:
        raise RuntimeError("OPENROUTER_API_KEY must exist in .env when DIGEST_MODE=daily")
    openrouter_model = env.get("OPENROUTER_MODEL", DEFAULT_OPENROUTER_MODEL).strip() or DEFAULT_OPENROUTER_MODEL
    if openrouter_model != DEFAULT_OPENROUTER_MODEL:
        raise RuntimeError(
            f"Unsupported OPENROUTER_MODEL={openrouter_model!r}; expected {DEFAULT_OPENROUTER_MODEL!r}"
        )
    adaptive_followup_enabled = _safe_bool(env.get("ADAPTIVE_FOLLOWUP_ENABLED"), True)
    followup_credit_cap = max(0.0, _safe_float(env.get("FOLLOWUP_CREDIT_CAP"), DEFAULT_FOLLOWUP_CREDIT_CAP))
    followup_max_queries = max(1, _safe_int(env.get("FOLLOWUP_MAX_QUERIES"), DEFAULT_FOLLOWUP_MAX_QUERIES))
    followup_max_results = max(1, _safe_int(env.get("FOLLOWUP_MAX_RESULTS"), DEFAULT_FOLLOWUP_MAX_RESULTS))
    telegram_proxy_url = env.get("TELEGRAM_PROXY_URL", "").strip()
    openrouter_proxy_url = env.get("OPENROUTER_PROXY_URL", "").strip() or telegram_proxy_url
    schedule_profile_rotation = _parse_schedule_profile_rotation(env.get("SCHEDULE_PROFILE_ROTATION"))
    schedule_hour, schedule_minute = _safe_schedule_time(
        env.get("SCHEDULE_TIME"),
        DEFAULT_SCHEDULE_HOUR,
        DEFAULT_SCHEDULE_MINUTE,
    )
    schedule_poll_interval_sec = max(
        5,
        _safe_int(env.get("SCHEDULE_POLL_INTERVAL_SEC"), DEFAULT_SCHEDULE_POLL_INTERVAL_SEC),
    )
    daily_lookback_days = max(
        1,
        min(3, _safe_int(env.get("DAILY_LOOKBACK_DAYS"), DEFAULT_DAILY_LOOKBACK_DAYS)),
    )
    daily_tavily_credit_limit = max(
        DAILY_PROFILE_CREDIT_RESERVATION,
        _safe_float(env.get("DAILY_TAVILY_CREDIT_LIMIT"), DEFAULT_DAILY_TAVILY_CREDIT_LIMIT),
    )

    tz = resolve_timezone(env.get("TZ", DEFAULT_TZ))
    return {
        "bot_token": bot_token,
        "digest_mode": digest_mode,
        "tavily_api_key": tavily_api_key,
        "openrouter_api_key": openrouter_api_key,
        "openrouter_model": openrouter_model,
        "adaptive_followup_enabled": adaptive_followup_enabled,
        "followup_credit_cap": followup_credit_cap,
        "followup_max_queries": followup_max_queries,
        "followup_max_results": followup_max_results,
        "telegram_proxy_url": telegram_proxy_url,
        "openrouter_proxy_url": openrouter_proxy_url,
        "schedule_profile_rotation": schedule_profile_rotation,
        "schedule_hour": schedule_hour,
        "schedule_minute": schedule_minute,
        "schedule_poll_interval_sec": schedule_poll_interval_sec,
        "daily_lookback_days": daily_lookback_days,
        "daily_tavily_credit_limit": daily_tavily_credit_limit,
        "tz": tz,
    }


async def main() -> None:
    settings = load_settings()
    profiles = load_profiles(PROMPTS_DIR)
    if not profiles:
        raise RuntimeError("No supported prompt files found in prompts directory.")
    missing_scheduled_commands = [
        command for command in settings["schedule_profile_rotation"] if command not in profiles
    ]
    if missing_scheduled_commands:
        raise RuntimeError(
            "Schedule rotation references unavailable profiles: " + ", ".join(missing_scheduled_commands)
        )

    configure_telegram_proxy(settings["telegram_proxy_url"])
    configure_telegram_feed_proxy(settings["telegram_proxy_url"])
    configure_openrouter_proxy(settings["openrouter_proxy_url"])
    db_init(DB_PATH)
    startup_now = datetime.now(settings["tz"])
    elapsed_slot = (
        startup_now.date().isoformat()
        if _is_schedule_due(
            startup_now,
            settings["schedule_hour"],
            settings["schedule_minute"],
        )
        else None
    )
    previous_mode, skipped_chats = db_prepare_schedule_mode(
        DB_PATH,
        settings["digest_mode"],
        elapsed_schedule_slot=elapsed_slot,
    )
    request_lock = asyncio.Lock()

    print(f"[startup] Prompt commands: {', '.join(sorted(profiles.keys()))}")
    print(
        "[startup] Digest mode: "
        f"{settings['digest_mode']}, daily_lookback_days={settings['daily_lookback_days']}, "
        f"daily_tavily_credit_limit={settings['daily_tavily_credit_limit']}"
    )
    if previous_mode != settings["digest_mode"]:
        print(
            "[startup] Digest mode transition: "
            f"{previous_mode or 'uninitialized'} -> {settings['digest_mode']}, "
            f"current_slot_skipped_for_chats={skipped_chats}"
        )
    print(
        "[startup] Telegram proxy: "
        + (mask_proxy_url(settings["telegram_proxy_url"]) if settings["telegram_proxy_url"] else "disabled")
    )
    print(
        "[startup] OpenRouter proxy: "
        + (mask_proxy_url(settings["openrouter_proxy_url"]) if settings["openrouter_proxy_url"] else "disabled")
    )
    print(
        "[startup] OpenRouter filter: "
        + ("enabled" if settings["openrouter_api_key"] else "disabled (OPENROUTER_API_KEY missing)")
    )
    print(
        "[startup] Weekly Perplexity follow-up: "
        f"{'enabled' if settings['adaptive_followup_enabled'] else 'disabled'}, "
        f"credit_cap={settings['followup_credit_cap']}, "
        f"max_queries={settings['followup_max_queries']}, "
        f"max_results={settings['followup_max_results']}"
    )
    print(
        "[startup] Schedule: "
        f"mode={settings['digest_mode']}, enabled via {SCHEDULE_COMMAND}, "
        f"time={settings['schedule_hour']:02d}:{settings['schedule_minute']:02d}, "
        f"poll_interval={settings['schedule_poll_interval_sec']}s, "
        f"rotation={','.join(settings['schedule_profile_rotation'])}"
    )

    schedule_task = asyncio.create_task(
        schedule_loop(
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
            settings["digest_mode"],
            settings["daily_lookback_days"],
            settings["daily_tavily_credit_limit"],
            settings["schedule_profile_rotation"],
            settings["schedule_hour"],
            settings["schedule_minute"],
            settings["schedule_poll_interval_sec"],
            request_lock,
        )
    )
    try:
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
            settings["digest_mode"],
            settings["daily_lookback_days"],
            settings["daily_tavily_credit_limit"],
            settings["schedule_hour"],
            settings["schedule_minute"],
            request_lock,
        )
    finally:
        schedule_task.cancel()
        try:
            await schedule_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped")


# For local tests - python run_local_profile.py --command /logistics --no-cache 
