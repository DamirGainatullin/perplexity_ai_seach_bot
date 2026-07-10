import argparse
import asyncio
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

from core.db import db_init
from manage import DB_PATH, PROMPTS_DIR, generate_by_profile_with_usage, load_settings
from pipelines.profiles import load_profiles


BASE_DIR = Path(__file__).resolve().parent
DEBUG_DIR = BASE_DIR / "debug_reports"


def clear_today_cache(profile_name: str, cache_date: str, digest_mode: str) -> None:
    cache_prompt_name = profile_name if digest_mode == "weekly" else f"{profile_name}:daily"
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            DELETE FROM prompt_cache
            WHERE prompt_name = ? AND cache_date = ?
            """,
            (cache_prompt_name, cache_date),
        )


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run a profile pipeline locally without Telegram polling.")
    parser.add_argument(
        "--command",
        default="/logistics",
        help="Profile command: /logistics /metanol /precursors /rop /chesny_znak /spot /sales",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore today's cached response and force fresh generation.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional path to save output text.",
    )
    parser.add_argument(
        "--ignore-history",
        action="store_true",
        help="Do not remove items seen in earlier daily digests and do not write them to history.",
    )
    args = parser.parse_args()

    settings = load_settings()
    profiles = load_profiles(PROMPTS_DIR)
    db_init(DB_PATH)
    profile = profiles.get(args.command)
    if profile is None:
        available = ", ".join(sorted(profiles.keys()))
        raise RuntimeError(f"Unknown command/profile: {args.command}. Available: {available}")

    cache_date = datetime.now(settings["tz"]).date().isoformat()
    if args.no_cache:
        clear_today_cache(profile.name, cache_date, settings["digest_mode"])

    result, usage = await generate_by_profile_with_usage(
        settings["tavily_api_key"],
        settings["openrouter_api_key"],
        settings["openrouter_model"],
        settings["adaptive_followup_enabled"],
        settings["followup_credit_cap"],
        settings["followup_max_queries"],
        settings["followup_max_results"],
        profile,
        settings["tz"],
        digest_mode=settings["digest_mode"],
        daily_lookback_days=settings["daily_lookback_days"],
        daily_tavily_credit_limit=settings["daily_tavily_credit_limit"],
        use_history=not args.ignore_history,
        persist_history=not args.ignore_history,
        use_cache=not args.no_cache,
        persist_cache=True,
    )

    if args.no_cache:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        report_path = DEBUG_DIR / (
            f"local_pipeline_{profile.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        report = {
            "meta": {
                "generated_at": datetime.now().isoformat(timespec="seconds"),
                "profile": profile.name,
                "command": args.command,
                "cache_date": cache_date,
                "no_cache": True,
            },
            "settings": {
                "digest_mode": settings["digest_mode"],
                "daily_lookback_days": settings["daily_lookback_days"],
                "daily_tavily_credit_limit": settings["daily_tavily_credit_limit"],
                "ignore_history": args.ignore_history,
                "adaptive_followup_enabled": settings["adaptive_followup_enabled"],
                "followup_credit_cap": settings["followup_credit_cap"],
                "followup_max_queries": settings["followup_max_queries"],
                "followup_max_results": settings["followup_max_results"],
                "openrouter_model": settings["openrouter_model"],
            },
            "usage": usage,
            "result_text": result,
        }
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[debug] report saved: {report_path}")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(result)
        print(f"[done] saved to: {args.output}")
    else:
        try:
            print(result)
        except UnicodeEncodeError:
            sys.stdout.buffer.write((result + "\n").encode("utf-8", errors="replace"))


if __name__ == "__main__":
    asyncio.run(main())
