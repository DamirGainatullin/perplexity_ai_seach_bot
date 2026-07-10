from __future__ import annotations

import re
import time
from datetime import date, timedelta
from typing import Any, Optional

from requests import RequestException
from tavily import TavilyClient

from pipelines.helpers import (
    build_include_domains,
    build_summary,
    extract_urls,
    infer_category,
    parse_date_candidates,
    parse_date_from_url,
    parse_published_iso,
    short_query,
)
from pipelines.models import PromptProfile
from pipelines.openrouter_filter import (
    run_daily_stage1_openrouter,
    run_daily_stage2_stage3_openrouter,
)
from pipelines.telegram_feed import extract_telegram_channels, parse_recent_open_channel_posts


DAILY_PROFILE_CREDIT_RESERVATION = 4.0
DAILY_BASIC_MAX_RESULTS = 10
DAILY_ADVANCED_MAX_RESULTS = 8
DAILY_EXTRACT_URL_LIMIT = 5
DAILY_WEB_CANDIDATE_LIMIT = DAILY_BASIC_MAX_RESULTS + DAILY_ADVANCED_MAX_RESULTS
DAILY_TELEGRAM_CANDIDATE_LIMIT = 30
DAILY_TELEGRAM_POSTS_PER_CHANNEL = 12


def _search_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.search(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily daily search failed after retries: {last_error}") from last_error


def _extract_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.extract(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily daily extract failed after retries: {last_error}") from last_error


def build_daily_probe_query(profile: PromptProfile, now_date: date) -> str:
    base = profile.query_plan[0].query if profile.query_plan else profile.report_label
    base = re.sub(r"(?:за\s+)?(?:последние\s+)?7\s+дн(?:ей|я)", "за последние сутки", base, flags=re.I)
    base = " ".join(base.split())
    return short_query(
        f"{base}. Свежие публикации {now_date.year}, официальные документы, "
        "разъяснения ведомств и новая судебная практика Российской Федерации"
    )


def _build_telegram_title(channel: str, text: str, post_id: int) -> str:
    preview = " ".join((text or "").split())
    if not preview:
        return f"Telegram @{channel}, публикация {post_id}"
    if len(preview) <= 90:
        return f"Telegram @{channel}: {preview}"
    return f"Telegram @{channel}: {preview[:87]}..."


def _result_debug(response: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "title": str(item.get("title", "")),
            "url": str(item.get("url", "")),
            "published_date": str(item.get("published_date", "")),
            "score": float(item.get("score", 0.0) or 0.0),
            "content": " ".join(str(item.get("content", "")).split())[:1500],
        }
        for item in (response.get("results", []) or [])
    ]


def _reliable_date(item: dict[str, Any]) -> tuple[str, str]:
    published = parse_published_iso(str(item.get("published_date", "")))
    if published:
        return published, "tavily_published_date"
    url_date = parse_date_from_url(str(item.get("url", "")))
    if url_date:
        return url_date.isoformat(), "url"
    return "", "unknown"


def _labeled_publication_date(text: str) -> str:
    normalized = " ".join((text or "").split())
    low = normalized.lower()
    markers = (
        "дата публикации",
        "опубликовано",
        "опубликован",
        "размещено",
        "published",
        "publication date",
    )
    for marker in markers:
        start = 0
        while True:
            idx = low.find(marker, start)
            if idx < 0:
                break
            tail = normalized[idx + len(marker) : idx + len(marker) + 90]
            if re.search(r"\b(?:вступ|действ|примен)", tail.lower()):
                start = idx + len(marker)
                continue
            candidates = parse_date_candidates(tail)
            if candidates:
                return candidates[0].isoformat()
            start = idx + len(marker)
    return ""


def _is_outside_window(value: str, start_date: date, end_date: date) -> bool:
    if not value:
        return False
    try:
        resolved = date.fromisoformat(value)
    except ValueError:
        return False
    return resolved < start_date or resolved > end_date


def _web_item_to_row(item: dict[str, Any], profile: PromptProfile, source: str) -> dict[str, str]:
    title = str(item.get("title", "")).strip() or "(без заголовка)"
    content = str(item.get("content", "")).strip()
    effective_date, date_confidence = _reliable_date(item)
    score = float(item.get("score", 0.0) or 0.0)
    return {
        "category": infer_category(f"{title} {content}", profile),
        "title": title,
        "summary": build_summary(content, title),
        "content": content,
        "url": str(item.get("url", "")).strip(),
        "date": effective_date,
        "date_confidence": date_confidence,
        "source": source,
        "rank_score": f"{score:.6f}",
    }


def _merge_rows_by_url(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for row in rows:
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        previous = merged.get(url)
        if previous is None or float(row.get("rank_score", 0) or 0) > float(previous.get("rank_score", 0) or 0):
            merged[url] = row
    return list(merged.values())


def _collect_telegram_rows(
    profile: PromptProfile,
    prompt_text: str,
    start_date: date,
    end_date: date,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    include_domains = build_include_domains(extract_urls(prompt_text))
    channels = extract_telegram_channels(include_domains)
    rows: list[dict[str, str]] = []
    errors: list[str] = []
    for channel in channels:
        posts, error = parse_recent_open_channel_posts(
            channel,
            start_date=start_date,
            end_date=end_date,
            max_posts=DAILY_TELEGRAM_POSTS_PER_CHANNEL,
        )
        if error:
            errors.append(f"{channel}: {error}")
            continue
        for post in posts:
            title = _build_telegram_title(post.channel, post.text, post.post_id)
            rows.append(
                {
                    "category": infer_category(f"{title} {post.text}", profile),
                    "title": title,
                    "summary": build_summary(post.text, title),
                    "content": post.text,
                    "url": post.url,
                    "date": post.published_iso,
                    "date_confidence": "telegram_datetime",
                    "source": "telegram",
                    "rank_score": "0.500000",
                }
            )
    rows.sort(key=lambda row: str(row.get("date", "")), reverse=True)
    return rows[:DAILY_TELEGRAM_CANDIDATE_LIMIT], {
        "telegram_channels_total": len(channels),
        "telegram_channels_ok": len(channels) - len(errors),
        "telegram_errors_count": len(errors),
        "telegram_errors": errors,
        "telegram_posts_collected": len(rows),
        "telegram_candidates_used": min(len(rows), DAILY_TELEGRAM_CANDIDATE_LIMIT),
    }


def run_daily_pipeline(
    profile: PromptProfile,
    prompt_text: str,
    tavily_api_key: str,
    openrouter_api_key: str,
    openrouter_model: str,
    now_date: date,
    recent_history: list[dict[str, str]],
    *,
    lookback_days: int = 2,
    credit_allowance: float = DAILY_PROFILE_CREDIT_RESERVATION,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    window_days = max(1, int(lookback_days))
    start_d = now_date - timedelta(days=window_days - 1)
    end_d = now_date
    history_urls = {str(item.get("url", "")).strip() for item in recent_history if item.get("url")}
    usage: dict[str, Any] = {
        "pipeline_mode": "daily",
        "start_date": start_d.isoformat(),
        "end_date": end_d.isoformat(),
        "lookback_days": window_days,
        "credit_allowance": float(credit_allowance),
        "basic_search_credits": 0.0,
        "advanced_search_credits": 0.0,
        "extract_credits": 0.0,
        "total_credits": 0.0,
        "history_items_loaded": len(recent_history),
    }

    telegram_rows, telegram_usage = _collect_telegram_rows(profile, prompt_text, start_d, end_d)
    usage.update(telegram_usage)
    telegram_rows = [row for row in telegram_rows if row.get("url") not in history_urls]

    client = TavilyClient(tavily_api_key)
    consumed_budget = 0.0
    probe_rows: list[dict[str, str]] = []
    if credit_allowance >= 1.0:
        probe_query = build_daily_probe_query(profile, now_date)
        probe_payload = {
            "query": probe_query,
            "topic": "general",
            "search_depth": "basic",
            "country": "russia",
            "start_date": start_d.isoformat(),
            "include_answer": False,
            "include_domains": None,
            "include_usage": True,
            "max_results": DAILY_BASIC_MAX_RESULTS,
        }
        probe_response = _search_with_retry(client, probe_payload)
        probe_actual = float((probe_response.get("usage") or {}).get("credits", 0) or 0)
        probe_charge = max(1.0, probe_actual)
        consumed_budget += probe_charge
        usage["basic_search_credits"] = probe_charge
        usage["daily_probe"] = {
            "payload": probe_payload,
            "api_credits": probe_actual,
            "results_count": len(probe_response.get("results", []) or []),
            "results": _result_debug(probe_response),
        }
        for item in probe_response.get("results", []) or []:
            row = _web_item_to_row(dict(item), profile, "tavily_basic")
            if not row["url"] or row["url"] in history_urls:
                continue
            if _is_outside_window(row["date"], start_d, end_d):
                continue
            probe_rows.append(row)
    else:
        usage["daily_probe"] = {"status": "skipped_credit_limit"}

    probe_rows.sort(key=lambda row: float(row.get("rank_score", 0) or 0), reverse=True)
    initial_rows = _merge_rows_by_url(probe_rows[:DAILY_BASIC_MAX_RESULTS] + telegram_rows)
    stage1_rows, followup_query, stage1_usage = run_daily_stage1_openrouter(
        initial_rows,
        prompt_text,
        recent_history,
        openrouter_api_key,
        openrouter_model,
    )
    usage.update(stage1_usage)
    usage["daily_stage1_input_rows"] = initial_rows
    if initial_rows and stage1_usage.get("openrouter_stage1_status") != "ok":
        raise RuntimeError(f"OpenRouter daily stage 1 failed: {stage1_usage.get('openrouter_stage1_status')}")

    advanced_rows: list[dict[str, str]] = []
    if followup_query and consumed_budget + 2.0 <= float(credit_allowance) + 1e-9:
        advanced_payload = {
            "query": short_query(followup_query),
            "topic": "general",
            "search_depth": "advanced",
            "country": "russia",
            "start_date": start_d.isoformat(),
            "include_answer": False,
            "include_domains": None,
            "include_usage": True,
            "max_results": DAILY_ADVANCED_MAX_RESULTS,
        }
        advanced_response = _search_with_retry(client, advanced_payload)
        advanced_actual = float((advanced_response.get("usage") or {}).get("credits", 0) or 0)
        advanced_charge = max(2.0, advanced_actual)
        consumed_budget += advanced_charge
        usage["advanced_search_credits"] = advanced_charge
        usage["daily_followup"] = {
            "status": "ok",
            "payload": advanced_payload,
            "api_credits": advanced_actual,
            "results_count": len(advanced_response.get("results", []) or []),
            "results": _result_debug(advanced_response),
        }
        for item in advanced_response.get("results", []) or []:
            row = _web_item_to_row(dict(item), profile, "tavily_advanced")
            if not row["url"] or row["url"] in history_urls:
                continue
            if _is_outside_window(row["date"], start_d, end_d):
                continue
            advanced_rows.append(row)
    else:
        usage["daily_followup"] = {
            "status": "skipped_no_query" if not followup_query else "skipped_credit_limit",
            "query": followup_query,
        }

    combined_rows = _merge_rows_by_url(stage1_rows + advanced_rows[:DAILY_ADVANCED_MAX_RESULTS])
    extract_candidates = [
        row
        for row in combined_rows
        if row.get("source", "").startswith("tavily") and not row.get("date") and row.get("url")
    ][:DAILY_EXTRACT_URL_LIMIT]
    extracted_by_url: dict[str, dict[str, Any]] = {}
    if extract_candidates and consumed_budget + 1.0 <= float(credit_allowance) + 1e-9:
        extract_payload = {
            "urls": [row["url"] for row in extract_candidates],
            "extract_depth": "basic",
            "format": "text",
            "include_usage": True,
        }
        extract_response = _extract_with_retry(client, extract_payload)
        extract_actual = float((extract_response.get("usage") or {}).get("credits", 0) or 0)
        extract_charge = max(1.0, extract_actual)
        consumed_budget += extract_charge
        usage["extract_credits"] = extract_charge
        for entry in extract_response.get("results", []) or []:
            url = str(entry.get("url", "")).strip()
            if url:
                extracted_by_url[url] = entry
        usage["daily_extract"] = {
            "status": "ok",
            "payload": extract_payload,
            "api_credits": extract_actual,
            "results_count": len(extracted_by_url),
            "results": [
                {
                    "url": url,
                    "content": " ".join(
                        str(entry.get("raw_content", "") or entry.get("content", "")).split()
                    )[:3000],
                }
                for url, entry in extracted_by_url.items()
            ],
            "failed_results": extract_response.get("failed_results", []) or [],
        }
    else:
        usage["daily_extract"] = {
            "status": "skipped_no_candidates" if not extract_candidates else "skipped_credit_limit",
            "urls": [row["url"] for row in extract_candidates],
        }

    dated_rows: list[dict[str, str]] = []
    date_rejections: list[dict[str, str]] = []
    for row in combined_rows[:DAILY_WEB_CANDIDATE_LIMIT + DAILY_TELEGRAM_CANDIDATE_LIMIT]:
        current = dict(row)
        extracted = extracted_by_url.get(current.get("url", ""))
        if extracted:
            raw = str(extracted.get("raw_content", "") or extracted.get("content", ""))
            if raw:
                current["content"] = raw[:6000]
                current["summary"] = build_summary(raw, current.get("title", ""))
                labeled_date = _labeled_publication_date(raw[:3000])
                if labeled_date:
                    current["date"] = labeled_date
                    current["date_confidence"] = "extract_labeled_publication_date"
        if _is_outside_window(current.get("date", ""), start_d, end_d):
            date_rejections.append(
                {
                    "url": current.get("url", ""),
                    "date": current.get("date", ""),
                    "date_confidence": current.get("date_confidence", ""),
                }
            )
            continue
        dated_rows.append(current)

    final_rows, final_usage = run_daily_stage2_stage3_openrouter(
        dated_rows,
        prompt_text,
        recent_history,
        openrouter_api_key,
        openrouter_model,
    )
    usage.update(final_usage)
    if dated_rows and final_usage.get("openrouter_stage3_status") != "ok":
        raise RuntimeError(f"OpenRouter daily stage 3 failed: {final_usage.get('openrouter_stage3_status')}")
    usage["daily_date_rejections"] = date_rejections
    usage["daily_rows_before_stage2"] = dated_rows
    usage["final_rows"] = final_rows
    usage["final_rows_count"] = len(final_rows)
    usage["final_web_rows"] = sum(1 for row in final_rows if row.get("source", "").startswith("tavily"))
    usage["final_telegram_rows"] = sum(1 for row in final_rows if row.get("source") == "telegram")
    usage["total_credits"] = consumed_budget
    return final_rows, usage
