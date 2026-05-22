import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional

from requests import RequestException
from tavily import TavilyClient

from pipelines.helpers import (
    build_include_domains,
    build_summary,
    extract_urls,
    group_domains,
    infer_category,
    is_topic_related,
    legal_score,
    parse_date_candidates,
    parse_date_from_url,
    parse_published_iso,
    resolve_domain_scope,
    short_query,
)
from pipelines.models import PromptProfile, QueryRule
from pipelines.telegram_feed import extract_telegram_channels, parse_recent_open_channel_posts


@dataclass
class SearchRun:
    rule: QueryRule
    include_domains: list[str]
    response: dict[str, Any]


def _build_telegram_title(channel: str, text: str, post_id: int) -> str:
    preview = " ".join((text or "").split())
    if not preview:
        return f"Telegram @{channel} post {post_id}"
    if len(preview) <= 90:
        return f"Telegram @{channel}: {preview}"
    return f"Telegram @{channel}: {preview[:87]}..."


def _search_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.search(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily search failed after retries: {last_error}") from last_error


def _extract_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.extract(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily extract failed after retries: {last_error}") from last_error


def run_budget_pipeline(
    profile: PromptProfile,
    prompt_text: str,
    tavily_api_key: str,
    now_date: date,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    # Stage 1: extract source URLs from prompt and map them into domain groups.
    source_urls = extract_urls(prompt_text)
    include_domains = build_include_domains(source_urls)
    groups = group_domains(include_domains, profile)

    start_date = (now_date - timedelta(days=7)).isoformat()
    end_date = now_date.isoformat()

    client = TavilyClient(tavily_api_key)
    plan = profile.query_plan[: profile.budget.search_run_limit]
    runs: list[SearchRun] = []
    search_credits = 0.0

    # Stage 2: run limited Tavily search calls according to prompt profile query plan.
    for rule in plan:
        scoped_domains = resolve_domain_scope(rule.domain_scope, groups)
        response = _search_with_retry(
            client,
            {
                "query": short_query(rule.query),
                "topic": rule.topic,
                "search_depth": "advanced",
                "start_date": start_date,
                "end_date": end_date,
                "include_answer": "basic",
                "include_domains": scoped_domains or None,
                "include_usage": True,
                "max_results": profile.budget.max_results,
            },
        )
        search_credits += float((response.get("usage") or {}).get("credits", 0))
        runs.append(SearchRun(rule=rule, include_domains=scoped_domains, response=response))

    # Stage 3: merge results by URL and keep best-scored candidate.
    merged_by_url: dict[str, dict[str, Any]] = {}
    for run in runs:
        for item in run.response.get("results", []) or []:
            url = item.get("url") or ""
            if not url:
                continue
            text = f"{item.get('title', '')} {item.get('content', '')}"
            if not is_topic_related(text, profile.topic_keywords):
                continue
            keyword_score = legal_score(text, profile.legal_keywords)
            score = keyword_score + float(item.get("score", 0.0)) * 4.0
            candidate = dict(item)
            candidate["strategy"] = run.rule.strategy
            candidate["quality_score"] = score
            candidate["keyword_score"] = keyword_score
            candidate["published_iso"] = parse_published_iso(str(candidate.get("published_date", "")))
            url_date = parse_date_from_url(url)
            candidate["url_date_iso"] = url_date.isoformat() if url_date else ""
            previous = merged_by_url.get(url)
            if previous is None or float(candidate["quality_score"]) > float(previous.get("quality_score", 0)):
                merged_by_url[url] = candidate

    ranked = sorted(
        merged_by_url.values(),
        key=lambda x: (float(x.get("quality_score", 0.0)), float(x.get("score", 0.0))),
        reverse=True,
    )

    # Stage 4: extract top URLs to improve date detection with minimal credit overhead.
    extract_subset = [item.get("url", "") for item in ranked[: profile.budget.extract_url_limit] if item.get("url")]
    extract_credits = 0.0
    extracted_by_url: dict[str, dict[str, Any]] = {}
    if extract_subset:
        extract_response = _extract_with_retry(
            client,
            {
                "urls": extract_subset,
                "extract_depth": "basic",
                "format": "text",
                "include_usage": True,
            },
        )
        extract_credits = float((extract_response.get("usage") or {}).get("credits", 0))
        for entry in extract_response.get("results", []) or []:
            url = entry.get("url", "")
            if url:
                extracted_by_url[url] = entry

    # Stage 5: apply strict 7-day filter using published date, URL date, then extract fallback.
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    strict_items: list[dict[str, Any]] = []
    for item in ranked:
        url = item.get("url", "")
        in_window = False
        effective_date = ""
        published_iso = str(item.get("published_iso", "") or "")
        url_date_iso = str(item.get("url_date_iso", "") or "")

        if published_iso:
            try:
                pub_d = date.fromisoformat(published_iso)
                if start_d <= pub_d <= end_d:
                    in_window = True
                    effective_date = pub_d.isoformat()
            except ValueError:
                pass
        if (not in_window) and url_date_iso:
            try:
                url_d = date.fromisoformat(url_date_iso)
                if start_d <= url_d <= end_d:
                    in_window = True
                    effective_date = url_d.isoformat()
            except ValueError:
                pass
        if (not in_window) and url in extracted_by_url:
            raw = str(extracted_by_url[url].get("raw_content", "") or extracted_by_url[url].get("content", ""))
            head = " ".join(raw.split())[:1800]
            for cand in parse_date_candidates(head):
                if start_d <= cand <= end_d:
                    in_window = True
                    effective_date = cand.isoformat()
                    break

        item["strict_in_week"] = in_window
        item["strict_effective_date"] = effective_date
        if in_window and int(item.get("keyword_score", 0)) > 0:
            strict_items.append(item)

    strict_items.sort(
        key=lambda x: (x.get("strict_effective_date", ""), float(x.get("quality_score", 0.0))),
        reverse=True,
    )

    # Stage 5b: fetch recent posts from open Telegram channels found in prompt sources.
    telegram_channels = extract_telegram_channels(include_domains)
    telegram_items: list[dict[str, Any]] = []
    telegram_errors: list[str] = []
    existing_urls = {str(item.get("url", "")).strip() for item in strict_items if item.get("url")}
    for channel in telegram_channels:
        posts, error = parse_recent_open_channel_posts(
            channel,
            start_date=start_d,
            end_date=end_d,
            max_posts=8,
        )
        if error:
            telegram_errors.append(f"{channel}: {error}")
            continue
        for post in posts:
            if post.url in existing_urls:
                continue
            existing_urls.add(post.url)
            telegram_items.append(
                {
                    "title": _build_telegram_title(post.channel, post.text, post.post_id),
                    "content": post.text,
                    "url": post.url,
                    "strict_effective_date": post.published_iso,
                    "quality_score": 0.5,
                }
            )

    strict_items.extend(telegram_items)
    strict_items.sort(
        key=lambda x: (x.get("strict_effective_date", ""), float(x.get("quality_score", 0.0))),
        reverse=True,
    )

    # Stage 6: build final digest rows in standard output format.
    digest_rows: list[dict[str, str]] = []
    for item in strict_items[:15]:
        text_for_category = f"{item.get('title', '')} {item.get('content', '')}"
        digest_rows.append(
            {
                "category": infer_category(text_for_category, profile),
                "title": str(item.get("title", "(без заголовка)")).strip() or "(без заголовка)",
                "summary": build_summary(str(item.get("content", "")), str(item.get("title", ""))),
                "url": str(item.get("url", "")).strip(),
                "date": str(item.get("strict_effective_date", "")).strip(),
            }
        )

    usage = {
        "search_credits": search_credits,
        "extract_credits": extract_credits,
        "total_credits": search_credits + extract_credits,
        "target_credits": profile.budget.target_credits,
        "search_runs": len(runs),
        "max_results_per_run": profile.budget.max_results,
        "extract_url_limit": profile.budget.extract_url_limit,
        "start_date": start_date,
        "end_date": end_date,
        "strict_items_count": len(strict_items),
        "telegram_channels_total": len(telegram_channels),
        "telegram_channels_ok": len(telegram_channels) - len(telegram_errors),
        "telegram_errors_count": len(telegram_errors),
        "telegram_posts_added": len(telegram_items),
    }
    return digest_rows, usage


def format_digest_response(profile: PromptProfile, rows: list[dict[str, str]], usage: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(profile.report_label)
    lines.append("")
    lines.append(f"Период: {usage['start_date']} .. {usage['end_date']}")
    lines.append("")
    if not rows:
        lines.append("Материалы с подтвержденной датой за последние 7 дней не найдены.")
        lines.append("")
    else:
        for idx, row in enumerate(rows, start=1):
            lines.append(f"{idx}) {row['category']}")
            lines.append(f"Заголовок: {row['title']}")
            lines.append(f"Резюме: {row['summary']}")
            lines.append(f"Ссылка: {row['url']}")
            lines.append(f"Дата: {row['date']}")
            lines.append("")
    lines.append(
        f"Budget usage (credits): total={usage['total_credits']:.1f} "
        f"(search={usage['search_credits']:.1f}, extract={usage['extract_credits']:.1f}), "
        f"target<={usage['target_credits']}"
    )
    lines.append(
        "Telegram sources: "
        f"channels={usage.get('telegram_channels_total', 0)}, "
        f"ok={usage.get('telegram_channels_ok', 0)}, "
        f"posts_added={usage.get('telegram_posts_added', 0)}, "
        f"errors={usage.get('telegram_errors_count', 0)}"
    )
    if "followup_plan_status" in usage or "followup_search_status" in usage:
        lines.append(
            "Adaptive follow-up: "
            f"seed={usage.get('perplexity_seed_status', 'n/a')}, "
            f"plan={usage.get('followup_plan_status', 'n/a')}, "
            f"queries={usage.get('followup_plan_queries_count', 0)}, "
            f"runs={usage.get('followup_search_runs_executed', 0)}, "
            f"rows_added={usage.get('followup_search_rows_added', 0)}, "
            f"credits={float(usage.get('followup_search_credits', 0.0)):.1f}"
        )
    if "openrouter_filter_status" in usage:
        lines.append(
            "OpenRouter filter: "
            f"status={usage.get('openrouter_filter_status')}, "
            f"input={usage.get('openrouter_filter_input_items', 0)}, "
            f"output={usage.get('openrouter_filter_output_items', 0)}, "
            f"removed={usage.get('openrouter_filter_removed_items', 0)}"
        )
    return "\n".join(lines).strip()
