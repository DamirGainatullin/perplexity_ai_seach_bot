import re
import time
from dataclasses import dataclass
from datetime import date, timedelta
from html import escape
from typing import Any, Optional

from requests import RequestException
from tavily import TavilyClient

from pipelines.helpers import (
    build_include_domains,
    build_summary,
    extract_urls,
    group_domains,
    infer_category,
    resolve_domain_scope,
    short_query,
)
from pipelines.models import PromptProfile, QueryRule
from pipelines.publication_date import verify_publication_dates
from pipelines.tavily_proxy import format_tavily_error, get_tavily_proxies
from pipelines.telegram_feed import extract_telegram_channels, parse_recent_open_channel_posts

_SECTION_LINE_RE = re.compile(r"^\d+\)\s+")
FINAL_SUMMARY_CHAR_LIMIT = 420


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


def _truncate_summary_for_output(text: str, limit: int = FINAL_SUMMARY_CHAR_LIMIT) -> str:
    normalized = " ".join(str(text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: max(0, limit - 3)].rstrip() + "..."


def _search_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.search(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily search failed after retries: {format_tavily_error(last_error)}") from last_error


def _extract_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.extract(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily extract failed after retries: {format_tavily_error(last_error)}") from last_error


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

    client = TavilyClient(tavily_api_key, proxies=get_tavily_proxies())
    plan = profile.query_plan[: profile.budget.search_run_limit]
    runs: list[SearchRun] = []
    search_credits = 0.0
    search_run_details: list[dict[str, Any]] = []

    # Stage 2: run limited Tavily search calls according to prompt profile query plan.
    for rule in plan:
        scoped_domains = resolve_domain_scope(rule.domain_scope, groups)
        query_text = short_query(rule.query)
        response = _search_with_retry(
            client,
            {
                "query": query_text,
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
        run_credits = float((response.get("usage") or {}).get("credits", 0))
        search_credits += run_credits
        run_results = response.get("results", []) or []
        search_run_details.append(
            {
                "strategy": rule.strategy,
                "topic": rule.topic,
                "domain_scope": rule.domain_scope,
                "query": query_text,
                "include_domains_count": len(scoped_domains),
                "include_domains": scoped_domains,
                "credits": run_credits,
                "results_count": len(run_results),
                "top_results": [
                    {
                        "title": str(x.get("title", "")),
                        "url": str(x.get("url", "")),
                        "published_date": str(x.get("published_date", "")),
                        "score": float(x.get("score", 0.0) or 0.0),
                    }
                    for x in run_results[:5]
                ],
            }
        )
        runs.append(SearchRun(rule=rule, include_domains=scoped_domains, response=response))

    # Stage 3: merge results by URL and keep best-scored candidate.
    merged_by_url: dict[str, dict[str, Any]] = {}
    for run in runs:
        for item in run.response.get("results", []) or []:
            url = item.get("url") or ""
            if not url:
                continue
            score = float(item.get("score", 0.0) or 0.0)
            candidate = dict(item)
            candidate["strategy"] = run.rule.strategy
            candidate["quality_score"] = score
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

    # Stage 5: independently verify publication dates. Tavily's date alone is not trusted
    # because general search can expose a page update/crawl date instead of publication date.
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    verification_candidates = []
    for item in ranked[: profile.budget.extract_url_limit]:
        url = str(item.get("url", "")).strip()
        extracted = extracted_by_url.get(url, {})
        verification_candidates.append(
            {
                "url": url,
                "tavily_published_date": str(item.get("published_date", "")),
                "extracted_text": str(extracted.get("raw_content", "") or extracted.get("content", "")),
            }
        )
    verified_dates = verify_publication_dates(verification_candidates, start_d, end_d)

    strict_items: list[dict[str, Any]] = []
    web_items_total = 0
    web_items_in_week = 0
    web_items_with_resolved_date = 0
    web_items_without_date = 0
    web_items_out_of_week_dropped = 0
    web_items_unverified_date_dropped = 0
    web_items_not_selected_for_date_verification = 0
    web_date_source_counts: dict[str, int] = {}
    web_date_rejections: list[dict[str, str]] = []
    for item in ranked:
        url = str(item.get("url", "")).strip()
        verification = verified_dates.get(url)
        if verification is None:
            web_items_not_selected_for_date_verification += 1
            if len(web_date_rejections) < 30:
                web_date_rejections.append({"url": url, "reason": "not_selected_for_verification"})
            continue
        if verification.status == "outside_window":
            web_items_out_of_week_dropped += 1
            if len(web_date_rejections) < 30:
                web_date_rejections.append(
                    {
                        "url": url,
                        "reason": "outside_window",
                        "date": verification.date_iso,
                        "source": verification.source,
                    }
                )
            continue
        if verification.status != "verified":
            web_items_unverified_date_dropped += 1
            if len(web_date_rejections) < 30:
                web_date_rejections.append(
                    {
                        "url": url,
                        "reason": "unverified_publication_date",
                        "tavily_published_date": verification.tavily_published_date,
                        "detail": verification.detail,
                    }
                )
            continue

        item["strict_in_week"] = True
        item["strict_effective_date"] = verification.date_iso
        item["date_confidence"] = verification.source
        strict_items.append(item)
        web_items_total += 1
        web_items_in_week += 1
        web_items_with_resolved_date += 1
        web_date_source_counts[verification.source] = web_date_source_counts.get(verification.source, 0) + 1

    strict_items.sort(
        key=lambda x: (float(x.get("quality_score", 0.0)), x.get("strict_effective_date", "")),
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
        key=lambda x: (float(x.get("quality_score", 0.0)), x.get("strict_effective_date", "")),
        reverse=True,
    )

    # Stage 6: build final digest rows in standard output format.
    # Keep a wider pre-LLM candidate pool so web sources are less likely to be crowded out.
    digest_rows: list[dict[str, str]] = []
    pre_llm_limit = 30
    pre_llm_candidates = strict_items[:pre_llm_limit]
    for item in pre_llm_candidates:
        text_for_category = f"{item.get('title', '')} {item.get('content', '')}"
        digest_rows.append(
            {
                "category": infer_category(text_for_category, profile),
                "title": str(item.get("title", "(без заголовка)")).strip() or "(без заголовка)",
                "summary": build_summary(str(item.get("content", "")), str(item.get("title", ""))),
                "content": str(item.get("content", "")).strip(),
                "url": str(item.get("url", "")).strip(),
                "date": str(item.get("strict_effective_date", "")).strip(),
                "rank_score": f"{float(item.get('quality_score', 0.0) or 0.0):.6f}",
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
        "web_items_total": web_items_total,
        "web_items_in_week": web_items_in_week,
        "web_items_with_resolved_date": web_items_with_resolved_date,
        "web_items_without_date": web_items_without_date,
        "web_items_out_of_week_dropped": web_items_out_of_week_dropped,
        "web_items_unverified_date_dropped": web_items_unverified_date_dropped,
        "web_items_not_selected_for_date_verification": web_items_not_selected_for_date_verification,
        "web_date_source_counts": web_date_source_counts,
        "web_date_rejections": web_date_rejections,
        "telegram_channels_total": len(telegram_channels),
        "telegram_channels_ok": len(telegram_channels) - len(telegram_errors),
        "telegram_errors_count": len(telegram_errors),
        "telegram_posts_added": len(telegram_items),
        "pre_llm_limit": pre_llm_limit,
        "pre_llm_candidates_total": len(pre_llm_candidates),
        "pre_llm_candidates_tg": sum(
            1 for x in pre_llm_candidates if str(x.get("url", "")).strip().startswith("https://t.me/")
        ),
        "pre_llm_candidates_web": sum(
            1 for x in pre_llm_candidates if not str(x.get("url", "")).strip().startswith("https://t.me/")
        ),
        "search_run_details": search_run_details,
        "extract_urls_requested": extract_subset,
        "extract_urls_extracted": len(extracted_by_url),
        "strict_top_items_debug": [
            {
                "title": str(x.get("title", "")),
                "url": str(x.get("url", "")),
                "date": str(x.get("strict_effective_date", "")),
                "quality_score": float(x.get("quality_score", 0.0) or 0.0),
            }
            for x in strict_items[:20]
        ],
    }
    return digest_rows, usage


def format_digest_response(profile: PromptProfile, rows: list[dict[str, str]], usage: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(profile.report_label)
    lines.append("")
    lines.append(f"Период: {usage['start_date']} .. {usage['end_date']}")
    lines.append("")
    if not rows:
        lines.append(
            str(
                usage.get(
                    "empty_message",
                    "Материалы с подтвержденной датой за последние 7 дней не найдены.",
                )
            )
        )
        lines.append("")
    else:
        for idx, row in enumerate(rows, start=1):
            lines.append(f"{idx}) {row['category']}")
            lines.append(str(row['title']))
            lines.append(f"Резюме: {_truncate_summary_for_output(row['summary'])}")
            lines.append(f"Ссылка: {row['url']}")
            lines.append(f"Дата: {row['date'] or 'не установлена'}")
            lines.append("")
    return "\n".join(lines).strip()


def format_digest_response_html(text: str) -> str:
    lines: list[str] = []
    highlight_next = False
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line:
            lines.append("")
            highlight_next = False
            continue
        if highlight_next:
            lines.append(f"<b>{escape(line)}</b>")
            highlight_next = False
            continue
        lines.append(escape(line))
        if _SECTION_LINE_RE.match(line):
            highlight_next = True
    return "\n".join(lines)
