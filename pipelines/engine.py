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

    # Stage 5: date resolution + hard out-of-window guard.
    # If a reliable date is found and it is outside the 7-day window, the web item is dropped.
    # If date cannot be resolved, the item is kept with empty date.
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)
    strict_items: list[dict[str, Any]] = []
    web_items_total = 0
    web_items_in_week = 0
    web_items_with_resolved_date = 0
    web_items_without_date = 0
    web_items_out_of_week_dropped = 0
    for item in ranked:
        url = item.get("url", "")
        in_window = False
        effective_date = ""
        published_iso = str(item.get("published_iso", "") or "")
        url_date_iso = str(item.get("url_date_iso", "") or "")
        resolved_dates: list[date] = []
        text_head = " ".join(f"{item.get('title', '')} {item.get('content', '')}".split())[:2500]
        for cand in parse_date_candidates(text_head):
            resolved_dates.append(cand)
            if start_d <= cand <= end_d:
                in_window = True
                effective_date = max(effective_date, cand.isoformat())

        if published_iso:
            try:
                pub_d = date.fromisoformat(published_iso)
                resolved_dates.append(pub_d)
                if start_d <= pub_d <= end_d:
                    in_window = True
                    effective_date = max(effective_date, pub_d.isoformat())
            except ValueError:
                pass
        if url_date_iso:
            try:
                url_d = date.fromisoformat(url_date_iso)
                resolved_dates.append(url_d)
                if start_d <= url_d <= end_d:
                    in_window = True
                    effective_date = max(effective_date, url_d.isoformat())
            except ValueError:
                pass
        if url in extracted_by_url:
            raw = str(extracted_by_url[url].get("raw_content", "") or extracted_by_url[url].get("content", ""))
            head = " ".join(raw.split())[:6000]
            for cand in parse_date_candidates(head):
                resolved_dates.append(cand)
                if start_d <= cand <= end_d:
                    in_window = True
                    effective_date = max(effective_date, cand.isoformat())

        if resolved_dates and (not in_window):
            # Hard date guard: if we can confirm date and it is outside last 7 days, drop item.
            web_items_out_of_week_dropped += 1
            continue

        if (not effective_date) and resolved_dates:
            # If we resolved multiple in-window dates, keep the latest for sorting/debug.
            best_date = max(resolved_dates)
            effective_date = best_date.isoformat()

        item["strict_in_week"] = in_window
        item["strict_effective_date"] = effective_date
        strict_items.append(item)
        web_items_total += 1
        if in_window:
            web_items_in_week += 1
        if effective_date:
            web_items_with_resolved_date += 1
        else:
            web_items_without_date += 1

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
    if "web_items_total" in usage:
        lines.append(
            "Web sources: "
            f"total={usage.get('web_items_total', 0)}, "
            f"in_week={usage.get('web_items_in_week', 0)}, "
            f"resolved_date={usage.get('web_items_with_resolved_date', 0)}, "
            f"no_date={usage.get('web_items_without_date', 0)}, "
            f"dropped_out_of_week={usage.get('web_items_out_of_week_dropped', 0)}"
        )
    if "pre_llm_candidates_total" in usage:
        lines.append(
            "Pre-LLM mix: "
            f"limit={usage.get('pre_llm_limit', 0)}, "
            f"total={usage.get('pre_llm_candidates_total', 0)}, "
            f"web={usage.get('pre_llm_candidates_web', 0)}, "
            f"tg={usage.get('pre_llm_candidates_tg', 0)}"
        )
    if "perplexity_seed_status" in usage:
        lines.append(
            "Perplexity seed: "
            f"status={usage.get('perplexity_seed_status', 'n/a')}, "
            f"sender_messages={usage.get('perplexity_seed_sender_messages', 0)}, "
            f"slot_matched_reports={usage.get('perplexity_seed_profile_messages', 0)}, "
            f"sender_filter={usage.get('perplexity_seed_sender_filter', 'n/a')}"
        )
    if "followup_plan_status" in usage or "followup_search_status" in usage:
        lines.append(
            "Adaptive follow-up: "
            f"seed_status={usage.get('perplexity_seed_status', 'n/a')}, "
            f"plan={usage.get('followup_plan_status', 'n/a')}, "
            f"queries={usage.get('followup_plan_queries_count', 0)}, "
            f"runs={usage.get('followup_search_runs_executed', 0)}, "
            f"rows_added={usage.get('followup_search_rows_added', 0)}, "
            f"rows_after_merge={usage.get('followup_rows_after_merge', 0)}, "
            f"final_rows={usage.get('final_rows_count', 0)}, "
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
    if "openrouter_pipeline_status" in usage or "openrouter_stage1_status" in usage:
        lines.append(
            "OpenRouter 3-stage: "
            f"pipeline={usage.get('openrouter_pipeline_status', 'n/a')}, "
            f"s1={usage.get('openrouter_stage1_status', 'n/a')}, "
            f"s2={usage.get('openrouter_stage2_status', 'n/a')}, "
            f"s3={usage.get('openrouter_stage3_status', 'n/a')}"
        )
    return "\n".join(lines).strip()
