from __future__ import annotations

import json
import re
import time
from datetime import date, timedelta
from typing import Any, Optional

import requests
from requests import RequestException
from tavily import TavilyClient

from pipelines.helpers import (
    build_summary,
    infer_category,
    is_topic_related,
    legal_score,
    parse_date_from_url,
    parse_published_iso,
    short_query,
)
from pipelines.models import PromptProfile
from pipelines.openrouter_filter import DEFAULT_OPENROUTER_MODEL, OPENROUTER_URL


DEFAULT_FOLLOWUP_CREDIT_CAP = 8.0
DEFAULT_FOLLOWUP_MAX_RESULTS = 5
DEFAULT_FOLLOWUP_MAX_QUERIES = 4


def _extract_json_from_text(text: str) -> dict[str, Any] | None:
    raw = (text or "").strip()
    if not raw:
        return None

    fenced = re.search(r"```(?:json)?\s*(.*?)```", raw, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        raw = fenced.group(1).strip()

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    first_brace = raw.find("{")
    last_brace = raw.rfind("}")
    if first_brace == -1 or last_brace == -1 or last_brace <= first_brace:
        return None

    fragment = raw[first_brace : last_brace + 1]
    try:
        parsed = json.loads(fragment)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, dict):
        return parsed
    return None


def _build_planner_prompt(prompt_text: str, max_queries: int) -> str:
    compact_prompt = " ".join((prompt_text or "").split())
    if len(compact_prompt) > 4000:
        compact_prompt = compact_prompt[:4000] + "..."
    return (
        "Ты планировщик поисковых запросов для Tavily.\n"
        "Тебе дан краткий внешнй summary и список уже найденных материалов.\n"
        "Нужно предложить дополнительные запросы, чтобы закрыть пробелы в покрытии темы.\n"
        "Критично:\n"
        "- не придумывай факты;\n"
        "- запросы должны быть проверяемыми и юридически релевантными;\n"
        "- не указывай домены, не добавляй site: фильтры;\n"
        f"- верни не более {max_queries} запросов.\n"
        "Формат ответа строго JSON без markdown:\n"
        '{"queries":[{"query":"..."},{"query":"..."}]}\n'
        "Если доп. запросы не нужны:\n"
        '{"queries":[]}\n\n'
        f"ТЕМАТИКА ИЗ ПРОМПТА:\n{compact_prompt}"
    )


def _normalize_queries(items: list[Any], max_queries: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if isinstance(item, dict):
            raw = str(item.get("query", "")).strip()
        else:
            raw = str(item).strip()
        if not raw:
            continue
        cleaned = " ".join(raw.split())
        if len(cleaned) < 16:
            continue
        lowered = cleaned.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(cleaned)
        if len(result) >= max_queries:
            break
    return result


def plan_followup_queries(
    prompt_text: str,
    perplexity_summary: str,
    current_rows: list[dict[str, str]],
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    max_queries: int = DEFAULT_FOLLOWUP_MAX_QUERIES,
    timeout_sec: int = 60,
) -> tuple[list[str], dict[str, Any]]:
    if not openrouter_api_key:
        return [], {"followup_plan_status": "skipped_no_openrouter_key"}
    if not (perplexity_summary or "").strip():
        return [], {"followup_plan_status": "skipped_no_perplexity_summary"}

    rows_payload = [
        {
            "title": row.get("title", ""),
            "summary": row.get("summary", ""),
            "url": row.get("url", ""),
            "date": row.get("date", ""),
        }
        for row in current_rows[:15]
    ]
    user_payload = {
        "perplexity_summary": (perplexity_summary or "")[:6000],
        "current_tavily_results": rows_payload,
        "task": "сформируй дополнительные поисковые запросы для проверки и углубления coverage",
    }

    payload = {
        "model": model or DEFAULT_OPENROUTER_MODEL,
        "temperature": 0.1,
        "max_tokens": 500,
        "messages": [
            {"role": "system", "content": _build_planner_prompt(prompt_text, max_queries=max_queries)},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
    }
    headers = {
        "Authorization": f"Bearer {openrouter_api_key}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout_sec)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        return [], {"followup_plan_status": f"error_request: {exc}"}

    content = (
        ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        if isinstance(data, dict)
        else ""
    )
    parsed = _extract_json_from_text(str(content))
    if not parsed:
        return [], {"followup_plan_status": "error_parse_response"}

    queries_raw = parsed.get("queries")
    if not isinstance(queries_raw, list):
        return [], {"followup_plan_status": "error_invalid_schema"}
    queries = _normalize_queries(queries_raw, max_queries=max_queries)

    usage = data.get("usage") if isinstance(data, dict) else {}
    return queries, {
        "followup_plan_status": "ok",
        "followup_plan_model": model or DEFAULT_OPENROUTER_MODEL,
        "followup_plan_queries_count": len(queries),
        "followup_plan_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "followup_plan_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def _search_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.search(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily search failed after retries: {last_error}") from last_error


def run_followup_queries(
    profile: PromptProfile,
    tavily_api_key: str,
    now_date: date,
    queries: list[str],
    credit_cap: float = DEFAULT_FOLLOWUP_CREDIT_CAP,
    max_results_per_query: int = DEFAULT_FOLLOWUP_MAX_RESULTS,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not queries:
        return [], {"followup_search_status": "skipped_no_queries"}

    start_date = (now_date - timedelta(days=7)).isoformat()
    end_date = now_date.isoformat()
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)

    client = TavilyClient(tavily_api_key)
    search_credits = 0.0
    runs_executed = 0
    errors: list[str] = []
    merged_by_url: dict[str, dict[str, Any]] = {}

    for query in queries:
        # Advanced search typically costs 2 credits. Keep a hard margin by cap.
        if search_credits + 2.0 > float(credit_cap) + 1e-9:
            break

        payload = {
            "query": short_query(query),
            "topic": "news",
            "search_depth": "advanced",
            "start_date": start_date,
            "end_date": end_date,
            "include_answer": "basic",
            "include_domains": None,
            "include_usage": True,
            "max_results": max_results_per_query,
        }
        try:
            response = _search_with_retry(client, payload)
        except Exception as exc:
            errors.append(f"{query[:120]}: {exc}")
            continue

        runs_executed += 1
        search_credits += float((response.get("usage") or {}).get("credits", 0))
        for item in response.get("results", []) or []:
            url = item.get("url") or ""
            if not url:
                continue

            text = f"{item.get('title', '')} {item.get('content', '')}"
            if not is_topic_related(text, profile.topic_keywords):
                continue

            keyword_score = legal_score(text, profile.legal_keywords)
            if keyword_score <= 0:
                continue

            published_iso = parse_published_iso(str(item.get("published_date", "")))
            url_date = parse_date_from_url(url)
            url_date_iso = url_date.isoformat() if url_date else ""

            effective_date = ""
            if published_iso:
                try:
                    pub_d = date.fromisoformat(published_iso)
                    if start_d <= pub_d <= end_d:
                        effective_date = published_iso
                except ValueError:
                    pass
            if (not effective_date) and url_date_iso:
                try:
                    url_d = date.fromisoformat(url_date_iso)
                    if start_d <= url_d <= end_d:
                        effective_date = url_date_iso
                except ValueError:
                    pass
            if not effective_date:
                continue

            score = keyword_score + float(item.get("score", 0.0)) * 4.0
            candidate = {
                "title": str(item.get("title", "(без заголовка)")).strip() or "(без заголовка)",
                "content": str(item.get("content", "")),
                "url": str(url).strip(),
                "date": effective_date,
                "quality_score": score,
            }
            old = merged_by_url.get(candidate["url"])
            if old is None or float(candidate["quality_score"]) > float(old.get("quality_score", 0.0)):
                merged_by_url[candidate["url"]] = candidate

    ranked = sorted(
        merged_by_url.values(),
        key=lambda x: (x.get("date", ""), float(x.get("quality_score", 0.0))),
        reverse=True,
    )
    rows = [
        {
            "category": infer_category(f"{item.get('title', '')} {item.get('content', '')}", profile),
            "title": str(item.get("title", "(без заголовка)")).strip() or "(без заголовка)",
            "summary": build_summary(str(item.get("content", "")), str(item.get("title", ""))),
            "url": str(item.get("url", "")).strip(),
            "date": str(item.get("date", "")).strip(),
        }
        for item in ranked
    ]

    return rows, {
        "followup_search_status": "ok",
        "followup_search_queries_requested": len(queries),
        "followup_search_runs_executed": runs_executed,
        "followup_search_rows_added": len(rows),
        "followup_search_credits": search_credits,
        "followup_search_credit_cap": float(credit_cap),
        "followup_search_errors_count": len(errors),
    }


def merge_digest_rows(base_rows: list[dict[str, str]], extra_rows: list[dict[str, str]], limit: int = 15) -> list[dict[str, str]]:
    merged_by_url: dict[str, dict[str, str]] = {}
    for row in base_rows + extra_rows:
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        old = merged_by_url.get(url)
        if old is None:
            merged_by_url[url] = row
            continue
        # Prefer row with later date; fallback to longer summary.
        old_date = str(old.get("date", ""))
        new_date = str(row.get("date", ""))
        if new_date > old_date:
            merged_by_url[url] = row
        elif new_date == old_date and len(str(row.get("summary", ""))) > len(str(old.get("summary", ""))):
            merged_by_url[url] = row

    merged = sorted(
        merged_by_url.values(),
        key=lambda x: str(x.get("date", "")),
        reverse=True,
    )
    return merged[:limit]
