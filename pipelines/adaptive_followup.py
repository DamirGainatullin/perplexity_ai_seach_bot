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
    parse_date_candidates,
    parse_date_from_url,
    parse_published_iso,
    short_query,
)
from pipelines.models import PromptProfile
from pipelines.openrouter_filter import DEFAULT_OPENROUTER_MODEL, OPENROUTER_URL


DEFAULT_FOLLOWUP_CREDIT_CAP = 8.0
DEFAULT_FOLLOWUP_MAX_RESULTS = 5
DEFAULT_FOLLOWUP_MAX_QUERIES = 4
FOLLOWUP_RU_ANCHORS = ("Россия", "РФ", "Минтранс", "НПА", "разъяснения")
FOLLOWUP_EXTRACT_URL_LIMIT = 10
FOLLOWUP_PLANNER_SUMMARY_MAX_CHARS = 12000
FOLLOWUP_PLANNER_ROWS_LIMIT = 30


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
        "Ты аналитик, который готовит дополнительные поисковые запросы для Tavily.\n"
        "Входные данные: агрегированная сводка Perplexity за несколько ежедневных отчетов и список уже найденных материалов.\n"
        "Цель: закрыть пробелы в покрытии темы из профиля.\n\n"
        "Сначала выполни отбор релевантных тезисов:\n"
        "1) Выдели из Perplexity только наиболее релевантные теме события/заголовки.\n"
        f"2) Выбери не более {max_queries} пунктов с максимальной юридической релевантностью.\n"
        "3) Приоритет: новые НПА, поправки, судебная практика, официальные разъяснения, правоприменение в РФ.\n"
        "4) Если одна и та же тема повторяется в нескольких ежедневных отчетах, объедини ее в один тезис и не дублируй.\n"
        "5) Игнорируй нерелевантные или слишком общие международные сюжеты, если нет прямой связи с регулированием в РФ.\n\n"
        "Затем сформируй поисковые запросы:\n"
        "1) На каждый выбранный пункт — один запрос Tavily.\n"
        "2) Запросы должны быть на русском языке.\n"
        "3) Запросы должны быть проверяемыми и юридически конкретными.\n"
        "4) Не указывай домены, не используй site: фильтры.\n"
        "5) Учитывай уже найденные материалы, чтобы не дублировать их.\n\n"
        "Формат ответа: строго JSON, без markdown:\n"
        '{"selected_headlines":["..."],"queries":[{"query":"...","headline":"...","reason":"..."}]}\n'
        "Если релевантных дополнений нет, верни:\n"
        '{"selected_headlines":[],"queries":[]}\n\n'
        f"ТЕМАТИКА ИЗ ПРОМПТА:\n{compact_prompt}"
    )


def _build_planner_fallback_prompt(prompt_text: str, max_queries: int) -> str:
    compact_prompt = " ".join((prompt_text or "").split())
    if len(compact_prompt) > 3000:
        compact_prompt = compact_prompt[:3000] + "..."
    return (
        "Ты формируешь дополнительные запросы для Tavily по агрегированной сводке Perplexity за несколько ежедневных писем.\n"
        "Выбери только релевантные теме пункты и составь до "
        f"{max_queries}"
        " запросов.\n"
        "Если одинаковая тема повторяется в нескольких письмах, не дублируй ее.\n"
        "Запросы должны быть на русском, юридически конкретные, без доменов и без site:.\n"
        "Не выдумывай факты.\n\n"
        "Верни строго JSON без markdown:\n"
        '{"queries":["...","..."]}\n'
        "Если релевантных дополнений нет:\n"
        '{"queries":[]}\n\n'
        f"ТЕМАТИКА ИЗ ПРОМПТА:\n{compact_prompt}"
    )


def _call_planner_model(
    *,
    openrouter_api_key: str,
    model: str,
    system_prompt: str,
    user_payload: dict[str, Any],
    timeout_sec: int,
    max_tokens: int,
) -> tuple[dict[str, Any] | None, dict[str, Any], str]:
    payload = {
        "model": model or DEFAULT_OPENROUTER_MODEL,
        "temperature": 0.1,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
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
        return None, {}, f"error_request: {exc}"

    content = (
        ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        if isinstance(data, dict)
        else ""
    )
    parsed = _extract_json_from_text(str(content))
    usage = data.get("usage") if isinstance(data, dict) else {}
    if not parsed:
        return None, usage or {}, "error_parse_response"
    return parsed, usage or {}, "ok"


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


def _boost_ru_queries(queries: list[str]) -> tuple[list[str], list[str]]:
    boosted: list[str] = []
    raw_queries: list[str] = []
    seen: set[str] = set()
    for query in queries:
        cleaned = " ".join((query or "").split())
        if not cleaned:
            continue
        raw_queries.append(cleaned)
        missing = [anchor for anchor in FOLLOWUP_RU_ANCHORS if anchor.lower() not in cleaned.lower()]
        candidate = cleaned if not missing else f"{cleaned} {' '.join(missing)}"
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        boosted.append(candidate)
    return boosted, raw_queries


def _normalize_headlines(items: Any, max_items: int) -> list[str]:
    if not isinstance(items, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        raw = str(item).strip()
        if not raw:
            continue
        cleaned = " ".join(raw.split())
        if len(cleaned) < 10:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
        if len(result) >= max_items:
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
        for row in current_rows[:FOLLOWUP_PLANNER_ROWS_LIMIT]
    ]
    user_payload = {
        "perplexity_summary": (perplexity_summary or "")[:FOLLOWUP_PLANNER_SUMMARY_MAX_CHARS],
        "current_tavily_results": rows_payload,
        "task": (
            "Выбери самые релевантные для темы тезисы из Perplexity, "
            "затем сформируй до 4 дополнительных русскоязычных запросов Tavily по этим тезисам."
        ),
    }

    parsed, usage, status = _call_planner_model(
        openrouter_api_key=openrouter_api_key,
        model=model,
        system_prompt=_build_planner_prompt(prompt_text, max_queries=max_queries),
        user_payload=user_payload,
        timeout_sec=timeout_sec,
        max_tokens=700,
    )
    if status == "ok" and isinstance(parsed, dict):
        queries_raw = parsed.get("queries")
        if isinstance(queries_raw, list):
            queries_normalized = _normalize_queries(queries_raw, max_queries=max_queries)
            queries, raw_queries = _boost_ru_queries(queries_normalized)
            selected_headlines = _normalize_headlines(parsed.get("selected_headlines"), max_queries)
            return queries, {
                "followup_plan_status": "ok",
                "followup_plan_model": model or DEFAULT_OPENROUTER_MODEL,
                "followup_plan_queries_count": len(queries),
                "followup_plan_queries": queries,
                "followup_plan_queries_raw": raw_queries,
                "followup_plan_rows_input_count": len(rows_payload),
                "followup_plan_perplexity_summary_chars": len(user_payload["perplexity_summary"]),
                "followup_plan_ru_anchors": list(FOLLOWUP_RU_ANCHORS),
                "followup_plan_selected_headlines_count": len(selected_headlines),
                "followup_plan_selected_headlines": selected_headlines,
                "followup_plan_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
                "followup_plan_completion_tokens": (usage or {}).get("completion_tokens", 0),
                "followup_plan_attempts": 1,
            }

    fallback_payload = {
        "perplexity_summary": (perplexity_summary or "")[:FOLLOWUP_PLANNER_SUMMARY_MAX_CHARS],
        "task": (
            "Выбери самые релевантные для темы пункты и сформируй до 4 русскоязычных "
            "юридических поисковых запросов для Tavily."
        ),
    }
    parsed_fb, usage_fb, status_fb = _call_planner_model(
        openrouter_api_key=openrouter_api_key,
        model=model,
        system_prompt=_build_planner_fallback_prompt(prompt_text, max_queries=max_queries),
        user_payload=fallback_payload,
        timeout_sec=timeout_sec,
        max_tokens=500,
    )
    if status_fb != "ok" or not isinstance(parsed_fb, dict):
        return [], {
            "followup_plan_status": status if status != "ok" else "error_invalid_schema",
            "followup_plan_fallback_status": status_fb,
            "followup_plan_attempts": 2,
        }

    queries_raw_fb = parsed_fb.get("queries")
    if not isinstance(queries_raw_fb, list):
        return [], {
            "followup_plan_status": status if status != "ok" else "error_invalid_schema",
            "followup_plan_fallback_status": "error_invalid_schema",
            "followup_plan_attempts": 2,
        }

    queries_fb_normalized = _normalize_queries(queries_raw_fb, max_queries=max_queries)
    queries_fb, raw_queries_fb = _boost_ru_queries(queries_fb_normalized)
    selected_headlines_fb = _normalize_headlines(parsed_fb.get("selected_headlines"), max_queries)
    return queries_fb, {
        "followup_plan_status": "ok_fallback",
        "followup_plan_model": model or DEFAULT_OPENROUTER_MODEL,
        "followup_plan_queries_count": len(queries_fb),
        "followup_plan_queries": queries_fb,
        "followup_plan_queries_raw": raw_queries_fb,
        "followup_plan_rows_input_count": len(rows_payload),
        "followup_plan_perplexity_summary_chars": len(fallback_payload["perplexity_summary"]),
        "followup_plan_ru_anchors": list(FOLLOWUP_RU_ANCHORS),
        "followup_plan_selected_headlines_count": len(selected_headlines_fb),
        "followup_plan_selected_headlines": selected_headlines_fb,
        "followup_plan_prompt_tokens": int((usage or {}).get("prompt_tokens", 0) or 0)
        + int((usage_fb or {}).get("prompt_tokens", 0) or 0),
        "followup_plan_completion_tokens": int((usage or {}).get("completion_tokens", 0) or 0)
        + int((usage_fb or {}).get("completion_tokens", 0) or 0),
        "followup_plan_attempts": 2,
        "followup_plan_fallback_status": status_fb,
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


def _extract_with_retry(client: TavilyClient, payload: dict[str, Any]) -> dict[str, Any]:
    last_error: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            return client.extract(**payload)
        except (RequestException, Exception) as exc:
            last_error = exc
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"Tavily extract failed after retries: {last_error}") from last_error


def run_followup_queries(
    profile: PromptProfile,
    tavily_api_key: str,
    now_date: date,
    queries: list[str],
    credit_cap: float = DEFAULT_FOLLOWUP_CREDIT_CAP,
    max_results_per_query: int = DEFAULT_FOLLOWUP_MAX_RESULTS,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not queries:
        return [], {"followup_search_status": "skipped_no_queries", "followup_search_runs_debug": []}

    start_date = (now_date - timedelta(days=7)).isoformat()
    end_date = now_date.isoformat()
    start_d = date.fromisoformat(start_date)
    end_d = date.fromisoformat(end_date)

    client = TavilyClient(tavily_api_key)
    search_credits = 0.0
    extract_credits = 0.0
    runs_executed = 0
    errors: list[str] = []
    merged_by_url: dict[str, dict[str, Any]] = {}
    pending_no_date: dict[str, dict[str, Any]] = {}
    runs_debug: list[dict[str, Any]] = []

    for query in queries:
        # Advanced search typically costs 2 credits. Keep a hard margin by cap.
        if search_credits + 2.0 > float(credit_cap) + 1e-9:
            runs_debug.append(
                {
                    "query": short_query(query),
                    "status": "skipped_budget_cap",
                    "credits": 0.0,
                    "results_count": 0,
                    "top_results": [],
                }
            )
            break

        payload = {
            "query": short_query(query),
            "topic": "general",
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
            runs_debug.append(
                {
                    "query": short_query(query),
                    "status": "error",
                    "error": str(exc),
                    "credits": 0.0,
                    "results_count": 0,
                    "top_results": [],
                }
            )
            continue

        runs_executed += 1
        run_credits = float((response.get("usage") or {}).get("credits", 0))
        search_credits += run_credits
        run_results = response.get("results", []) or []
        runs_debug.append(
            {
                "query": short_query(query),
                "status": "ok",
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
        for item in run_results:
            url = item.get("url") or ""
            if not url:
                continue

            score = float(item.get("score", 0.0) or 0.0)

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

            base_candidate = {
                "title": str(item.get("title", "(Р±РµР· Р·Р°РіРѕР»РѕРІРєР°)")).strip() or "(Р±РµР· Р·Р°РіРѕР»РѕРІРєР°)",
                "content": str(item.get("content", "")),
                "url": str(url).strip(),
                "quality_score": score,
            }
            if effective_date:
                candidate = dict(base_candidate)
                candidate["date"] = effective_date
                old = merged_by_url.get(candidate["url"])
                if old is None or float(candidate["quality_score"]) > float(old.get("quality_score", 0.0)):
                    merged_by_url[candidate["url"]] = candidate
            else:
                old_pending = pending_no_date.get(base_candidate["url"])
                if old_pending is None or float(base_candidate["quality_score"]) > float(old_pending.get("quality_score", 0.0)):
                    pending_no_date[base_candidate["url"]] = base_candidate

    extract_urls_requested: list[str] = []
    extract_urls_extracted = 0
    if pending_no_date:
        pending_ranked = sorted(
            pending_no_date.values(),
            key=lambda x: float(x.get("quality_score", 0.0)),
            reverse=True,
        )
        extract_urls_requested = [
            str(item.get("url", "")).strip()
            for item in pending_ranked[:FOLLOWUP_EXTRACT_URL_LIMIT]
            if str(item.get("url", "")).strip()
        ]
        if extract_urls_requested:
            try:
                extract_response = _extract_with_retry(
                    client,
                    {
                        "urls": extract_urls_requested,
                        "extract_depth": "basic",
                        "format": "text",
                        "include_usage": True,
                    },
                )
                extract_credits = float((extract_response.get("usage") or {}).get("credits", 0))
                extract_urls_extracted = len(extract_response.get("results", []) or [])
                for entry in extract_response.get("results", []) or []:
                    url = str(entry.get("url", "")).strip()
                    if not url or url not in pending_no_date:
                        continue
                    raw = str(entry.get("raw_content", "") or entry.get("content", ""))
                    head = " ".join(raw.split())[:1800]
                    effective_date = ""
                    for cand in parse_date_candidates(head):
                        if start_d <= cand <= end_d:
                            effective_date = cand.isoformat()
                            break
                    if not effective_date:
                        continue
                    candidate = dict(pending_no_date[url])
                    candidate["date"] = effective_date
                    old = merged_by_url.get(url)
                    if old is None or float(candidate["quality_score"]) > float(old.get("quality_score", 0.0)):
                        merged_by_url[url] = candidate
            except Exception as exc:
                errors.append(f"extract: {exc}")

    ranked = sorted(
        merged_by_url.values(),
        key=lambda x: (x.get("date", ""), float(x.get("quality_score", 0.0))),
        reverse=True,
    )
    rows = [
        {
            "category": infer_category(f"{item.get('title', '')} {item.get('content', '')}", profile),
            "title": str(item.get("title", "(Р±РµР· Р·Р°РіРѕР»РѕРІРєР°)")).strip() or "(Р±РµР· Р·Р°РіРѕР»РѕРІРєР°)",
            "summary": build_summary(str(item.get("content", "")), str(item.get("title", ""))),
            "content": str(item.get("content", "")).strip(),
            "url": str(item.get("url", "")).strip(),
            "date": str(item.get("date", "")).strip(),
            "rank_score": f"{float(item.get('quality_score', 0.0) or 0.0):.6f}",
        }
        for item in ranked
    ]

    return rows, {
        "followup_search_status": "ok",
        "followup_search_queries_requested": len(queries),
        "followup_search_runs_executed": runs_executed,
        "followup_search_rows_added": len(rows),
        "followup_search_credits": search_credits + extract_credits,
        "followup_extract_credits": extract_credits,
        "followup_extract_urls_requested": len(extract_urls_requested),
        "followup_extract_urls_extracted": extract_urls_extracted,
        "followup_search_credit_cap": float(credit_cap),
        "followup_search_errors_count": len(errors),
        "followup_search_errors": errors[:20],
        "followup_search_runs_debug": runs_debug,
    }


def merge_digest_rows(base_rows: list[dict[str, str]], extra_rows: list[dict[str, str]], limit: int = 15) -> list[dict[str, str]]:
    def _score(row: dict[str, str]) -> float:
        try:
            return float(row.get("rank_score", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    merged_by_url: dict[str, dict[str, str]] = {}
    for row in base_rows + extra_rows:
        url = str(row.get("url", "")).strip()
        if not url:
            continue
        old = merged_by_url.get(url)
        if old is None:
            merged_by_url[url] = row
            continue
        # Prefer row with later date; for same date prefer higher rank score.
        old_date = str(old.get("date", ""))
        new_date = str(row.get("date", ""))
        if new_date > old_date:
            merged_by_url[url] = row
        elif new_date == old_date and _score(row) > _score(old):
            merged_by_url[url] = row
        elif (
            new_date == old_date
            and _score(row) == _score(old)
            and len(str(row.get("summary", ""))) > len(str(old.get("summary", "")))
        ):
            merged_by_url[url] = row

    merged = sorted(
        merged_by_url.values(),
        key=lambda x: (_score(x), str(x.get("date", ""))),
        reverse=True,
    )
    return merged[:limit]

