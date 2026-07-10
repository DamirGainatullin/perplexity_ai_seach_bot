from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests

from core.env import read_text_with_fallback


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "openai/gpt-4.1-mini"
_OPENROUTER_PROXY_URL = ""

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_PROMPTS_DIR = BASE_DIR / "prompts" / "openrouter"

LEGACY_PROMPT_FILE = "legacy_filter.txt"
STAGE1_PROMPT_FILE = "stage1_filter_dedup.txt"
STAGE2_PROMPT_FILE = "stage2_summarize.txt"
STAGE3_PROMPT_FILE = "stage3_finalize.txt"
DAILY_STAGE1_PROMPT_FILE = "daily_stage1_filter_followup.txt"
DAILY_STAGE3_PROMPT_FILE = "daily_stage3_finalize.txt"

DEFAULT_LEGACY_PROMPT_TEMPLATE = """
Ты фильтр релевантности для юридической новостной сводки.
Используй профильную тематику из PROFILE_TOPIC_SPEC.

Требования:
- Оставь только релевантные материалы.
- Удали дубли и почти-дубли.
- Не выдумывай факты и не искажай исходные данные.

Верни строго JSON без markdown:
{"keep_indices":[1,2,3]}
Индексы 1-based и относятся к входному списку materials.
Если ничего не подходит, верни:
{"keep_indices":[]}

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE1_PROMPT_TEMPLATE = """
Ты этап 1 трёхэтапного редакционного контура юридической сводки.
Задача: первичная фильтрация и дедупликация.

Требования:
1) Оставь только материалы, релевантные тематике PROFILE_TOPIC_SPEC.
2) Удали дубли и почти-дубли (одно и то же событие из разных источников).
3) По возможности сохрани разнообразие источников.
4) На этом этапе не пересказывай и не переписывай тексты.
5) Не выдумывай факты.

Верни строго JSON без markdown:
{"keep_indices":[1,2,3]}
Индексы 1-based и относятся к входному списку materials.
Если ничего не подходит, верни: {"keep_indices":[]}.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE2_PROMPT_TEMPLATE = """
Ты этап 2 трёхэтапного редакционного контура юридической сводки.
Задача: суммаризация (без дополнительной фильтрации).

Для каждого входного материала подготовь краткое, но полноценное резюме.
Сохраняй ключевые юридические факты: субъект, действие, дату, номера документов,
обязанности, запреты, ответственность, санкции (если есть).

Правила:
- Не выдумывай факты.
- Стиль нейтральный, юридически аккуратный.
- Один summary на один index.
- Целевая длина summary: 220-450 символов.
- Если фактов много, оставь только самые важные для понимания сути новости.
- Не растягивай summary вводными фразами и повторениями.

Верни строго JSON:
{"items":[{"index":1,"summary":"..."}]}
Если по элементу нельзя уверенно сделать summary, пропусти его.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE3_PROMPT_TEMPLATE = """
Ты этап 3 трёхэтапного редакционного контура юридической сводки.
Задача: финальная фильтрация и подготовка карточек к выдаче.

Действия:
1) Ещё раз проверь релевантность PROFILE_TOPIC_SPEC.
2) Удали слабые по релевантности остатки.
3) Проставь каждой новости короткую юридическую категорию.
4) При необходимости переформулируй заголовок и summary для ясности без изменения фактов.
5) Удали оставшиеся дубли.
6) Summary должен остаться кратким: обычно 220-450 символов, без повторов и длинных перечислений.

Верни строго JSON:
{"items":[{"index":1,"category":"...","title":"...","summary":"..."}]}
Индексы 1-based относительно входного списка данного этапа.
Если оставлять нечего, верни: {"items":[]}.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_DAILY_STAGE1_PROMPT_TEMPLATE = """
Ты этап 1 ежедневного редакционного контура юридической сводки.
Отфильтруй материалы по PROFILE_TOPIC_SPEC, удали дубли и при наличии конкретного
свежего информационного повода составь ровно один уточняющий поисковый запрос.
Оставь максимум 12 наиболее значимых материалов.

Не оставляй материалы, если из текста явно следует, что публикация старая. Дата
будущего вступления нормы в силу не является датой публикации. Не пересказывай тексты.
Уточняющий запрос должен быть на русском языке, относиться к России, содержать предмет
события, ведомство или номер документа, если они известны. Не используй операторы site:
и не ограничивай запрос доменами. Если конкретного повода нет, верни null.

Верни строго JSON без markdown:
{"keep_indices":[1,2],"followup_query":"... или null"}

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_DAILY_STAGE3_PROMPT_TEMPLATE = """
Ты этап 3 ежедневного редакционного контура юридической сводки.
Выполни финальную проверку релевантности PROFILE_TOPIC_SPEC, подготовь категории,
заголовки и резюме. Удали дубли внутри materials и смысловые повторы событий из
recently_sent, даже если ссылки различаются. Не удаляй реальное продолжение события,
если появились новые юридически значимые факты.

Материал со старой подтверждённой датой не оставляй. Если date пустая, оцени свежесть
только по фактам исходного текста и ничего не выдумывай. Summary: 220-450 символов.

Верни строго JSON:
{"items":[{"index":1,"category":"...","title":"...","summary":"..."}]}
Если оставлять нечего, верни {"items":[]}.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()


def configure_openrouter_proxy(proxy_url: str) -> None:
    global _OPENROUTER_PROXY_URL
    _OPENROUTER_PROXY_URL = (proxy_url or "").strip()


def get_openrouter_proxies() -> dict[str, str] | None:
    if not _OPENROUTER_PROXY_URL:
        return None
    return {
        "http": _OPENROUTER_PROXY_URL,
        "https": _OPENROUTER_PROXY_URL,
    }


def _compact_prompt(prompt_text: str, max_len: int = 7000) -> str:
    compact = " ".join((prompt_text or "").split())
    if len(compact) <= max_len:
        return compact
    return compact[: max_len - 3] + "..."


def _normalize_text(value: Any, max_len: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 3].rstrip() + "..."


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


def _resolve_prompt_template(prompts_dir: Path, file_name: str, default_template: str) -> tuple[str, str]:
    path = prompts_dir / file_name
    if path.exists():
        try:
            return read_text_with_fallback(path), str(path)
        except Exception:
            return default_template, f"default:{file_name}"
    return default_template, f"default:{file_name}"


def _render_template(template: str, prompt_text: str) -> str:
    return template.replace("{{PROFILE_PROMPT}}", _compact_prompt(prompt_text))


def _build_material_payload(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        payload.append(
            {
                "index": idx,
                "category": _normalize_text(row.get("category", ""), 180),
                "title": _normalize_text(row.get("title", ""), 400),
                "summary": _normalize_text(row.get("summary", ""), 1500),
                "content": _normalize_text(row.get("content", row.get("summary", "")), 3000),
                "url": str(row.get("url", "")).strip(),
                "date": str(row.get("date", "")).strip(),
                "date_confidence": str(row.get("date_confidence", "")).strip(),
                "source": str(row.get("source", "")).strip(),
            }
        )
    return payload


def _build_history_payload(rows: list[dict[str, str]], limit: int = 40) -> list[dict[str, str]]:
    payload: list[dict[str, str]] = []
    for row in rows[:limit]:
        payload.append(
            {
                "title": _normalize_text(row.get("title", ""), 300),
                "summary": _normalize_text(row.get("summary", ""), 350),
                "url": str(row.get("url", "")).strip(),
                "date": str(row.get("date", "")).strip(),
            }
        )
    return payload


def _is_telegram_url(url: str) -> bool:
    value = (url or "").strip().lower()
    return value.startswith("https://t.me/") or value.startswith("http://t.me/")


def _ensure_web_presence(
    final_rows: list[dict[str, str]],
    source_rows: list[dict[str, str]],
    *,
    min_web_rows: int = 1,
    limit: int = 15,
) -> tuple[list[dict[str, str]], int]:
    if not final_rows:
        return final_rows, 0
    current_web = [row for row in final_rows if not _is_telegram_url(str(row.get("url", "")))]
    if len(current_web) >= min_web_rows:
        return final_rows, 0

    existing_urls = {str(row.get("url", "")).strip() for row in final_rows if str(row.get("url", "")).strip()}
    candidates = [
        row
        for row in source_rows
        if (not _is_telegram_url(str(row.get("url", ""))))
        and (str(row.get("url", "")).strip() not in existing_urls)
        and str(row.get("url", "")).strip()
    ]
    if not candidates:
        return final_rows, 0

    candidates.sort(key=lambda x: str(x.get("date", "")), reverse=True)
    need = max(0, min_web_rows - len(current_web))
    additions = candidates[:need]
    if not additions:
        return final_rows, 0

    merged = list(final_rows) + additions
    merged.sort(key=lambda x: str(x.get("date", "")), reverse=True)
    return merged[:limit], len(additions)


def _call_openrouter_json(
    *,
    system_prompt: str,
    user_payload: dict[str, Any],
    openrouter_api_key: str,
    model: str,
    max_tokens: int,
    timeout_sec: int,
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
        "X-OpenRouter-Metadata": "enabled",
    }

    request_kwargs: dict[str, Any] = {
        "url": OPENROUTER_URL,
        "json": payload,
        "headers": headers,
        "timeout": timeout_sec,
    }
    proxies = get_openrouter_proxies()
    if proxies:
        request_kwargs["proxies"] = proxies

    try:
        response = requests.post(**request_kwargs)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
        response_obj = getattr(exc, "response", None)
        if response_obj is not None:
            status_code = getattr(response_obj, "status_code", "unknown")
            response_body = " ".join(str(getattr(response_obj, "text", "") or "").split())[:1500]
            return None, {}, f"error_request: status={status_code} body={response_body}"
        return None, {}, f"error_request: {exc}"

    usage = data.get("usage") if isinstance(data, dict) else {}
    content = (
        ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        if isinstance(data, dict)
        else ""
    )
    parsed = _extract_json_from_text(str(content))
    if not parsed:
        return None, usage or {}, "error_parse_response"
    return parsed, usage or {}, "ok"


def filter_digest_rows_with_openrouter(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
    prompts_dir: Path | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return rows, {"openrouter_filter_status": "skipped_empty"}
    if not openrouter_api_key:
        return rows, {"openrouter_filter_status": "skipped_no_key"}

    resolved_prompts_dir = prompts_dir or DEFAULT_PROMPTS_DIR
    template, prompt_path = _resolve_prompt_template(
        resolved_prompts_dir,
        LEGACY_PROMPT_FILE,
        DEFAULT_LEGACY_PROMPT_TEMPLATE,
    )
    system_prompt = _render_template(template, prompt_text)

    parsed, usage, status = _call_openrouter_json(
        system_prompt=system_prompt,
        user_payload={
            "materials": _build_material_payload(rows),
            "task": "keep only relevant materials and remove duplicates",
            "schema": {"keep_indices": [1, 2]},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=700,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return rows, {
            "openrouter_filter_status": status,
            "openrouter_filter_prompt_file": prompt_path,
        }

    indices_raw = parsed.get("keep_indices")
    if not isinstance(indices_raw, list):
        return rows, {
            "openrouter_filter_status": "error_invalid_schema",
            "openrouter_filter_prompt_file": prompt_path,
        }

    keep_indices: list[int] = []
    seen: set[int] = set()
    for value in indices_raw:
        try:
            idx = int(value)
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows) or idx in seen:
            continue
        seen.add(idx)
        keep_indices.append(idx)
    keep_indices.sort()

    filtered_rows = [rows[idx - 1] for idx in keep_indices]
    return filtered_rows, {
        "openrouter_filter_status": "ok",
        "openrouter_filter_model": model or DEFAULT_OPENROUTER_MODEL,
        "openrouter_filter_prompt_file": prompt_path,
        "openrouter_filter_input_items": len(rows),
        "openrouter_filter_output_items": len(filtered_rows),
        "openrouter_filter_removed_items": len(rows) - len(filtered_rows),
        "openrouter_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def _stage1_filter_rows(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str,
    timeout_sec: int,
    prompts_dir: Path,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    template, prompt_path = _resolve_prompt_template(prompts_dir, STAGE1_PROMPT_FILE, DEFAULT_STAGE1_PROMPT_TEMPLATE)
    system_prompt = _render_template(template, prompt_text)

    parsed, usage, status = _call_openrouter_json(
        system_prompt=system_prompt,
        user_payload={
            "materials": _build_material_payload(rows),
            "task": "primary filtering by topic relevance and deduplication",
            "schema": {"keep_indices": [1, 2]},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=900,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return rows, {
            "openrouter_stage1_status": status,
            "openrouter_stage1_prompt_file": prompt_path,
        }

    keep_raw = parsed.get("keep_indices")
    if not isinstance(keep_raw, list):
        return rows, {
            "openrouter_stage1_status": "error_invalid_schema",
            "openrouter_stage1_prompt_file": prompt_path,
        }

    keep_indices: list[int] = []
    seen: set[int] = set()
    for value in keep_raw:
        try:
            idx = int(value)
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows) or idx in seen:
            continue
        seen.add(idx)
        keep_indices.append(idx)
    keep_indices.sort()

    filtered = [rows[idx - 1] for idx in keep_indices]
    return filtered, {
        "openrouter_stage1_status": "ok",
        "openrouter_stage1_prompt_file": prompt_path,
        "openrouter_stage1_input_items": len(rows),
        "openrouter_stage1_output_items": len(filtered),
        "openrouter_stage1_removed_items": len(rows) - len(filtered),
        "openrouter_stage1_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_stage1_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def _stage2_summarize_rows(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str,
    timeout_sec: int,
    prompts_dir: Path,
    max_summary_chars: int,
    max_tokens: int = 2200,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return rows, {"openrouter_stage2_status": "skipped_empty"}

    template, prompt_path = _resolve_prompt_template(prompts_dir, STAGE2_PROMPT_FILE, DEFAULT_STAGE2_PROMPT_TEMPLATE)
    system_prompt = _render_template(template, prompt_text)

    parsed, usage, status = _call_openrouter_json(
        system_prompt=system_prompt,
        user_payload={
            "materials": _build_material_payload(rows),
            "task": "produce complete concise summaries for each item",
            "schema": {"items": [{"index": 1, "summary": "..."}]},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=max_tokens,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return rows, {
            "openrouter_stage2_status": status,
            "openrouter_stage2_prompt_file": prompt_path,
        }

    items_raw = parsed.get("items")
    if not isinstance(items_raw, list):
        return rows, {
            "openrouter_stage2_status": "error_invalid_schema",
            "openrouter_stage2_prompt_file": prompt_path,
        }

    summary_by_index: dict[int, str] = {}
    for item in items_raw:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows):
            continue
        summary = _normalize_text(item.get("summary", ""), max_summary_chars).strip()
        if not summary:
            continue
        summary_by_index[idx] = summary

    updated: list[dict[str, str]] = []
    for idx, row in enumerate(rows, start=1):
        row_copy = dict(row)
        if idx in summary_by_index:
            row_copy["summary"] = summary_by_index[idx]
        updated.append(row_copy)

    return updated, {
        "openrouter_stage2_status": "ok",
        "openrouter_stage2_prompt_file": prompt_path,
        "openrouter_stage2_input_items": len(rows),
        "openrouter_stage2_updated_items": len(summary_by_index),
        "openrouter_stage2_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_stage2_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def _stage3_finalize_rows(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str,
    timeout_sec: int,
    prompts_dir: Path,
    max_summary_chars: int,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return rows, {"openrouter_stage3_status": "skipped_empty"}

    template, prompt_path = _resolve_prompt_template(prompts_dir, STAGE3_PROMPT_FILE, DEFAULT_STAGE3_PROMPT_TEMPLATE)
    system_prompt = _render_template(template, prompt_text)

    parsed, usage, status = _call_openrouter_json(
        system_prompt=system_prompt,
        user_payload={
            "materials": _build_material_payload(rows),
            "task": "final relevance check, categorization, and digest-ready formatting",
            "schema": {"items": [{"index": 1, "category": "...", "title": "...", "summary": "..."}]},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=2600,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return rows, {
            "openrouter_stage3_status": status,
            "openrouter_stage3_prompt_file": prompt_path,
        }

    items_raw = parsed.get("items")
    if not isinstance(items_raw, list):
        return rows, {
            "openrouter_stage3_status": "error_invalid_schema",
            "openrouter_stage3_prompt_file": prompt_path,
        }

    final_rows: list[dict[str, str]] = []
    seen_indices: set[int] = set()
    for item in items_raw:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows) or idx in seen_indices:
            continue
        seen_indices.add(idx)

        base = dict(rows[idx - 1])
        category = _normalize_text(item.get("category", ""), 160).strip()
        title = _normalize_text(item.get("title", ""), 260).strip()
        summary = _normalize_text(item.get("summary", ""), max_summary_chars).strip()
        if category:
            base["category"] = category
        if title:
            base["title"] = title
        if summary:
            base["summary"] = summary
        final_rows.append(base)

    return final_rows, {
        "openrouter_stage3_status": "ok",
        "openrouter_stage3_prompt_file": prompt_path,
        "openrouter_stage3_input_items": len(rows),
        "openrouter_stage3_output_items": len(final_rows),
        "openrouter_stage3_removed_items": len(rows) - len(final_rows),
        "openrouter_stage3_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_stage3_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def run_daily_stage1_openrouter(
    rows: list[dict[str, str]],
    prompt_text: str,
    recent_history: list[dict[str, str]],
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
    prompts_dir: Path | None = None,
) -> tuple[list[dict[str, str]], str, dict[str, Any]]:
    if not rows:
        return [], "", {"openrouter_stage1_status": "skipped_empty"}
    if not openrouter_api_key:
        return [], "", {"openrouter_stage1_status": "error_no_key"}

    resolved_prompts_dir = prompts_dir or DEFAULT_PROMPTS_DIR
    template, prompt_path = _resolve_prompt_template(
        resolved_prompts_dir,
        DAILY_STAGE1_PROMPT_FILE,
        DEFAULT_DAILY_STAGE1_PROMPT_TEMPLATE,
    )
    parsed, usage, status = _call_openrouter_json(
        system_prompt=_render_template(template, prompt_text),
        user_payload={
            "materials": _build_material_payload(rows),
            "recently_sent": _build_history_payload(recent_history),
            "task": "filter daily materials, remove duplicates, and optionally create one follow-up query",
            "schema": {"keep_indices": [1, 2], "followup_query": "string or null"},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=1100,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return [], "", {
            "openrouter_stage1_status": status,
            "openrouter_stage1_prompt_file": prompt_path,
        }

    keep_raw = parsed.get("keep_indices")
    if not isinstance(keep_raw, list):
        return [], "", {
            "openrouter_stage1_status": "error_invalid_schema",
            "openrouter_stage1_prompt_file": prompt_path,
        }

    keep_indices: list[int] = []
    seen: set[int] = set()
    for value in keep_raw:
        try:
            idx = int(value)
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows) or idx in seen:
            continue
        seen.add(idx)
        keep_indices.append(idx)
    keep_indices.sort()
    keep_indices = keep_indices[:12]

    raw_query = parsed.get("followup_query")
    followup_query = _normalize_text(raw_query, 390).strip() if isinstance(raw_query, str) else ""
    followup_query = " ".join(re.sub(r"\bsite:\S+", " ", followup_query, flags=re.IGNORECASE).split())
    if followup_query.lower() in {"null", "none", "нет", "не требуется"}:
        followup_query = ""

    filtered = [rows[idx - 1] for idx in keep_indices]
    return filtered, followup_query, {
        "openrouter_stage1_status": "ok",
        "openrouter_stage1_prompt_file": prompt_path,
        "openrouter_stage1_input_items": len(rows),
        "openrouter_stage1_output_items": len(filtered),
        "openrouter_stage1_removed_items": len(rows) - len(filtered),
        "openrouter_stage1_keep_indices": keep_indices,
        "openrouter_stage1_followup_query": followup_query,
        "openrouter_stage1_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_stage1_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def _daily_stage3_finalize_rows(
    rows: list[dict[str, str]],
    prompt_text: str,
    recent_history: list[dict[str, str]],
    openrouter_api_key: str,
    model: str,
    timeout_sec: int,
    prompts_dir: Path,
    max_summary_chars: int,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return [], {"openrouter_stage3_status": "skipped_empty"}

    template, prompt_path = _resolve_prompt_template(
        prompts_dir,
        DAILY_STAGE3_PROMPT_FILE,
        DEFAULT_DAILY_STAGE3_PROMPT_TEMPLATE,
    )
    parsed, usage, status = _call_openrouter_json(
        system_prompt=_render_template(template, prompt_text),
        user_payload={
            "materials": _build_material_payload(rows),
            "recently_sent": _build_history_payload(recent_history),
            "task": "final daily relevance check, historical deduplication, categorization, and formatting",
            "schema": {"items": [{"index": 1, "category": "...", "title": "...", "summary": "..."}]},
        },
        openrouter_api_key=openrouter_api_key,
        model=model,
        max_tokens=2600,
        timeout_sec=timeout_sec,
    )
    if status != "ok" or parsed is None:
        return rows, {
            "openrouter_stage3_status": status,
            "openrouter_stage3_prompt_file": prompt_path,
        }

    items_raw = parsed.get("items")
    if not isinstance(items_raw, list):
        return rows, {
            "openrouter_stage3_status": "error_invalid_schema",
            "openrouter_stage3_prompt_file": prompt_path,
        }

    final_rows: list[dict[str, str]] = []
    seen_indices: set[int] = set()
    for item in items_raw:
        if not isinstance(item, dict):
            continue
        try:
            idx = int(item.get("index"))
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows) or idx in seen_indices:
            continue
        seen_indices.add(idx)
        base = dict(rows[idx - 1])
        category = _normalize_text(item.get("category", ""), 160).strip()
        title = _normalize_text(item.get("title", ""), 260).strip()
        summary = _normalize_text(item.get("summary", ""), max_summary_chars).strip()
        if category:
            base["category"] = category
        if title:
            base["title"] = title
        if summary:
            base["summary"] = summary
        final_rows.append(base)

    return final_rows, {
        "openrouter_stage3_status": "ok",
        "openrouter_stage3_prompt_file": prompt_path,
        "openrouter_stage3_input_items": len(rows),
        "openrouter_stage3_output_items": len(final_rows),
        "openrouter_stage3_removed_items": len(rows) - len(final_rows),
        "openrouter_stage3_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_stage3_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }


def run_daily_stage2_stage3_openrouter(
    rows: list[dict[str, str]],
    prompt_text: str,
    recent_history: list[dict[str, str]],
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
    prompts_dir: Path | None = None,
    max_summary_chars: int = 420,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return [], {
            "openrouter_pipeline_mode": "daily_three_stage",
            "openrouter_pipeline_status": "skipped_empty",
        }
    if not openrouter_api_key:
        return [], {
            "openrouter_pipeline_mode": "daily_three_stage",
            "openrouter_pipeline_status": "error_no_key",
        }

    resolved_prompts_dir = prompts_dir or DEFAULT_PROMPTS_DIR
    stage2_rows, stage2_usage = _stage2_summarize_rows(
        rows,
        prompt_text,
        openrouter_api_key,
        model,
        timeout_sec,
        resolved_prompts_dir,
        max_summary_chars,
        3200,
    )
    current_rows = stage2_rows if stage2_usage.get("openrouter_stage2_status") == "ok" else rows
    stage3_rows, stage3_usage = _daily_stage3_finalize_rows(
        current_rows,
        prompt_text,
        recent_history,
        openrouter_api_key,
        model,
        timeout_sec,
        resolved_prompts_dir,
        max_summary_chars,
    )
    if stage3_usage.get("openrouter_stage3_status") == "ok":
        current_rows = stage3_rows

    usage: dict[str, Any] = {
        "openrouter_pipeline_mode": "daily_three_stage",
        "openrouter_pipeline_status": (
            "ok"
            if stage2_usage.get("openrouter_stage2_status") == "ok"
            and stage3_usage.get("openrouter_stage3_status") == "ok"
            else "partial"
        ),
        "openrouter_filter_model": model or DEFAULT_OPENROUTER_MODEL,
        "openrouter_filter_output_items": len(current_rows),
        "openrouter_stage2_output_rows": stage2_rows,
        "openrouter_stage3_output_rows": current_rows,
    }
    usage.update(stage2_usage)
    usage.update(stage3_usage)
    return current_rows, usage


def run_three_stage_openrouter_pipeline(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
    prompts_dir: Path | None = None,
    max_summary_chars: int = 420,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return rows, {
            "openrouter_pipeline_mode": "three_stage",
            "openrouter_pipeline_status": "skipped_empty",
            "openrouter_filter_status": "skipped_empty",
        }
    if not openrouter_api_key:
        return rows, {
            "openrouter_pipeline_mode": "three_stage",
            "openrouter_pipeline_status": "skipped_no_key",
            "openrouter_filter_status": "skipped_no_key",
        }

    resolved_prompts_dir = prompts_dir or DEFAULT_PROMPTS_DIR
    current_rows = rows

    stage1_rows, stage1_usage = _stage1_filter_rows(
        current_rows,
        prompt_text,
        openrouter_api_key,
        model,
        timeout_sec,
        resolved_prompts_dir,
    )
    if stage1_usage.get("openrouter_stage1_status") == "ok":
        current_rows = stage1_rows

    stage2_rows, stage2_usage = _stage2_summarize_rows(
        current_rows,
        prompt_text,
        openrouter_api_key,
        model,
        timeout_sec,
        resolved_prompts_dir,
        max_summary_chars=max_summary_chars,
    )
    if stage2_usage.get("openrouter_stage2_status") == "ok":
        current_rows = stage2_rows

    pre_stage3_rows = list(current_rows)
    stage3_rows, stage3_usage = _stage3_finalize_rows(
        current_rows,
        prompt_text,
        openrouter_api_key,
        model,
        timeout_sec,
        resolved_prompts_dir,
        max_summary_chars=max_summary_chars,
    )
    if stage3_usage.get("openrouter_stage3_status") == "ok":
        current_rows = stage3_rows

    usage: dict[str, Any] = {
        "openrouter_pipeline_mode": "three_stage",
        "openrouter_pipeline_input_items": len(rows),
    }
    usage.update(stage1_usage)
    usage.update(stage2_usage)
    usage.update(stage3_usage)

    stage_statuses = [
        str(stage1_usage.get("openrouter_stage1_status", "")),
        str(stage2_usage.get("openrouter_stage2_status", "")),
        str(stage3_usage.get("openrouter_stage3_status", "")),
    ]
    ok_count = sum(1 for status in stage_statuses if status == "ok")
    error_count = sum(1 for status in stage_statuses if status.startswith("error_"))

    if ok_count == 0:
        legacy_rows, legacy_usage = filter_digest_rows_with_openrouter(
            rows,
            prompt_text,
            openrouter_api_key,
            model=model,
            timeout_sec=timeout_sec,
            prompts_dir=resolved_prompts_dir,
        )
        usage["openrouter_pipeline_status"] = "fallback_legacy"
        usage["openrouter_pipeline_fallback"] = "legacy_filter"
        legacy_rows, legacy_diversity_added = _ensure_web_presence(legacy_rows, rows, min_web_rows=0, limit=15)
        usage["openrouter_source_diversity_added_web"] = legacy_diversity_added
        for key, value in legacy_usage.items():
            usage[f"openrouter_legacy_{key}"] = value
        usage.update(legacy_usage)
        return legacy_rows, usage

    if error_count == 0:
        pipeline_status = "ok"
    elif ok_count > 0:
        pipeline_status = "partial"
    else:
        pipeline_status = "error"

    current_rows, diversity_added = _ensure_web_presence(
        current_rows,
        rows,
        min_web_rows=0,
        limit=15,
    )
    usage["openrouter_source_diversity_added_web"] = diversity_added

    total_prompt_tokens = int(stage1_usage.get("openrouter_stage1_prompt_tokens", 0) or 0) + int(
        stage2_usage.get("openrouter_stage2_prompt_tokens", 0) or 0
    ) + int(stage3_usage.get("openrouter_stage3_prompt_tokens", 0) or 0)
    total_completion_tokens = int(stage1_usage.get("openrouter_stage1_completion_tokens", 0) or 0) + int(
        stage2_usage.get("openrouter_stage2_completion_tokens", 0) or 0
    ) + int(stage3_usage.get("openrouter_stage3_completion_tokens", 0) or 0)

    usage.update(
        {
            "openrouter_pipeline_status": pipeline_status,
            "openrouter_filter_status": "ok",
            "openrouter_filter_model": model or DEFAULT_OPENROUTER_MODEL,
            "openrouter_filter_input_items": len(rows),
            "openrouter_filter_output_items": len(current_rows),
            "openrouter_filter_removed_items": len(rows) - len(current_rows),
            "openrouter_prompt_tokens": total_prompt_tokens,
            "openrouter_completion_tokens": total_completion_tokens,
        }
    )
    return current_rows, usage
