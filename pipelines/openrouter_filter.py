from __future__ import annotations

import json
import re
from typing import Any

import requests


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "openai/gpt-4.1-mini"


def _build_system_prompt(prompt_text: str) -> str:
    compact_prompt = " ".join((prompt_text or "").split())
    if len(compact_prompt) > 5000:
        compact_prompt = compact_prompt[:5000] + "..."
    return (
        "Ты фильтр юридической новостной сводки.\n"
        "Ниже дана спецификация тематики из исходного промпта.\n"
        "Оставь только новости, которые соответствуют этой тематике.\n"
        "Удаляй дубликаты (одинаковые или почти одинаковые события из разных источников).\n"
        "Не придумывай новые новости и не меняй факты.\n"
        "Ответ верни СТРОГО в JSON-формате без markdown:\n"
        '{"keep_indices":[1,2,3]}\n'
        "Индексы 1-based и относятся к списку материалов, который придет от пользователя.\n"
        "Если ничего не подходит, верни:\n"
        '{"keep_indices":[]}\n\n'
        f"ТЕМАТИКА ИЗ ПРОМПТА:\n{compact_prompt}"
    )


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
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        return None
    return None


def filter_digest_rows_with_openrouter(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if not rows:
        return rows, {"openrouter_filter_status": "skipped_empty"}
    if not openrouter_api_key:
        return rows, {"openrouter_filter_status": "skipped_no_key"}

    payload_rows = [
        {
            "index": idx,
            "category": row.get("category", ""),
            "title": row.get("title", ""),
            "summary": row.get("summary", ""),
            "url": row.get("url", ""),
            "date": row.get("date", ""),
        }
        for idx, row in enumerate(rows, start=1)
    ]

    payload = {
        "model": model or DEFAULT_OPENROUTER_MODEL,
        "temperature": 0.1,
        "max_tokens": 700,
        "messages": [
            {"role": "system", "content": _build_system_prompt(prompt_text)},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "materials": payload_rows,
                        "task": "оставь только релевантные теме материалы и убери дубликаты",
                    },
                    ensure_ascii=False,
                ),
            },
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
        return rows, {"openrouter_filter_status": f"error_request: {exc}"}

    content = (
        ((data.get("choices") or [{}])[0].get("message") or {}).get("content", "")
        if isinstance(data, dict)
        else ""
    )
    parsed = _extract_json_from_text(str(content))
    if not parsed:
        return rows, {"openrouter_filter_status": "error_parse_response"}

    indices_raw = parsed.get("keep_indices")
    if not isinstance(indices_raw, list):
        return rows, {"openrouter_filter_status": "error_invalid_schema"}

    keep_indices: list[int] = []
    seen: set[int] = set()
    for value in indices_raw:
        try:
            idx = int(value)
        except (TypeError, ValueError):
            continue
        if idx < 1 or idx > len(rows):
            continue
        if idx in seen:
            continue
        seen.add(idx)
        keep_indices.append(idx)
    keep_indices.sort()

    filtered_rows = [rows[idx - 1] for idx in keep_indices]
    usage = data.get("usage") if isinstance(data, dict) else {}
    return filtered_rows, {
        "openrouter_filter_status": "ok",
        "openrouter_filter_model": model or DEFAULT_OPENROUTER_MODEL,
        "openrouter_filter_input_items": len(rows),
        "openrouter_filter_output_items": len(filtered_rows),
        "openrouter_filter_removed_items": len(rows) - len(filtered_rows),
        "openrouter_prompt_tokens": (usage or {}).get("prompt_tokens", 0),
        "openrouter_completion_tokens": (usage or {}).get("completion_tokens", 0),
    }
