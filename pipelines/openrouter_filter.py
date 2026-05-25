from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import requests

from core.env import read_text_with_fallback


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "openai/gpt-4.1-mini"

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_PROMPTS_DIR = BASE_DIR / "prompts" / "openrouter"

LEGACY_PROMPT_FILE = "legacy_filter.txt"
STAGE1_PROMPT_FILE = "stage1_filter_dedup.txt"
STAGE2_PROMPT_FILE = "stage2_summarize.txt"
STAGE3_PROMPT_FILE = "stage3_finalize.txt"

DEFAULT_LEGACY_PROMPT_TEMPLATE = """
You are a strict relevance filter for a legal and regulatory news digest.
Use the profile topic specification below.
Keep only materials relevant to the topic and remove duplicates or near-duplicates.
Do not invent facts. Do not change source facts.
Return STRICT JSON only (no markdown):
{"keep_indices":[1,2,3]}
Indices are 1-based and refer to the input materials list.
If nothing is relevant, return:
{"keep_indices":[]}

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE1_PROMPT_TEMPLATE = """
You are Stage 1 of a 3-stage editorial pipeline for legal news.
Task: primary filtering and deduplication only.
1. Keep only materials relevant to PROFILE_TOPIC_SPEC.
2. Remove exact duplicates and near-duplicates.
3. Keep source diversity when possible.
4. Do NOT summarize or rewrite content here.
5. Do NOT invent facts.
Return STRICT JSON only (no markdown):
{"keep_indices":[1,2,3]}
Indices are 1-based and refer to input materials.
If nothing is relevant, return {"keep_indices":[]}.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE2_PROMPT_TEMPLATE = """
You are Stage 2 of a 3-stage editorial pipeline.
Task: summarization only (no filtering).
For each material, produce a complete concise summary preserving key legal facts, actors,
dates, numbers, and obligations/sanctions if present.
Rules:
- Do not invent facts.
- Keep neutral legal style.
- One summary per input index.
- Target length: 350-900 characters per summary.
Return STRICT JSON only:
{"items":[{"index":1,"summary":"..."}]}
If some item cannot be summarized confidently, omit it from items.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()

DEFAULT_STAGE3_PROMPT_TEMPLATE = """
You are Stage 3 of a 3-stage editorial pipeline.
Task: final editorial filtering and presentation shaping.
Input already passed primary filter and has draft summaries.
Actions:
1. Do final relevance check against PROFILE_TOPIC_SPEC.
2. Optionally remove leftovers that are weakly relevant.
3. Group/classify each kept item into a concise legal category.
4. Rewrite title/summary for clarity without changing facts.
5. Remove remaining duplicates.
Return STRICT JSON only:
{"items":[{"index":1,"category":"...","title":"...","summary":"..."}]}
Indices are 1-based against the input list of this stage.
If nothing should be kept, return {"items":[]}.

PROFILE_TOPIC_SPEC:
{{PROFILE_PROMPT}}
""".strip()


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
            }
        )
    return payload


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
    }

    try:
        response = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout_sec)
        response.raise_for_status()
        data = response.json()
    except Exception as exc:
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
        max_tokens=2200,
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


def run_three_stage_openrouter_pipeline(
    rows: list[dict[str, str]],
    prompt_text: str,
    openrouter_api_key: str,
    model: str = DEFAULT_OPENROUTER_MODEL,
    timeout_sec: int = 60,
    prompts_dir: Path | None = None,
    max_summary_chars: int = 900,
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
