from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SlotConfig:
    hour: int
    minute: int
    before_min: int = 10
    after_min: int = 45


PROFILE_SLOTS: dict[str, SlotConfig | None] = {
    "logistics": SlotConfig(hour=18, minute=0, before_min=5, after_min=10),
    "metanol": SlotConfig(hour=18, minute=30, before_min=5, after_min=10),
    "precursors": SlotConfig(hour=19, minute=0, before_min=5, after_min=10),
    "chesny_znak": None,
    # Placeholder for future schedule setup.
    "rop": None,
    "sales": None,
    "spot": None,
}
PERPLEXITY_ALLOWED_SENDER = "team@mail.perplexity.ai"
PERPLEXITY_SEED_DAYS_BACK = 7
PERPLEXITY_SEED_FETCH_MAX_RESULTS = 100


def _resolve_moscow_tz():
    try:
        return ZoneInfo("Europe/Moscow")
    except Exception:
        return timezone(timedelta(hours=3))


def is_perplexity_followup_enabled_for_profile(profile_name: str) -> bool:
    profile_key = (profile_name or "").strip().lower()
    if profile_key not in PROFILE_SLOTS:
        return False
    return PROFILE_SLOTS.get(profile_key) is not None


def _message_text(item) -> str:
    return (item.body_text or item.snippet or "").strip()


def _extract_sender_email(raw_sender: str) -> str:
    sender = (raw_sender or "").strip().lower()
    if not sender:
        return ""
    if "<" in sender and ">" in sender:
        left = sender.find("<")
        right = sender.find(">", left + 1)
        if left != -1 and right != -1:
            sender = sender[left + 1 : right].strip()
    return sender


def _is_allowed_perplexity_sender(raw_sender: str) -> bool:
    return _extract_sender_email(raw_sender) == PERPLEXITY_ALLOWED_SENDER


def _minutes_from_slot(local_dt: datetime, slot: SlotConfig) -> float:
    slot_dt = datetime.combine(local_dt.date(), time(slot.hour, slot.minute), tzinfo=local_dt.tzinfo)
    return (local_dt - slot_dt).total_seconds() / 60.0


def _assign_profile_by_time(local_dt: datetime) -> tuple[str, float] | None:
    candidates: list[tuple[str, float]] = []
    for profile, slot in PROFILE_SLOTS.items():
        if slot is None:
            continue
        diff_min = _minutes_from_slot(local_dt, slot)
        if -float(slot.before_min) <= diff_min <= float(slot.after_min):
            candidates.append((profile, abs(diff_min)))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[1], x[0]))
    return candidates[0]


def _format_aggregated_seed_summary(
    assigned: list[tuple[Any, float, datetime]],
    max_chars: int,
) -> tuple[str, int]:
    blocks: list[str] = []
    used_reports = 0
    total_chars = 0

    for item, abs_delta, local_dt in assigned:
        body = _message_text(item)
        if not body:
            continue
        block = (
            f"[Perplexity report | local_time={local_dt.isoformat(timespec='seconds')} | "
            f"slot_abs_delta_min={round(float(abs_delta), 1)}]\n"
            f"Subject: {item.subject or '(no subject)'}\n"
            f"{body.strip()}"
        ).strip()
        if not block:
            continue

        projected_len = total_chars + (2 if blocks else 0) + len(block)
        if blocks and projected_len > max_chars:
            break
        if (not blocks) and len(block) > max_chars:
            block = block[: max_chars - 3].rstrip() + "..."
            projected_len = len(block)

        blocks.append(block)
        total_chars = projected_len
        used_reports += 1

    return "\n\n".join(blocks).strip(), used_reports


def load_aggregated_perplexity_summary_for_profile(
    profile_name: str,
    prompts_dir: Path,
    base_dir: Path,
    max_chars: int = 16000,
) -> tuple[str, dict[str, Any]]:
    profile_key = (profile_name or "").strip().lower()
    slot = PROFILE_SLOTS.get(profile_key)
    if profile_key not in PROFILE_SLOTS:
        return "", {"perplexity_seed_status": "profile_not_supported"}
    if slot is None:
        return "", {"perplexity_seed_status": "slot_not_configured_for_profile"}

    try:
        from gmail_connector import GmailConnector
    except Exception as exc:
        return "", {"perplexity_seed_status": f"error_import: {exc}"}

    try:
        connector = GmailConnector.from_env(base_dir=base_dir)
        connector.config = replace(
            connector.config,
            days_back=max(PERPLEXITY_SEED_DAYS_BACK, int(getattr(connector.config, "days_back", 0) or 0)),
            max_results=max(PERPLEXITY_SEED_FETCH_MAX_RESULTS, int(getattr(connector.config, "max_results", 0) or 0)),
            from_filters=(PERPLEXITY_ALLOWED_SENDER,),
            subject_filters=(),
        )
        messages = connector.fetch_messages(prompt_topics=None)
    except Exception as exc:
        return "", {"perplexity_seed_status": f"error_fetch: {exc}"}

    msk_tz = _resolve_moscow_tz()
    assigned: list[tuple[Any, float, datetime]] = []
    parsed_with_date = 0
    perplexity_sender_messages = 0
    for item in messages:
        if not _is_allowed_perplexity_sender(getattr(item, "sender", "")):
            continue
        perplexity_sender_messages += 1
        if not item.internal_date:
            continue
        parsed_with_date += 1
        text = _message_text(item)
        if not text:
            continue
        local_dt = item.internal_date.astimezone(msk_tz)
        decision = _assign_profile_by_time(local_dt)
        if not decision:
            continue
        assigned_profile, abs_delta = decision
        if assigned_profile != profile_key:
            continue
        assigned.append((item, abs_delta, local_dt))

    if not assigned:
        return "", {
            "perplexity_seed_status": "not_found_in_time_window",
            "perplexity_seed_total_messages": len(messages),
            "perplexity_seed_sender_messages": perplexity_sender_messages,
            "perplexity_seed_messages_with_date": parsed_with_date,
            "perplexity_seed_sender_filter": PERPLEXITY_ALLOWED_SENDER,
        }

    assigned.sort(key=lambda x: ((x[0].internal_date or datetime.min), -x[1]), reverse=True)
    summary, used_reports = _format_aggregated_seed_summary(assigned, max_chars=max_chars)
    if not summary:
        return "", {"perplexity_seed_status": "empty_message_body"}

    latest_item, latest_delta_min, latest_local_dt = assigned[0]
    oldest_item, oldest_delta_min, oldest_local_dt = assigned[-1]
    meta = {
        "perplexity_seed_status": "ok_aggregated",
        "perplexity_seed_total_messages": len(messages),
        "perplexity_seed_sender_messages": perplexity_sender_messages,
        "perplexity_seed_profile_messages": len(assigned),
        "perplexity_seed_reports_used": used_reports,
        "perplexity_seed_days_back": PERPLEXITY_SEED_DAYS_BACK,
        "perplexity_seed_fetch_max_results": PERPLEXITY_SEED_FETCH_MAX_RESULTS,
        "perplexity_seed_slot": f"{slot.hour:02d}:{slot.minute:02d}",
        "perplexity_seed_slot_abs_delta_min": round(float(latest_delta_min), 1),
        "perplexity_seed_latest_local_time": latest_local_dt.isoformat(timespec="seconds"),
        "perplexity_seed_oldest_local_time": oldest_local_dt.isoformat(timespec="seconds"),
        "perplexity_seed_sender_filter": PERPLEXITY_ALLOWED_SENDER,
        "perplexity_seed_latest_subject": latest_item.subject,
        "perplexity_seed_oldest_subject": oldest_item.subject,
        "perplexity_seed_sender": latest_item.sender,
        "perplexity_seed_chars": len(summary),
    }
    return summary, meta


# Backward-compatible alias for existing imports/call sites.
def load_latest_perplexity_summary_for_profile(
    profile_name: str,
    prompts_dir: Path,
    base_dir: Path,
    max_chars: int = 16000,
) -> tuple[str, dict[str, Any]]:
    return load_aggregated_perplexity_summary_for_profile(
        profile_name=profile_name,
        prompts_dir=prompts_dir,
        base_dir=base_dir,
        max_chars=max_chars,
    )
