from __future__ import annotations

from dataclasses import dataclass
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
    "logistics": SlotConfig(hour=18, minute=0, before_min=5, after_min=5),
    "metanol": SlotConfig(hour=18, minute=30, before_min=5, after_min=5),
    # Placeholders for future schedule setup.
    "precursors": None,
    "rop": None,
}


def _resolve_moscow_tz():
    try:
        return ZoneInfo("Europe/Moscow")
    except Exception:
        return timezone(timedelta(hours=3))


def _message_text(item) -> str:
    return (item.body_text or item.snippet or "").strip()


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


def load_latest_perplexity_summary_for_profile(
    profile_name: str,
    prompts_dir: Path,
    base_dir: Path,
    max_chars: int = 8000,
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
        messages = connector.fetch_messages(prompt_topics=None)
    except Exception as exc:
        return "", {"perplexity_seed_status": f"error_fetch: {exc}"}

    msk_tz = _resolve_moscow_tz()
    assigned: list[tuple[Any, float, datetime]] = []
    parsed_with_date = 0
    for item in messages:
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
            "perplexity_seed_messages_with_date": parsed_with_date,
        }

    assigned.sort(key=lambda x: ((x[0].internal_date or datetime.min), -x[1]), reverse=True)
    latest, delta_min, latest_local_dt = assigned[0]

    summary = _message_text(latest)
    if not summary:
        return "", {"perplexity_seed_status": "empty_message_body"}

    summary = summary[:max_chars]
    meta = {
        "perplexity_seed_status": "ok",
        "perplexity_seed_profile_messages": len(assigned),
        "perplexity_seed_slot": f"{slot.hour:02d}:{slot.minute:02d}",
        "perplexity_seed_slot_abs_delta_min": round(float(delta_min), 1),
        "perplexity_seed_local_time": latest_local_dt.isoformat(timespec="seconds"),
        "perplexity_seed_subject": latest.subject,
        "perplexity_seed_sender": latest.sender,
        "perplexity_seed_chars": len(summary),
    }
    return summary, meta
