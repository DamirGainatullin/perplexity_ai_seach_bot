from datetime import timedelta, timezone
from pathlib import Path


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().lstrip("=")
    return values


def read_text_with_fallback(path: Path) -> str:
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "cp866"):
        try:
            return path.read_text(encoding=encoding).strip()
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Cannot decode file: {path}")


def resolve_timezone(tz_name: str) -> timezone:
    normalized = (tz_name or "").strip()
    if normalized in {"Europe/Moscow", "MSK", "UTC+3", "+03:00", "+03"}:
        return timezone(timedelta(hours=3))
    if normalized in {"UTC", "Etc/UTC", "+00:00", "+00"}:
        return timezone.utc
    return timezone.utc

