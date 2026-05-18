import re
from datetime import date
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import urlsplit

from pipelines.models import PromptProfile


def extract_urls(text: str) -> list[str]:
    raw_urls = re.findall(r"https?://[^\s,;]+", text)
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_url in raw_urls:
        url = raw_url.rstrip(").,;]")
        if url and url not in seen:
            seen.add(url)
            cleaned.append(url)
    return cleaned


def build_include_domains(urls: list[str]) -> list[str]:
    domains: list[str] = []
    seen: set[str] = set()
    for url in urls:
        parsed = urlsplit(url)
        host = parsed.netloc.lower().strip()
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        include_value = host
        if host == "t.me":
            channel = parsed.path.strip("/").split("/", 1)[0]
            if channel:
                include_value = f"t.me/{channel}"
        if include_value not in seen:
            seen.add(include_value)
            domains.append(include_value)
    return domains


def group_domains(include_domains: list[str], profile: PromptProfile) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {
        "official": [],
        "legal_media": [],
        "law_firms": [],
        "other": [],
    }
    for domain in include_domains:
        host = domain.split("/", 1)[0]
        if host in profile.official_domains:
            groups["official"].append(domain)
        elif host in profile.media_domains:
            groups["legal_media"].append(domain)
        elif host in profile.law_firm_domains:
            groups["law_firms"].append(domain)
        elif not domain.startswith("t.me/"):
            groups["other"].append(domain)
    return groups


def resolve_domain_scope(scope: str, groups: dict[str, list[str]]) -> list[str]:
    if scope == "none":
        return []
    if scope == "official":
        return groups["official"]
    if scope == "legal_media":
        return groups["legal_media"]
    if scope == "law_firms":
        return groups["law_firms"]
    if scope == "other":
        return groups["other"]
    if scope == "all_ru":
        return groups["official"] + groups["legal_media"] + groups["law_firms"] + groups["other"]
    return []


def short_query(text: str, max_len: int = 390) -> str:
    stripped = " ".join(text.split())
    if len(stripped) <= max_len:
        return stripped
    return stripped[: max_len - 3] + "..."


def parse_published_iso(raw_published_date: str) -> str:
    raw = (raw_published_date or "").strip()
    if not raw:
        return ""
    try:
        return parsedate_to_datetime(raw).date().isoformat()
    except Exception:
        return ""


def parse_date_from_url(url: str) -> Optional[date]:
    if not url:
        return None
    lower = url.lower()
    match = re.search(r"000120(20\d{2})(\d{2})(\d{2})\d*", lower)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            return None
    match2 = re.search(r"/(20\d{2})/(\d{2})/(\d{2})/", lower)
    if match2:
        try:
            return date(int(match2.group(1)), int(match2.group(2)), int(match2.group(3)))
        except ValueError:
            return None
    return None


def parse_date_candidates(text: str) -> list[date]:
    candidates: list[date] = []
    for y, m, d in re.findall(r"\b(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})\b", text):
        try:
            candidates.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    for d, m, y in re.findall(r"\b(\d{1,2})\.(\d{1,2})\.(20\d{2})\b", text):
        try:
            candidates.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    return candidates


def is_topic_related(text: str, topic_keywords: tuple[str, ...]) -> bool:
    low = (text or "").lower()
    return any(keyword in low for keyword in topic_keywords)


def legal_score(text: str, legal_keywords: tuple[str, ...]) -> int:
    low = (text or "").lower()
    return sum(1 for keyword in legal_keywords if keyword in low)


def infer_category(text: str, profile: PromptProfile) -> str:
    low = (text or "").lower()
    for rule in profile.category_rules:
        if any(keyword in low for keyword in rule.keywords):
            return rule.name
    return "Смежные правовые изменения"


def build_summary(content: str, title: str, limit: int = 320) -> str:
    text = " ".join((content or "").split())
    if not text:
        text = (title or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."

