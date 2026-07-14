from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from html import unescape
from typing import Any
from urllib.parse import urlsplit

import requests

from pipelines.helpers import parse_date_candidates, parse_date_from_url, parse_published_iso


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
PUBLICATION_META_KEYS = {
    "article:published_time",
    "datepublished",
    "date.created",
    "dc.date.issued",
    "parsely-pub-date",
    "pubdate",
    "publishdate",
    "sailthru.date",
}
PUBLICATION_MARKERS = (
    "дата публикации",
    "дата размещения",
    "опубликовано",
    "опубликован",
    "опубликована",
    "размещено",
    "publication date",
    "published on",
)
EFFECTIVE_DATE_MARKERS = ("вступ", "действ", "примен", "срок")
ARTICLE_PATH_MARKERS = ("/news/", "/press-center/", "/press_center/", "/press/")


@dataclass(frozen=True)
class PublicationDateVerification:
    status: str
    date_iso: str = ""
    source: str = ""
    detail: str = ""
    tavily_published_date: str = ""


def _parse_date_value(value: str) -> date | None:
    parsed_iso = parse_published_iso(value)
    if parsed_iso:
        try:
            return date.fromisoformat(parsed_iso)
        except ValueError:
            pass
    candidates = parse_date_candidates(value)
    return candidates[0] if candidates else None


def parse_labeled_publication_date(text: str) -> date | None:
    normalized = " ".join((text or "").split())
    low = normalized.lower()
    for marker in PUBLICATION_MARKERS:
        start = 0
        while True:
            marker_index = low.find(marker, start)
            if marker_index < 0:
                break
            tail = normalized[marker_index + len(marker) : marker_index + len(marker) + 140]
            year_match = re.search(r"20\d{2}", tail)
            prefix_before_date = tail[: year_match.start()] if year_match else tail
            if not any(blocker in prefix_before_date.lower() for blocker in EFFECTIVE_DATE_MARKERS):
                candidates = parse_date_candidates(tail)
                if candidates:
                    return candidates[0]
            start = marker_index + len(marker)
    return None


def _tag_attributes(tag: str) -> dict[str, str]:
    return {
        key.lower(): unescape(value).strip()
        for key, _quote, value in re.findall(r"([:\w-]+)\s*=\s*([\"'])(.*?)\2", tag, flags=re.DOTALL)
    }


def parse_publication_date_from_html(html: str, url: str) -> tuple[date | None, str]:
    raw_html = html or ""
    for tag in re.findall(r"<meta\b[^>]*>", raw_html, flags=re.IGNORECASE | re.DOTALL):
        attrs = _tag_attributes(tag)
        key = (attrs.get("property") or attrs.get("name") or attrs.get("itemprop") or "").lower()
        if key not in PUBLICATION_META_KEYS:
            continue
        parsed = _parse_date_value(attrs.get("content", ""))
        if parsed:
            return parsed, f"html_meta:{key}"

    for match in re.finditer(r'"datePublished"\s*:\s*"([^"]+)"', raw_html, flags=re.IGNORECASE):
        parsed = _parse_date_value(unescape(match.group(1)))
        if parsed:
            return parsed, "html_jsonld:datePublished"

    for tag in re.findall(r"<time\b[^>]*>", raw_html, flags=re.IGNORECASE | re.DOTALL):
        attrs = _tag_attributes(tag)
        marker = " ".join(
            (attrs.get("itemprop", ""), attrs.get("class", ""), attrs.get("property", ""))
        ).lower()
        if not any(value in marker for value in ("datepublished", "published", "publication")):
            continue
        parsed = _parse_date_value(attrs.get("datetime", ""))
        if parsed:
            return parsed, "html_time:published"

    path = urlsplit(url).path.lower()
    if any(marker in path for marker in ARTICLE_PATH_MARKERS):
        for heading in re.findall(r"<h[1-6]\b[^>]*>(.*?)</h[1-6]>", raw_html, flags=re.IGNORECASE | re.DOTALL):
            heading_text = " ".join(unescape(re.sub(r"<[^>]+>", " ", heading)).split())
            candidates = parse_date_candidates(heading_text)
            if candidates:
                return candidates[0], "html_article_heading"

    text = " ".join(unescape(re.sub(r"<[^>]+>", " ", raw_html)).split())
    labeled = parse_labeled_publication_date(text[:12000])
    if labeled:
        return labeled, "html_labeled_publication_date"
    return None, ""


def _verification_for_date(
    resolved: date,
    source: str,
    start_date: date,
    end_date: date,
    tavily_published_date: str,
) -> PublicationDateVerification:
    if start_date <= resolved <= end_date:
        return PublicationDateVerification(
            status="verified",
            date_iso=resolved.isoformat(),
            source=source,
            tavily_published_date=tavily_published_date,
        )
    return PublicationDateVerification(
        status="outside_window",
        date_iso=resolved.isoformat(),
        source=source,
        tavily_published_date=tavily_published_date,
    )


def _fetch_html_publication_date(url: str, timeout_sec: int) -> tuple[date | None, str, str]:
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ru,en;q=0.8"},
            timeout=timeout_sec,
            allow_redirects=True,
        )
        response.raise_for_status()
        resolved, source = parse_publication_date_from_html(response.text[:2_000_000], response.url or url)
        return resolved, source, ""
    except Exception as exc:
        return None, "", str(exc)[:500]


def verify_publication_dates(
    candidates: list[dict[str, Any]],
    start_date: date,
    end_date: date,
    *,
    timeout_sec: int = 12,
    max_workers: int = 4,
) -> dict[str, PublicationDateVerification]:
    results: dict[str, PublicationDateVerification] = {}
    fetch_candidates: dict[str, str] = {}

    for item in candidates:
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        tavily_date = str(item.get("tavily_published_date", "")).strip()
        url_date = parse_date_from_url(url)
        if url_date:
            results[url] = _verification_for_date(url_date, "url", start_date, end_date, tavily_date)
            continue

        extracted_text = str(item.get("extracted_text", ""))
        labeled_date = parse_labeled_publication_date(extracted_text[:12000])
        if labeled_date:
            results[url] = _verification_for_date(
                labeled_date,
                "tavily_extract_labeled",
                start_date,
                end_date,
                tavily_date,
            )
            continue
        fetch_candidates[url] = tavily_date

    if fetch_candidates:
        workers = max(1, min(int(max_workers), len(fetch_candidates)))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(_fetch_html_publication_date, url, timeout_sec): url
                for url in fetch_candidates
            }
            for future in as_completed(futures):
                url = futures[future]
                tavily_date = fetch_candidates[url]
                resolved, source, error = future.result()
                if resolved:
                    results[url] = _verification_for_date(
                        resolved,
                        source,
                        start_date,
                        end_date,
                        tavily_date,
                    )
                else:
                    results[url] = PublicationDateVerification(
                        status="unverified",
                        detail=error or "publication date not found in page metadata or article header",
                        tavily_published_date=tavily_date,
                    )
    return results
