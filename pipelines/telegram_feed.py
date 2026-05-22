import re
from dataclasses import dataclass
from datetime import date, datetime
from html import unescape

import requests


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

POST_ID_RE = re.compile(r'data-post="([A-Za-z0-9_]+)/(\d+)"', re.IGNORECASE)
DATE_RE = re.compile(r'<time[^>]*datetime="([^"]+)"', re.IGNORECASE)
TEXT_RE = re.compile(
    r'<div class="tgme_widget_message_text[^>]*>(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class TelegramPost:
    channel: str
    post_id: int
    published_date: date
    published_iso: str
    text: str
    url: str


def extract_telegram_channels(include_domains: list[str]) -> list[str]:
    channels: list[str] = []
    seen: set[str] = set()
    for value in include_domains:
        if not value.startswith("t.me/"):
            continue
        channel = value.split("/", 1)[1].strip()
        if not channel:
            continue
        key = channel.lower()
        if key in seen:
            continue
        seen.add(key)
        channels.append(channel)
    return channels


def _clean_text(raw_html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    text = TAG_RE.sub(" ", text)
    text = unescape(text)
    return " ".join(text.split())


def _fetch_channel_html(channel: str, timeout_sec: int = 20) -> str:
    response = requests.get(
        f"https://t.me/s/{channel}",
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9,ru;q=0.8"},
        timeout=timeout_sec,
        allow_redirects=True,
    )
    response.raise_for_status()
    return response.text


def parse_recent_open_channel_posts(
    channel: str,
    start_date: date,
    end_date: date,
    max_posts: int = 8,
) -> tuple[list[TelegramPost], str]:
    try:
        html = _fetch_channel_html(channel)
    except Exception as exc:
        return [], f"network error: {exc}"

    matches = list(POST_ID_RE.finditer(html))
    if not matches:
        return [], "no message blocks found"

    result: list[TelegramPost] = []
    seen_ids: set[int] = set()

    for idx, match in enumerate(matches):
        _, post_id_raw = match.groups()
        try:
            post_id = int(post_id_raw)
        except ValueError:
            continue
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        block_start = html.rfind('<div class="tgme_widget_message_wrap', 0, match.start())
        if block_start < 0:
            block_start = match.start()
        block_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(html)
        block = html[block_start:block_end]

        date_match = DATE_RE.search(block)
        if not date_match:
            continue
        raw_dt = date_match.group(1).strip()
        try:
            post_dt = datetime.fromisoformat(raw_dt)
        except ValueError:
            continue
        post_date = post_dt.date()
        if post_date < start_date or post_date > end_date:
            continue

        text_match = TEXT_RE.search(block)
        if text_match:
            cleaned = _clean_text(text_match.group(1))
        else:
            cleaned = ""
        if not cleaned:
            cleaned = "[media-only post]"

        result.append(
            TelegramPost(
                channel=channel,
                post_id=post_id,
                published_date=post_date,
                published_iso=post_date.isoformat(),
                text=cleaned,
                url=f"https://t.me/{channel}/{post_id}",
            )
        )
        if len(result) >= max_posts:
            break

    return result, ""
