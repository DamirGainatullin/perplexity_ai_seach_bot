from __future__ import annotations

import asyncio
from typing import Optional
from urllib.parse import urlencode, urlsplit, urlunsplit

import requests


TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"
MAX_TELEGRAM_MESSAGE_LENGTH = 4000
_TELEGRAM_PROXY_URL = ""


def split_message(text: str, limit: int = MAX_TELEGRAM_MESSAGE_LENGTH) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        while len(block) > limit:
            split_at = block.rfind("\n", 0, limit)
            if split_at <= 0:
                split_at = limit
            chunks.append(block[:split_at].strip())
            block = block[split_at:].strip()
        current = block
    if current:
        chunks.append(current)
    return chunks


def configure_telegram_proxy(proxy_url: str) -> None:
    global _TELEGRAM_PROXY_URL
    _TELEGRAM_PROXY_URL = (proxy_url or "").strip()


def mask_proxy_url(proxy_url: str) -> str:
    parts = urlsplit(proxy_url)
    if not parts.netloc or "@" not in parts.netloc:
        return proxy_url

    _, host_part = parts.netloc.rsplit("@", 1)
    return urlunsplit((parts.scheme, f"***@{host_part}", parts.path, parts.query, parts.fragment))


def tg_request(bot_token: str, method: str, payload: Optional[dict] = None) -> dict:
    url = TELEGRAM_API_URL.format(token=bot_token, method=method)
    http_method = "GET"
    request_kwargs: dict[str, object] = {
        "method": http_method,
        "url": url,
        "timeout": 60,
    }
    if payload is not None:
        http_method = "POST"
        request_kwargs["method"] = http_method
        request_kwargs["json"] = payload
    if _TELEGRAM_PROXY_URL:
        request_kwargs["proxies"] = {"http": _TELEGRAM_PROXY_URL, "https": _TELEGRAM_PROXY_URL}
    try:
        response = requests.request(**request_kwargs)
        response.raise_for_status()
        body = response.json()
    except requests.HTTPError as exc:
        details = ""
        if exc.response is not None:
            details = exc.response.text[:1000]
            status_code = exc.response.status_code
        else:
            status_code = "unknown"
        raise RuntimeError(f"Telegram API error {status_code}: {details}") from exc
    except requests.InvalidSchema as exc:
        if _TELEGRAM_PROXY_URL.lower().startswith("socks"):
            raise RuntimeError(
                "Telegram SOCKS proxy requires PySocks support. Install dependencies from requirements.txt."
            ) from exc
        raise RuntimeError(f"Telegram network error: {exc}") from exc
    except requests.RequestException as exc:
        raise RuntimeError(f"Telegram network error: {exc}") from exc
    if not body.get("ok"):
        raise RuntimeError(f"Telegram API returned error: {body}")
    return body["result"]


async def tg_get_updates(bot_token: str, offset: int | None, timeout: int = 30) -> list[dict]:
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    method = f"getUpdates?{urlencode(params)}"
    return await asyncio.to_thread(tg_request, bot_token, method, None)


async def tg_send_text(bot_token: str, chat_id: int, text: str) -> None:
    for chunk in split_message(text):
        payload = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}
        await asyncio.to_thread(tg_request, bot_token, "sendMessage", payload)
