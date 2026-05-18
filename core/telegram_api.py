import asyncio
import json
from typing import Optional
from urllib import error, parse, request


TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/{method}"
MAX_TELEGRAM_MESSAGE_LENGTH = 4000


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


def tg_request(bot_token: str, method: str, payload: Optional[dict] = None) -> dict:
    url = TELEGRAM_API_URL.format(token=bot_token, method=method)
    data = None
    headers = {}
    http_method = "GET"
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
        http_method = "POST"
    req = request.Request(url, data=data, headers=headers, method=http_method)
    try:
        with request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram API error {exc.code}: {details}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Telegram network error: {exc}") from exc
    if not body.get("ok"):
        raise RuntimeError(f"Telegram API returned error: {body}")
    return body["result"]


async def tg_get_updates(bot_token: str, offset: int | None, timeout: int = 30) -> list[dict]:
    params = {"timeout": timeout}
    if offset is not None:
        params["offset"] = offset
    method = f"getUpdates?{parse.urlencode(params)}"
    return await asyncio.to_thread(tg_request, bot_token, method, None)


async def tg_send_text(bot_token: str, chat_id: int, text: str) -> None:
    for chunk in split_message(text):
        payload = {"chat_id": chat_id, "text": chunk, "disable_web_page_preview": True}
        await asyncio.to_thread(tg_request, bot_token, "sendMessage", payload)
