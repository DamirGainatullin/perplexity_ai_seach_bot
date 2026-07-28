from __future__ import annotations


_TAVILY_PROXY_URL = ""


def configure_tavily_proxy(proxy_url: str) -> None:
    global _TAVILY_PROXY_URL
    _TAVILY_PROXY_URL = (proxy_url or "").strip()


def get_tavily_proxies() -> dict[str, str] | None:
    if not _TAVILY_PROXY_URL:
        return None
    return {
        "http": _TAVILY_PROXY_URL,
        "https": _TAVILY_PROXY_URL,
    }


def format_tavily_error(error: Exception | None) -> str:
    if error is None:
        return "unknown error"
    message = str(error).strip()
    if message:
        return f"{type(error).__name__}: {message}"
    return repr(error)
