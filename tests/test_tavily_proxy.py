import unittest
from unittest.mock import patch

from manage import load_settings
from pipelines.tavily_proxy import configure_tavily_proxy, format_tavily_error, get_tavily_proxies


class TavilyProxyTests(unittest.TestCase):
    def tearDown(self) -> None:
        configure_tavily_proxy("")

    def test_proxy_mapping_is_disabled_for_empty_url(self) -> None:
        configure_tavily_proxy("")

        self.assertIsNone(get_tavily_proxies())

    def test_proxy_mapping_routes_http_and_https_through_same_socks_url(self) -> None:
        proxy_url = "socks5://user:password@proxy.example:1080"
        configure_tavily_proxy(proxy_url)

        self.assertEqual(
            get_tavily_proxies(),
            {"http": proxy_url, "https": proxy_url},
        )

    def test_empty_sdk_error_keeps_exception_class_for_diagnostics(self) -> None:
        error = RuntimeError("")

        self.assertEqual(format_tavily_error(error), "RuntimeError('')")

    @patch("manage.load_env")
    def test_settings_use_telegram_proxy_as_tavily_fallback(self, load_env_mock) -> None:
        proxy_url = "socks5://proxy.example:1080"
        load_env_mock.return_value = {
            "BOT_TOKEN": "bot-token",
            "TAVILY_API_KEY": "tavily-key",
            "TELEGRAM_PROXY_URL": proxy_url,
        }

        settings = load_settings()

        self.assertEqual(settings["tavily_proxy_url"], proxy_url)

    @patch("manage.load_env")
    def test_explicit_tavily_proxy_overrides_telegram_proxy(self, load_env_mock) -> None:
        load_env_mock.return_value = {
            "BOT_TOKEN": "bot-token",
            "TAVILY_API_KEY": "tavily-key",
            "TELEGRAM_PROXY_URL": "socks5://telegram.example:1080",
            "TAVILY_PROXY_URL": "socks5://tavily.example:1080",
        }

        settings = load_settings()

        self.assertEqual(settings["tavily_proxy_url"], "socks5://tavily.example:1080")


if __name__ == "__main__":
    unittest.main()
