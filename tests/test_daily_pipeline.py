import asyncio
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from core.db import (
    db_get_cached_result,
    db_get_recent_digest_history,
    db_get_tavily_daily_usage,
    db_init,
    db_list_due_schedule_chat_ids,
    db_prepare_schedule_mode,
    db_reconcile_tavily_credits,
    db_reserve_tavily_credits,
    db_save_cached_response,
    db_save_digest_history,
    db_toggle_schedule_subscription,
)
from manage import (
    DEFAULT_SCHEDULE_PROFILE_ROTATION,
    _build_scheduled_reports,
    _is_schedule_due,
    _parse_schedule_profile_rotation,
    _scheduled_command_for_day,
)
from core.telegram_api import _redact_bot_token
from pipelines.adaptive_followup import _call_planner_model
from pipelines.daily_engine import _labeled_publication_date, build_daily_probe_query, run_daily_pipeline
from pipelines.openrouter_filter import (
    _call_openrouter_json,
    configure_openrouter_proxy,
    run_daily_stage1_openrouter,
)
from pipelines.profiles.logistics import build_logistics_profile


class FakeTavilyClient:
    search_responses: list[dict] = []
    search_payloads: list[dict] = []
    extract_responses: list[dict] = []
    extract_payloads: list[dict] = []

    def __init__(self, _api_key: str) -> None:
        pass

    def search(self, **payload):
        self.search_payloads.append(payload)
        return self.search_responses.pop(0)

    def extract(self, **payload):
        self.extract_payloads.append(payload)
        return self.extract_responses.pop(0)


def _profile() -> object:
    return build_logistics_profile(Path(__file__).resolve().parents[1] / "prompts")


class DailyPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeTavilyClient.search_payloads = []
        FakeTavilyClient.search_responses = []
        FakeTavilyClient.extract_payloads = []
        FakeTavilyClient.extract_responses = []

    def test_probe_query_is_daily_and_profile_specific(self) -> None:
        query = build_daily_probe_query(_profile(), date(2026, 7, 10))
        self.assertIn("логистика", query.lower())
        self.assertIn("последние сутки", query.lower())
        self.assertNotIn("7 дней", query.lower())

    def test_effective_date_is_not_treated_as_publication_date(self) -> None:
        text = "Опубликован закон, который вступит в силу 1 сентября 2026 года."
        self.assertEqual(_labeled_publication_date(text), "")
        self.assertEqual(_labeled_publication_date("Дата публикации: 10.07.2026."), "2026-07-10")

    @patch("pipelines.openrouter_filter._call_openrouter_json")
    def test_stage1_caps_items_and_removes_site_operator(self, call_mock) -> None:
        call_mock.return_value = (
            {
                "keep_indices": list(range(1, 21)),
                "followup_query": "site:example.test Минтранс новый приказ Россия",
            },
            {},
            "ok",
        )
        rows = [
            {
                "title": f"Item {index}",
                "summary": "Summary",
                "content": "Content",
                "url": f"https://source.test/{index}",
                "date": "2026-07-10",
            }
            for index in range(1, 21)
        ]
        filtered, query, usage = run_daily_stage1_openrouter(
            rows,
            "Profile",
            [],
            "key",
        )
        self.assertEqual(len(filtered), 12)
        self.assertNotIn("site:", query)
        self.assertEqual(usage["openrouter_stage1_keep_indices"], list(range(1, 13)))

    @patch("pipelines.daily_engine._collect_telegram_rows", return_value=([], {}))
    @patch("pipelines.daily_engine.run_daily_stage2_stage3_openrouter")
    @patch("pipelines.daily_engine.run_daily_stage1_openrouter")
    @patch("pipelines.daily_engine.TavilyClient", FakeTavilyClient)
    def test_basic_search_costs_one_credit_and_has_no_domain_scope(
        self,
        stage1_mock,
        final_mock,
        _telegram_mock,
    ) -> None:
        FakeTavilyClient.search_responses = [
            {
                "usage": {"credits": 1},
                "results": [
                    {
                        "title": "Новый приказ Минтранса",
                        "url": "https://example.test/news",
                        "published_date": "2026-07-10",
                        "content": "Минтранс опубликовал новый приказ о перевозках.",
                        "score": 0.8,
                    }
                ],
            }
        ]
        stage1_mock.side_effect = lambda rows, *_args: (
            rows,
            "",
            {"openrouter_stage1_status": "ok"},
        )
        final_mock.side_effect = lambda rows, *_args: (
            rows,
            {"openrouter_stage3_status": "ok"},
        )

        rows, usage = run_daily_pipeline(
            _profile(),
            "Тематический промпт",
            "tavily-key",
            "openrouter-key",
            "openai/gpt-4.1-mini",
            date(2026, 7, 10),
            [],
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(usage["total_credits"], 1.0)
        payload = FakeTavilyClient.search_payloads[0]
        self.assertEqual(payload["search_depth"], "basic")
        self.assertEqual(payload["country"], "russia")
        self.assertIsNone(payload["include_domains"])
        self.assertFalse(payload["include_answer"])
        self.assertEqual(payload["start_date"], "2026-07-09")

    @patch("pipelines.daily_engine._collect_telegram_rows", return_value=([], {}))
    @patch("pipelines.daily_engine.run_daily_stage2_stage3_openrouter")
    @patch("pipelines.daily_engine.run_daily_stage1_openrouter")
    @patch("pipelines.daily_engine.TavilyClient", FakeTavilyClient)
    def test_followup_and_extract_never_exceed_four_credits(
        self,
        stage1_mock,
        final_mock,
        _telegram_mock,
    ) -> None:
        base_item = {
            "title": "Сообщение о новом регулировании",
            "url": "https://example.test/base",
            "published_date": "",
            "content": "Минтранс сообщил о подготовке нового порядка.",
            "score": 0.7,
        }
        advanced_item = {
            "title": "Проект порядка опубликован",
            "url": "https://example.test/advanced",
            "published_date": "",
            "content": "Опубликован проект порядка перевозок.",
            "score": 0.9,
        }
        FakeTavilyClient.search_responses = [
            {"usage": {"credits": 1}, "results": [base_item]},
            {"usage": {"credits": 2}, "results": [advanced_item]},
        ]
        FakeTavilyClient.extract_responses = [
            {
                "usage": {"credits": 1},
                "results": [
                    {
                        "url": "https://example.test/base",
                        "raw_content": "Дата публикации 10.07.2026. Новый порядок перевозок.",
                    },
                    {
                        "url": "https://example.test/advanced",
                        "raw_content": "Опубликовано 10.07.2026. Проект порядка перевозок.",
                    },
                ],
                "failed_results": [],
            }
        ]
        stage1_mock.side_effect = lambda rows, *_args: (
            rows,
            "Минтранс новый порядок перевозок проект документа Россия 10 июля 2026",
            {"openrouter_stage1_status": "ok"},
        )
        final_mock.side_effect = lambda rows, *_args: (
            rows,
            {"openrouter_stage3_status": "ok"},
        )

        rows, usage = run_daily_pipeline(
            _profile(),
            "Тематический промпт",
            "tavily-key",
            "openrouter-key",
            "openai/gpt-4.1-mini",
            date(2026, 7, 10),
            [],
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(usage["total_credits"], 4.0)
        self.assertEqual(FakeTavilyClient.search_payloads[1]["search_depth"], "advanced")
        self.assertIsNone(FakeTavilyClient.search_payloads[1]["include_domains"])
        self.assertEqual(len(FakeTavilyClient.extract_payloads[0]["urls"]), 2)


class DailyDatabaseTests(unittest.TestCase):
    def test_cache_history_and_credit_reservation(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "test.sqlite3"
            db_init(db_path)

            db_save_cached_response(db_path, "logistics", "2026-07-10", "report", 3)
            self.assertEqual(
                db_get_cached_result(db_path, "logistics", "2026-07-10"),
                ("report", 3),
            )

            db_save_digest_history(
                db_path,
                "logistics",
                [{"url": "https://example.test/1", "title": "Title", "summary": "Summary", "date": "2026-07-10"}],
            )
            history = db_get_recent_digest_history(db_path, "logistics")
            self.assertEqual(history[0]["url"], "https://example.test/1")

            self.assertTrue(db_reserve_tavily_credits(db_path, "2026-07-10", 4, 5))
            self.assertFalse(db_reserve_tavily_credits(db_path, "2026-07-10", 2, 5))
            db_reconcile_tavily_credits(db_path, "2026-07-10", 4, 1)
            self.assertEqual(db_get_tavily_daily_usage(db_path, "2026-07-10"), 1.0)
            self.assertTrue(db_reserve_tavily_credits(db_path, "2026-07-10", 4, 5))

    def test_enabling_schedule_after_daily_slot_does_not_send_immediately(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "schedule.sqlite3"
            db_init(db_path)
            enabled = db_toggle_schedule_subscription(
                db_path,
                123,
                initial_last_sent_at="2026-07-10",
            )
            self.assertTrue(enabled)
            self.assertEqual(db_list_due_schedule_chat_ids(db_path, "2026-07-10"), [])

    def test_rotating_schedule_runs_every_day_after_configured_time(self) -> None:
        friday = datetime(2026, 7, 10, 8, 30, tzinfo=timezone.utc)
        self.assertTrue(_is_schedule_due(friday, 8, 30))
        self.assertFalse(_is_schedule_due(friday.replace(hour=8, minute=29), 8, 30))

    def test_weekday_rotation_starts_with_logistics_then_rop(self) -> None:
        monday = datetime(2026, 7, 13, 8, 30, tzinfo=timezone.utc)
        expected = (
            "/logistics",
            "/rop",
            "/metanol",
            "/precursors",
            "/chesny_znak",
            "/spot",
            "/sales",
        )
        self.assertEqual(DEFAULT_SCHEDULE_PROFILE_ROTATION, expected)
        self.assertEqual(
            tuple(_scheduled_command_for_day(monday.replace(day=13 + offset), expected) for offset in range(7)),
            expected,
        )

    def test_schedule_rotation_can_be_configured_without_slashes(self) -> None:
        parsed = _parse_schedule_profile_rotation(
            "logistics,rop,metanol,precursors,chesny_znak,spot,sales"
        )
        self.assertEqual(parsed, DEFAULT_SCHEDULE_PROFILE_ROTATION)

    def test_first_mode_transition_skips_elapsed_slot_only_once(self) -> None:
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            db_path = Path(tmp) / "mode.sqlite3"
            db_init(db_path)
            db_toggle_schedule_subscription(db_path, 123)

            previous, skipped = db_prepare_schedule_mode(
                db_path,
                "daily",
                elapsed_schedule_slot="2026-07-10",
            )
            self.assertEqual(previous, "")
            self.assertEqual(skipped, 1)
            self.assertEqual(db_list_due_schedule_chat_ids(db_path, "2026-07-10"), [])

            previous, skipped = db_prepare_schedule_mode(
                db_path,
                "daily",
                elapsed_schedule_slot="2026-07-11",
            )
            self.assertEqual(previous, "daily")
            self.assertEqual(skipped, 0)
            self.assertEqual(db_list_due_schedule_chat_ids(db_path, "2026-07-11"), [123])


class ScheduleRotationTests(unittest.IsolatedAsyncioTestCase):
    @patch("manage.generate_by_profile_with_usage", new_callable=AsyncMock)
    async def test_scheduled_builder_generates_only_selected_profile(self, generate_mock) -> None:
        logistics = object()
        rop = object()
        generate_mock.return_value = ("ROP report", {"final_rows_count": 2})

        reports = await _build_scheduled_reports(
            {"/logistics": logistics, "/rop": rop},
            "/rop",
            "tavily-key",
            "openrouter-key",
            "openai/gpt-4.1-mini",
            True,
            8.0,
            4,
            6,
            timezone.utc,
            "weekly",
            2,
            28.0,
            asyncio.Lock(),
        )

        self.assertEqual(reports, [("/rop", "ROP report", True, 2)])
        generate_mock.assert_awaited_once()
        self.assertIs(generate_mock.await_args.args[7], rop)
        self.assertEqual(generate_mock.await_args.kwargs["digest_mode"], "weekly")


class ProxyTests(unittest.TestCase):
    def tearDown(self) -> None:
        configure_openrouter_proxy("")

    @staticmethod
    def _response(content: str) -> Mock:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "choices": [{"message": {"content": content}}],
            "usage": {},
        }
        return response

    @patch("pipelines.openrouter_filter.requests.post")
    def test_daily_openrouter_call_uses_configured_proxy(self, post_mock) -> None:
        post_mock.return_value = self._response('{"keep_indices":[]}')
        configure_openrouter_proxy("socks5h://user:pass@proxy.test:1080")

        _parsed, _usage, status = _call_openrouter_json(
            system_prompt="prompt",
            user_payload={"materials": []},
            openrouter_api_key="key",
            model="openai/gpt-4.1-mini",
            max_tokens=20,
            timeout_sec=10,
        )

        self.assertEqual(status, "ok")
        self.assertEqual(
            post_mock.call_args.kwargs["proxies"],
            {
                "http": "socks5h://user:pass@proxy.test:1080",
                "https": "socks5h://user:pass@proxy.test:1080",
            },
        )

    @patch("pipelines.adaptive_followup.requests.post")
    def test_weekly_planner_uses_same_proxy(self, post_mock) -> None:
        post_mock.return_value = self._response('{"queries":[]}')
        configure_openrouter_proxy("socks5://proxy.test:1080")

        _parsed, _usage, status = _call_planner_model(
            openrouter_api_key="key",
            model="openai/gpt-4.1-mini",
            system_prompt="prompt",
            user_payload={},
            timeout_sec=10,
            max_tokens=20,
        )

        self.assertEqual(status, "ok")
        self.assertEqual(post_mock.call_args.kwargs["proxies"]["https"], "socks5://proxy.test:1080")

    def test_telegram_token_is_removed_from_network_errors(self) -> None:
        token = "123456:secret-token"
        redacted = _redact_bot_token(f"https://api.telegram.org/bot{token}/getUpdates", token)
        self.assertNotIn(token, redacted)
        self.assertIn("bot***/getUpdates", redacted)


if __name__ == "__main__":
    unittest.main()
