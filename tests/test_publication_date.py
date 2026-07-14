import unittest
from datetime import date
from pathlib import Path
from unittest.mock import Mock, patch

from pipelines.helpers import parse_date_candidates
from pipelines.engine import run_budget_pipeline
from pipelines.publication_date import (
    PublicationDateVerification,
    parse_labeled_publication_date,
    parse_publication_date_from_html,
    verify_publication_dates,
)
from pipelines.profiles.rop import build_rop_profile


class PublicationDateParsingTests(unittest.TestCase):
    def test_parses_russian_month_before_day(self) -> None:
        self.assertEqual(parse_date_candidates("Июль 13, 2026"), [date(2026, 7, 13)])

    def test_news_article_heading_is_accepted_as_publication_date(self) -> None:
        resolved, source = parse_publication_date_from_html(
            "<html><div class='contentBox'><h4>Июль 13, 2026</h4>"
            "<p>С 1 сентября 2026 года вступает в силу новый порядок.</p></div></html>",
            "https://rpn.gov.ru/regions/63/news/example.html",
        )
        self.assertEqual(resolved, date(2026, 7, 13))
        self.assertEqual(source, "html_article_heading")

    def test_old_news_article_heading_is_resolved_for_rejection(self) -> None:
        resolved, source = parse_publication_date_from_html(
            "<html><h4>Апрель 23, 2019</h4><p>Экологический сбор.</p></html>",
            "https://rpn.gov.ru/regions/38/news/ecological-fee.html",
        )
        self.assertEqual(resolved, date(2019, 4, 23))
        self.assertEqual(source, "html_article_heading")

    def test_reference_page_dates_are_not_treated_as_publication_date(self) -> None:
        resolved, source = parse_publication_date_from_html(
            "<html><h1>Расширенная ответственность производителей</h1>"
            "<p>Федеральный закон от 04.08.2023. Отчетность за 2024 год.</p></html>",
            "https://rpn.gov.ru/regions/76/for_users/management/extended-responsibility",
        )
        self.assertIsNone(resolved)
        self.assertEqual(source, "")

    def test_effective_date_is_not_labeled_publication_date(self) -> None:
        self.assertIsNone(
            parse_labeled_publication_date("Опубликован закон, который вступит в силу 1 сентября 2026 года.")
        )
        self.assertEqual(
            parse_labeled_publication_date("Дата публикации: 13.07.2026. Вступает в силу 1 сентября."),
            date(2026, 7, 13),
        )


class PublicationDateVerificationTests(unittest.TestCase):
    @patch("pipelines.publication_date.requests.get")
    def test_tavily_date_alone_does_not_verify_reference_page(self, get_mock) -> None:
        response = Mock()
        response.url = "https://example.test/reference"
        response.text = "<html><h1>Справочная информация</h1><p>Изменения с 2026 года.</p></html>"
        response.raise_for_status.return_value = None
        get_mock.return_value = response

        result = verify_publication_dates(
            [
                {
                    "url": response.url,
                    "tavily_published_date": "2026-07-14",
                    "extracted_text": "",
                }
            ],
            date(2026, 7, 7),
            date(2026, 7, 14),
            max_workers=1,
        )[response.url]

        self.assertEqual(result.status, "unverified")
        self.assertEqual(result.tavily_published_date, "2026-07-14")

    @patch("pipelines.publication_date.requests.get")
    def test_fresh_and_old_rpn_news_pages_are_distinguished(self, get_mock) -> None:
        def response_for(url: str, **_kwargs):
            response = Mock()
            response.url = url
            response.raise_for_status.return_value = None
            response.text = (
                "<html><h4>Июль 13, 2026</h4><p>Новый порядок.</p></html>"
                if "fresh" in url
                else "<html><h4>Апрель 23, 2019</h4><p>Старый материал.</p></html>"
            )
            return response

        get_mock.side_effect = response_for
        candidates = [
            {"url": "https://rpn.gov.ru/news/fresh.html", "tavily_published_date": "2026-07-14"},
            {"url": "https://rpn.gov.ru/news/old.html", "tavily_published_date": "2026-07-14"},
        ]
        results = verify_publication_dates(
            candidates,
            date(2026, 7, 7),
            date(2026, 7, 14),
            max_workers=1,
        )

        self.assertEqual(results[candidates[0]["url"]].status, "verified")
        self.assertEqual(results[candidates[0]["url"]].date_iso, "2026-07-13")
        self.assertEqual(results[candidates[1]["url"]].status, "outside_window")
        self.assertEqual(results[candidates[1]["url"]].date_iso, "2019-04-23")


class WeeklyEngineDateGateTests(unittest.TestCase):
    @patch("pipelines.engine.parse_recent_open_channel_posts", return_value=([], None))
    @patch("pipelines.engine.verify_publication_dates")
    @patch("pipelines.engine.TavilyClient")
    def test_weekly_engine_passes_only_independently_verified_web_rows(
        self,
        client_class_mock,
        verify_mock,
        _telegram_mock,
    ) -> None:
        fresh_url = "https://example.test/news/fresh"
        reference_url = "https://example.test/reference"
        client = client_class_mock.return_value
        client.search.return_value = {
            "usage": {"credits": 2},
            "results": [
                {
                    "title": "Свежая новость",
                    "url": fresh_url,
                    "published_date": "2026-07-14",
                    "content": "Новый порядок отчетности.",
                    "score": 0.9,
                },
                {
                    "title": "Справочная страница",
                    "url": reference_url,
                    "published_date": "2026-07-14",
                    "content": "Общее описание экологического сбора.",
                    "score": 0.8,
                },
            ],
        }
        client.extract.return_value = {"usage": {"credits": 1}, "results": []}
        verify_mock.return_value = {
            fresh_url: PublicationDateVerification(
                status="verified",
                date_iso="2026-07-13",
                source="html_article_heading",
                tavily_published_date="2026-07-14",
            ),
            reference_url: PublicationDateVerification(
                status="unverified",
                tavily_published_date="2026-07-14",
            ),
        }
        prompts_dir = Path(__file__).resolve().parents[1] / "prompts"
        profile = build_rop_profile(prompts_dir)

        rows, usage = run_budget_pipeline(
            profile,
            profile.prompt_path.read_text(encoding="utf-8"),
            "tavily-key",
            date(2026, 7, 14),
        )

        self.assertEqual([row["url"] for row in rows], [fresh_url])
        self.assertEqual(rows[0]["date"], "2026-07-13")
        self.assertEqual(usage["web_items_unverified_date_dropped"], 1)


if __name__ == "__main__":
    unittest.main()
