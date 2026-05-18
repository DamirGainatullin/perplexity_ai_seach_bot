from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_logistics_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="logistics",
        command="/logistics",
        prompt_path=prompts_dir / "logistics.txt",
        report_label="LOGISTICS",
        official_domains=frozenset(
            {
                "szrf.pravo.gov.ru",
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "vsrf.ru",
                "mintrans.gov.ru",
            }
        ),
        media_domains=frozenset(
            {
                "consultant.ru",
                "garant.ru",
                "rg.ru",
                "kontur-extern.ru",
                "ul.su",
            }
        ),
        law_firm_domains=frozenset(
            {
                "denuo.legal",
                "alumnipartners.ru",
                "pgplaw.ru",
                "nextons.ru",
                "kept.ru",
                "bgplaw.com",
            }
        ),
        topic_keywords=(
            "логист",
            "транспорт",
            "перевоз",
            "экспедиц",
            "морск",
            "железнодорож",
            "автоперевоз",
            "минтранс",
            "гис эпд",
            "каботаж",
            "накладн",
        ),
        legal_keywords=(
            "нпа",
            "постановлен",
            "приказ",
            "федеральн",
            "закон",
            "поправк",
            "разъяснен",
            "суд",
            "верховн",
            "арбитраж",
        ),
        category_rules=(
            CategoryRule("Морское право", ("морск", "мореплав", "судоход")),
            CategoryRule("Право железнодорожных перевозок", ("железнодорож", "ж/д", "ржд")),
            CategoryRule("Автоперевозки", ("авто", "каботаж", "грузоперевоз")),
            CategoryRule("Договор транспортной экспедиции", ("экспедиц",)),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "верховн")),
            CategoryRule("Транспортное право", ("нпа", "постановлен", "приказ", "закон")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="Россия логистика транспортное право изменения 7 дней НПА поправки судебная практика разъяснения",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_news",
                query="Россия новые постановления приказы по перевозкам и логистике за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="Верховный суд и официальные разъяснения по транспортному праву за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="publication.pravo.gov.ru транспорт перевозки экспедиция постановление 2026",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="pravo.gov.ru логистика автоперевозки железнодорожные перевозки НПА май 2026",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости логистики и перевозок Россия НПА поправки 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="обзоры юрфирм логистика транспортное право Россия за 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="логистика и перевозки Россия правовые изменения НПА поправки судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )

