from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_chesny_znak_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="chesny_znak",
        command="/chesny_znak",
        prompt_path=prompts_dir / "chesny_znak.txt",
        report_label="CHESNY_ZNAK",
        official_domains=frozenset(
            {
                "честныйзнак.рф",
                "markirovka.ru",
                "sozd.duma.gov.ru",
                "regulation.gov.ru",
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "szrf.pravo.gov.ru",
                "vsrf.ru",
                "kad.arbitr.ru",
            }
        ),
        media_domains=frozenset(
            {
                "consultant.ru",
                "garant.ru",
                "tochka.com",
                "kontur.ru",
                "kkm.ru",
                "sudact.ru",
            }
        ),
        law_firm_domains=frozenset(),
        topic_keywords=(
            "честный знак",
            "маркиров",
            "црпт",
            "тн вэд",
            "окпд2",
            "эдо",
            "код маркировки",
            "прослеживаем",
            "пилот",
            "средств индивидуальной защиты",
            "шины",
            "масла",
        ),
        legal_keywords=(
            "нпа",
            "проект",
            "постановлен",
            "приказ",
            "федеральн",
            "закон",
            "поправк",
            "разъяснен",
            "суд",
            "арбитраж",
            "црпт",
            "регламент",
        ),
        category_rules=(
            CategoryRule("НПА и проекты НПА по маркировке", ("нпа", "проект", "постановлен", "приказ", "закон")),
            CategoryRule("Товарные группы и сроки маркировки", ("товар", "группа", "срок", "ввод", "пилот", "эксперимент")),
            CategoryRule("ВЭД, ТН ВЭД и ОКПД2", ("тн вэд", "окпд2", "импорт", "экспорт", "вэд")),
            CategoryRule("ЭДО и работа в системе", ("эдо", "документооборот", "црпт", "код маркировки", "оператор")),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "дело", "спор")),
            CategoryRule("Разъяснения госорганов и ЦРПТ", ("разъяснен", "црпт", "честный знак", "маркировка")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="Россия Честный знак обязательная маркировка товаров изменения за 7 дней НПА проекты НПА разъяснения судебная практика",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_marking_rules",
                query="Честный знак маркировка новые постановления приказы и правила за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_product_groups",
                query="маркировка новые товарные группы пилоты ТН ВЭД ОКПД2 официальные изменения за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_courts_and_bills",
                query="судебная практика и законопроекты по маркировке Честный знак за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="industry_platform_updates",
                query="ЦРПТ Честный знак функциональные изменения системы маркировки разъяснения за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости маркировки товаров Честный знак Россия за 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="other_court_and_market",
                query="арбитражные споры и практика по маркировке товаров Честный знак за 7 дней",
                topic="news",
                domain_scope="other",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="маркировка товаров Честный знак Россия фактические правовые изменения разъяснения судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )
