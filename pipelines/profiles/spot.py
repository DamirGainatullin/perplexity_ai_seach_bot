from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_spot_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="spot",
        command="/spot",
        prompt_path=prompts_dir / "spot.txt",
        report_label="SPOT",
        official_domains=frozenset(
            {
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "government.ru",
                "nalog.gov.ru",
                "minfin.gov.ru",
                "minpromtorg.gov.ru",
                "mincyfry.gov.ru",
                "digital.gov.ru",
                "customs.ru",
                "customs.gov.ru",
                "rosakkreditatsiya.gov.ru",
                "rosaccreditation.gov.ru",
            }
        ),
        media_domains=frozenset(
            {
                "consultant.ru",
                "garant.ru",
                "kontur-extern.ru",
                "eg-online.ru",
                "tadviser.ru",
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
            "спот",
            "подтверждени",
            "ожидания товаров",
            "фнс",
            "налог",
            "тамож",
            "вэд",
            "росаккредитац",
            "минфин",
            "минцифры",
            "электронн",
            "документооборот",
        ),
        legal_keywords=(
            "нпа",
            "проект",
            "постановлен",
            "приказ",
            "закон",
            "разъяснен",
            "фнс",
            "минфин",
            "тамож",
            "росаккредитац",
            "цифров",
            "суд",
        ),
        category_rules=(
            CategoryRule("НПА и проекты НПА по СПОТ", ("нпа", "проект", "постановлен", "приказ", "закон")),
            CategoryRule("Запуск и функциональные изменения системы", ("запуск", "доработк", "функционал", "сервис", "система")),
            CategoryRule("ФНС, Минфин и налоговое администрирование", ("фнс", "налог", "минфин", "контроль")),
            CategoryRule("Таможня, ВЭД и аккредитация", ("тамож", "вэд", "росаккредитац", "импорт", "экспорт")),
            CategoryRule("ЭДО и цифровые требования", ("эдо", "документооборот", "цифров", "формат", "сведения")),
            CategoryRule("Судебная практика и споры", ("суд", "арбитраж", "спор", "дело")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="Россия система СПОТ подтверждение ожидания товаров изменения за 7 дней НПА разъяснения судебная практика",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_tax_system",
                query="СПОТ ФНС подтверждение ожидания товаров официальные изменения и новости за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_regulation",
                query="СПОТ новые постановления приказы и проекты НПА за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_cross_agency",
                query="СПОТ Минфин Минпромторг Минцифры таможня Росаккредитация разъяснения и новости за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_publication",
                query="publication.pravo.gov.ru СПОТ подтверждение ожидания товаров 2026",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости СПОТ подтверждение ожидания товаров Россия за 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="аналитика юрфирм по СПОТ подтверждению ожидания товаров и ВЭД Россия за 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="СПОТ Россия фактические правовые изменения разъяснения ФНС таможни и судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )
