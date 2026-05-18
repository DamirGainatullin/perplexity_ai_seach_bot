from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_metanol_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="metanol",
        command="/metanol",
        prompt_path=prompts_dir / "metanol.txt",
        report_label="METANOL",
        official_domains=frozenset(
            {
                "szrf.pravo.gov.ru",
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "vsrf.ru",
                "kad.arbitr.ru",
                "regulation.gov.ru",
                "minpromtorg.gov.ru",
                "rpn.gov.ru",
                "gisp.gov.ru",
                "gosnadzor.ru",
                "мвд.рф",
                "xn--b1aew.xn--p1ai",
            }
        ),
        media_domains=frozenset(
            {
                "consultant.ru",
                "garant.ru",
                "rg.ru",
            }
        ),
        law_firm_domains=frozenset(
            {
                "pgplaw.ru",
                "nextons.ru",
                "kept.ru",
                "bgplaw.com",
            }
        ),
        topic_keywords=(
            "метанол",
            "метанолсодержащ",
            "метилов",
            "ядовит",
            "фз 108",
            "108-фз",
            "спирт",
            "маркиров",
            "утилиз",
            "перевоз",
            "хранен",
            "производств",
        ),
        legal_keywords=(
            "нпа",
            "проект",
            "постановлен",
            "приказ",
            "федеральн",
            "закон",
            "лиценз",
            "ответствен",
            "разъяснен",
            "суд",
            "арбитраж",
            "контрол",
            "роспотребнадзор",
            "росприроднадзор",
            "минпромторг",
        ),
        category_rules=(
            CategoryRule("НПА и проекты НПА", ("проект", "нпа", "постановлен", "приказ", "закон", "регламент")),
            CategoryRule("Производство, хранение и перевозка", ("производств", "хранен", "перевоз", "реализац", "утилиз")),
            CategoryRule("Маркировка и безопасность", ("маркиров", "упаков", "паспорт", "безопасност")),
            CategoryRule("Контроль и надзор", ("роспотребнадзор", "росприроднадзор", "минпромторг", "мвд", "контрол")),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "дело")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="Россия регулирование оборота метанола изменения 7 дней НПА проекты НПА судебная практика разъяснения",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_news",
                query="ФЗ 108-ФЗ метанол новые постановления приказы и официальные разъяснения за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="регулирование метанола Роспотребнадзор Росприроднадзор Минпромторг официальные новости 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="publication.pravo.gov.ru метанол постановление приказ 2026",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="regulation.gov.ru проекты НПА метанол и метанолсодержащие жидкости за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости регулирования метанола Россия НПА проекты НПА 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="обзоры юрфирм метанол оборот метанола правовые изменения Россия 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="метанол и метанолсодержащие жидкости Россия правовые изменения НПА судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )

