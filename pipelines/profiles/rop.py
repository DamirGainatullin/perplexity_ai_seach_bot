from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_rop_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="rop",
        command="/rop",
        prompt_path=prompts_dir / "rop.txt",
        report_label="ROP",
        official_domains=frozenset(
            {
                "szrf.pravo.gov.ru",
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "vsrf.ru",
                "regulation.gov.ru",
                "rpn.gov.ru",
                "reo.ru",
                "xn--b1aew.xn--p1ai",
            }
        ),
        media_domains=frozenset(
            {
                "pravo.ru",
                "zakon.ru",
                "consultant.ru",
                "garant.ru",
            }
        ),
        law_firm_domains=frozenset(
            {
                "pgplaw.ru",
                "delfi-law.ru",
                "zharov.eco",
            }
        ),
        topic_keywords=(
            "роп",
            "расширенн",
            "ответственн",
            "производител",
            "импортер",
            "экологическ сбор",
            "утилизац",
            "упаковк",
            "реестр утилизатор",
            "отчетност",
            "89-фз",
            "451-фз",
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
            "минприрод",
            "росприроднадзор",
            "экологическ сбор",
            "реестр",
            "утилизатор",
        ),
        category_rules=(
            CategoryRule("НПА и проекты НПА по РОП", ("нпа", "проект", "постановлен", "приказ", "89-фз", "451-фз")),
            CategoryRule("Экологический сбор", ("экологическ сбор", "сбор", "уплат")),
            CategoryRule("Отчетность и подтверждение утилизации", ("отчетност", "подтвержден", "утилизац", "норматив")),
            CategoryRule("Реестр утилизаторов", ("реестр утилизатор", "утилизатор")),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "дело")),
            CategoryRule("Разъяснения Минприроды/РПН", ("минприрод", "росприроднадзор", "разъяснен")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="РОП расширенная ответственность производителя и импортера правовые изменения 7 дней НПА проекты НПА судебная практика",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_news",
                query="экологический сбор РОП новые постановления и приказы России за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="реестр утилизаторов и подтверждение утилизации отходов разъяснения РПН за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="РОП отчетность производителей и импортеров изменения требований к упаковке за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="проекты НПА regulation.gov.ru по РОП экологическому сбору и утилизации за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости РОП экологический сбор судебная практика и разъяснения Россия 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="аналитика юрфирм по РОП экологическому сбору и утилизации Россия последние 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="РОП Россия фактические правовые изменения новые НПА поправки разъяснения и судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )

