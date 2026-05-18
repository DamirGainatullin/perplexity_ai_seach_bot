from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_precursors_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="precursors",
        command="/precursors",
        prompt_path=prompts_dir / "precursors.txt",
        report_label="PRECURSORS",
        official_domains=frozenset(
            {
                "szrf.pravo.gov.ru",
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "vsrf.ru",
                "xn--b1aew.xn--p1ai",
                "rpn.gov.ru",
                "reo.ru",
            }
        ),
        media_domains=frozenset(
            {
                "pravo.ru",
                "zakon.ru",
                "consultant.ru",
                "garant.ru",
                "rg.ru",
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
            "прекурсор",
            "наркотическ",
            "контролируем",
            "оборот",
            "лиценз",
            "перечн",
            "хранен",
            "перевоз",
            "учет",
            "использован",
            "веществ",
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
            "лиценз",
            "контрол",
            "мвд",
            "перечень",
        ),
        category_rules=(
            CategoryRule("НПА и проекты НПА", ("нпа", "проект", "постановлен", "приказ", "закон", "поправк")),
            CategoryRule("Лицензирование и требования к бизнесу", ("лиценз", "требован", "обязан", "операц")),
            CategoryRule("Оборот и контроль прекурсоров", ("прекурсор", "оборот", "контрол", "учет", "хранен", "перевоз")),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "дело", "приговор")),
            CategoryRule("Разъяснения госорганов", ("разъяснен", "мвд", "росприроднадзор", "госорган")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="контроль за оборотом прекурсоров в РФ изменения за 7 дней НПА проекты НПА судебная практика разъяснения",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_news",
                query="прекурсоры новые постановления и приказы России за 7 дней publication.pravo.gov.ru pravo.gov.ru",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="изменение перечней прекурсоров и контролируемых веществ Россия за 7 дней официальные источники",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="лицензирование операций с прекурсорами разъяснения госорганов Россия за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_news",
                query="контроль оборота наркотических средств и прекурсоров разъяснения МВД и судов за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости о прекурсорах и контролируемых веществах Россия НПА поправки 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="аналитика юрфирм по регулированию прекурсоров лицензированию и контролю Россия 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="all_ru_news",
                query="прекурсоры Россия фактические правовые изменения новые НПА судебная практика разъяснения за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )

