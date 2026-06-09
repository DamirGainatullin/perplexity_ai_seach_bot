from pathlib import Path

from pipelines.models import BudgetConfig, CategoryRule, PromptProfile, QueryRule


def build_sales_profile(prompts_dir: Path) -> PromptProfile:
    return PromptProfile(
        name="sales",
        command="/sales",
        prompt_path=prompts_dir / "sales.txt",
        report_label="SALES",
        official_domains=frozenset(
            {
                "pravo.gov.ru",
                "publication.pravo.gov.ru",
                "kremlin.ru",
                "government.ru",
                "duma.gov.ru",
                "council.gov.ru",
                "ksrf.ru",
                "vsrf.ru",
                "regulation.gov.ru",
                "minjust.gov.ru",
            }
        ),
        media_domains=frozenset(
            {
                "consultant.ru",
                "garant.ru",
                "m-logos.ru",
                "zakon.ru",
                "pravo.ru",
                "lawfirm.ru",
                "boosty.to",
            }
        ),
        law_firm_domains=frozenset(
            {
                "pgplaw.ru",
                "epam.ru",
                "tedo.ru",
                "maximalegal.ru",
                "kiap.com",
                "alrud.ru",
                "alumnipartners.ru",
                "denuo.legal",
                "nextons.ru",
                "kept.ru",
            }
        ),
        topic_keywords=(
            "поставка",
            "договор",
            "продаж",
            "вэд",
            "внешнеэконом",
            "техническ",
            "регламент",
            "нефтехим",
            "качество товара",
            "ответственность производителя",
            "сертификац",
            "декларирован",
        ),
        legal_keywords=(
            "нпа",
            "проект",
            "постановлен",
            "приказ",
            "закон",
            "разъяснен",
            "пленум",
            "верховн",
            "конституцион",
            "суд",
            "арбитраж",
            "техрегламент",
        ),
        category_rules=(
            CategoryRule("Договор поставки и коммерческие условия", ("поставка", "договор", "покупатель", "продавец", "исполнение")),
            CategoryRule("ВЭД и международная торговля", ("вэд", "внешнеэконом", "импорт", "экспорт", "тамож")),
            CategoryRule("Техническое регулирование и обязательные требования", ("техническ", "техрегламент", "сертификац", "декларирован", "стандарт")),
            CategoryRule("Качество товара и ответственность производителя", ("качество", "недостат", "ответственность производителя", "гаранти", "безопасность")),
            CategoryRule("Судебная практика", ("суд", "арбитраж", "пленум", "обзор практики", "дело")),
            CategoryRule("Законопроекты и официальные разъяснения", ("нпа", "проект", "разъяснен", "постановлен", "приказ")),
        ),
        query_plan=(
            QueryRule(
                strategy="baseline_general",
                query="Россия продажи товаров договор поставки ВЭД техническое регулирование изменения за 7 дней НПА судебная практика",
                topic="general",
                domain_scope="none",
            ),
            QueryRule(
                strategy="official_contract_and_trade",
                query="договор поставки товаров новые официальные изменения и разъяснения за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_courts",
                query="Верховный суд Конституционный суд и официальная практика по поставке товаров и продажам за 7 дней",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="official_regulation",
                query="regulation.gov.ru и publication.pravo.gov.ru поставка товаров ВЭД техническое регулирование 2026",
                topic="news",
                domain_scope="official",
            ),
            QueryRule(
                strategy="legal_media_general",
                query="правовые новости договор поставки ВЭД техническое регулирование товаров Россия за 7 дней",
                topic="general",
                domain_scope="legal_media",
            ),
            QueryRule(
                strategy="law_firms_news",
                query="аналитика юрфирм поставка товаров ВЭД ответственность производителя и качество товара Россия за 7 дней",
                topic="news",
                domain_scope="law_firms",
            ),
            QueryRule(
                strategy="sales_ved_focus",
                query="продажа товаров ВЭД Россия официальные разъяснения и судебная практика за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
            QueryRule(
                strategy="sales_tech_regulation",
                query="нефтехимическая продукция техническое регулирование качество товара и обязательные требования Россия за 7 дней",
                topic="news",
                domain_scope="all_ru",
            ),
        ),
        budget=BudgetConfig(target_credits=30, search_run_limit=8, max_results=6, extract_url_limit=8),
    )
