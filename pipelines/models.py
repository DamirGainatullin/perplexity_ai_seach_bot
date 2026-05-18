from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BudgetConfig:
    target_credits: int = 30
    search_run_limit: int = 8
    max_results: int = 6
    extract_url_limit: int = 8


@dataclass(frozen=True)
class QueryRule:
    strategy: str
    query: str
    topic: str
    domain_scope: str


@dataclass(frozen=True)
class CategoryRule:
    name: str
    keywords: tuple[str, ...]


@dataclass(frozen=True)
class PromptProfile:
    name: str
    command: str
    prompt_path: Path
    report_label: str
    official_domains: frozenset[str]
    media_domains: frozenset[str]
    law_firm_domains: frozenset[str]
    topic_keywords: tuple[str, ...]
    legal_keywords: tuple[str, ...]
    category_rules: tuple[CategoryRule, ...]
    query_plan: tuple[QueryRule, ...]
    budget: BudgetConfig

