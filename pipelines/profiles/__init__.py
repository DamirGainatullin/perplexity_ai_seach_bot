from pathlib import Path

from pipelines.models import PromptProfile
from pipelines.profiles.chesny_znak import build_chesny_znak_profile
from pipelines.profiles.logistics import build_logistics_profile
from pipelines.profiles.metanol import build_metanol_profile
from pipelines.profiles.precursors import build_precursors_profile
from pipelines.profiles.rop import build_rop_profile
from pipelines.profiles.sales import build_sales_profile
from pipelines.profiles.spot import build_spot_profile


def load_profiles(prompts_dir: Path) -> dict[str, PromptProfile]:
    profiles = [
        build_logistics_profile(prompts_dir),
        build_metanol_profile(prompts_dir),
        build_precursors_profile(prompts_dir),
        build_rop_profile(prompts_dir),
        build_chesny_znak_profile(prompts_dir),
        build_spot_profile(prompts_dir),
        build_sales_profile(prompts_dir),
    ]
    result: dict[str, PromptProfile] = {}
    for profile in profiles:
        if profile.prompt_path.exists():
            result[profile.command] = profile
    return result
