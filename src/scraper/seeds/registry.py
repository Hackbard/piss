import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field

from scraper.config import get_settings

settings = get_settings()


class LandtagRegistryEntry(BaseModel):
    key_prefix: str = Field(..., description="Prefix for generated seed keys")
    state: str = Field(..., description="State name")
    parliament: str = Field(..., description="Parliament name")
    wikipedia_index_title: str = Field(..., description="Wikipedia index page title")
    member_list_search: List[str] = Field(..., description="Search queries for member lists")


class LandtageRegistry(BaseModel):
    version: int = Field(..., description="Registry version")
    defaults: Dict[str, Any] = Field(default_factory=dict, description="Default settings")
    landtage: Dict[str, LandtagRegistryEntry] = Field(..., description="Landtag entries")


def load_registry(registry_path: Optional[Path] = None) -> LandtageRegistry:
    """Load and validate the landtage registry."""
    if registry_path is None:
        registry_path = settings.scraper_registry_path

    if not registry_path.exists():
        raise FileNotFoundError(f"Registry file not found: {registry_path}")

    with open(registry_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    return LandtageRegistry.model_validate(data)


def get_registry_hash(registry_path: Optional[Path] = None) -> str:
    """Get SHA256 hash of registry file for provenance."""
    if registry_path is None:
        registry_path = settings.scraper_registry_path

    if not registry_path.exists():
        return ""

    content = registry_path.read_text(encoding="utf-8")
    return hashlib.sha256(content.encode("utf-8")).hexdigest()

