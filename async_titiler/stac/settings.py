"""API settings."""

from pydantic_settings import BaseSettings


class STACAPISettings(BaseSettings):
    """STAC API settings"""

    url: str | None = None

    model_config = {
        "env_prefix": "ATITILER_STACAPI_",
        "env_file": ".env",
        "extra": "ignore",
    }


class ItemsSettings(BaseSettings):
    """STAC API Items settings"""

    max_items: int = 100
    items_per_page: int = 10

    model_config = {
        "env_prefix": "ATITILER_STACAPI_",
        "env_file": ".env",
        "extra": "ignore",
    }
