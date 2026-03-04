"""API settings."""

from pydantic import field_validator
from pydantic_settings import BaseSettings


class ApiSettings(BaseSettings):
    """API settings"""

    name: str = "async-titiler"
    cors_origins: str = "*"
    cachecontrol: str = "public, max-age=3600"
    root_path: str = ""
    debug: bool = False
    template_directory: str | None = None

    model_config = {
        "env_prefix": "TITILER_ASYNC_API_",
        "env_file": ".env",
        "extra": "ignore",
    }

    @field_validator("cors_origins")
    def parse_cors_origin(cls, v):
        """Parse CORS origins."""
        return [origin.strip() for origin in v.split(",")]
