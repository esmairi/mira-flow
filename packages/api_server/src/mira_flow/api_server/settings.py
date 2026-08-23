"""api_server-only settings — env vars (or a local .env), validated once."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiServerSettings(BaseSettings):
    """Attributes:
    workflows_dir: Directory of workflow JSON configs to serve. Loaded
        once at app startup (see `app.py`'s lifespan) — restart the
        process to pick up added/changed workflow files.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    workflows_dir: str = "workflows"


api_server_settings = ApiServerSettings()
