from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Host-side default: repo_root/models/isolation_forest.joblib, matching scripts/train.py's own
# default (same parents[2] depth from backend/app/config.py as from backend/scripts/train.py).
# Inside the backend container this directory layout doesn't exist (only backend/ is copied in,
# per the Dockerfile) — docker-compose.yml sets MODEL_PATH explicitly there instead (PLANNING.md
# AD-21/AD-24).
_DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[2] / "models" / "isolation_forest.joblib"


class Settings(BaseSettings):
    """Typed access to all env vars. Fails fast at startup if something required is missing."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    kafka_bootstrap_servers: str
    ready_check_timeout_seconds: float = 2.0
    model_path: Path = _DEFAULT_MODEL_PATH


settings = Settings()
