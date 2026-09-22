from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed access to all env vars. Fails fast at startup if something required is missing."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    kafka_bootstrap_servers: str
    ready_check_timeout_seconds: float = 2.0


settings = Settings()
