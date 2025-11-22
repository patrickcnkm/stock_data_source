# app/settings.py
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # ---- App config (env-overridable) ----
    duckdb_path: str = "data/quant.duckdb"
    redis_url: str = "redis://localhost:6379/0"

    # Futu/OpenD
    futu_opend_ip: str = "127.0.0.1"
    futu_opend_quote_port: int = 11111
    futu_sub_total: int = 300  # your quota cap

    # Universe management
    hk_universe_symbols: str | None = None  # comma separated list, e.g., "HK.00700,HK.00005"

    # Alerts
    feishu_webhook: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_pass: str | None = None
    alert_email_to: str | None = None

    # Pydantic v2 config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

@lru_cache
def get_settings() -> Settings:
    return Settings()