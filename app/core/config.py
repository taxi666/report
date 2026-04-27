from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "tracking-api"
    queue_maxsize: int = 10000
    queue_worker_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_tracking_topic: str = "tracking_events"
    kafka_client_id: str = "tracking-api"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
