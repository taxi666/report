from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "tracking-api"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_tracking_topic: str = "tracking_events"
    kafka_client_id: str = "tracking-api"
    kafka_consumer_group_id: str = "tracking-clickhouse-writer"
    kafka_consumer_auto_offset_reset: str = "earliest"
    consumer_batch_size: int = 500
    consumer_flush_interval_seconds: float = 1.0
    clickhouse_host: str = "127.0.0.1"
    clickhouse_http_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "analytics"
    clickhouse_tracking_table: str = "tracking_events"
    clickhouse_connect_timeout_seconds: float = 5.0

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()
