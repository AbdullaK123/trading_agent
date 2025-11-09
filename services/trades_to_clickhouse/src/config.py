from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppConfig(BaseSettings):

    model_config = SettingsConfigDict(
        extra="forbid",
        env_file=".env"
    )
    kafka_broker_address: Optional[str] = None
    kafka_topic: str
    kafka_consumer_group: str
    ohlcv_window_seconds: int
    clickhouse_host: str
    clickhouse_port: int


config = AppConfig()
