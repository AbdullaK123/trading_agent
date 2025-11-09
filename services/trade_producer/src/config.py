from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List

class AppConfig(BaseSettings):

    model_config = SettingsConfigDict(
        extra="forbid",
        env_file=".env"
    )
    kafka_broker_address: Optional[str] = None
    kafka_topic: str
    product_ids: List[str]


config = AppConfig()
