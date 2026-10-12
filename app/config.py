from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    rabbitmq_url: str
    api_key: SecretStr

    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    max_delivery_attempts: int = Field(default=3, ge=1)
    retry_base_delay: float = Field(default=2.0, gt=0)

    webhook_timeout: float = Field(default=5.0, gt=0)

    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
