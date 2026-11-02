from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    database_url: str = 'postgresql+psycopg://openlake:openlake@localhost:5432/openlake'
    secret_key: str = 'CHANGE_ME_IN_PRODUCTION_USE_LONG_RANDOM_VALUE'
    encryption_key: str = ''
    data_dir: str = '/app/storage'
    frontend_origin: str = 'http://localhost:3000'
    cookie_secure: bool = False
    max_upload_mb: int = 20
    max_query_rows: int = 500
    ai_max_output_tokens: int = 900
    spark_url: str = ''
    spark_token: str = ''
    spark_min_bytes: int = 256 * 1024 * 1024
    redis_url: str = ''
    cache_ttl_seconds: int = 300
    iceberg_auto_maintenance: bool = False
    iceberg_maintenance_interval_minutes: int = 1440

@lru_cache

def get_settings() -> Settings:
    return Settings()
