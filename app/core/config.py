from pydantic_settings import BaseSettings
import os

class Settings(BaseSettings):
    REDIS_NODES: str = os.getenv("REDIS_NODES", "redis://redis_7070:6379,redis://redis_7071:6379")
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "")
    REDIS_DB: int = int(os.getenv("REDIS_DB", 0))

    VIRTUAL_NODES: int = int(os.getenv("VIRTUAL_NODES", 100))
    BATCH_INTERVAL_SECONDS: float = float(os.getenv("BATCH_INTERVAL_SECONDS", 30.0))

    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    API_PREFIX: str = os.getenv("API_PREFIX", "/api/v1")

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
