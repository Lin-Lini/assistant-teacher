<<<<<<< HEAD
from pydantic import BaseModel
import os

class Settings(BaseModel):
    app_env: str = os.getenv("APP_ENV", "dev")
    kafka_bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    database_url: str = os.getenv("DATABASE_URL", "")
    es_url: str = os.getenv("ES_URL", "http://es:9200")
    embeddings_url: str = os.getenv("EMBEDDINGS_URL", "http://vectorizer:8080/embed")
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access: str = os.getenv("MINIO_ACCESS_KEY", "minio")
    minio_secret: str = os.getenv("MINIO_SECRET_KEY", "minio12345")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "materials")

settings = Settings()
=======
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_ENV: str = "dev"

    # Elasticsearch
    ES_URL: str = os.getenv("ES_URL", "http://elasticsearch:9200")

    # Векторизатор
    VECTORIZER_URL: str = os.getenv("VECTORIZER_URL", "http://vectorizer:8001")
    EMBEDDINGS_URL: str = os.getenv(
        "EMBEDDINGS_URL",
        f"{VECTORIZER_URL}/embed",
    )

    S3_ENDPOINT: str = (
        os.getenv("S3_ENDPOINT")
        or os.getenv("MINIO_ENDPOINT")
        or "http://minio:9000"
    )
    S3_ACCESS_KEY: str = (
        os.getenv("S3_ACCESS_KEY")
        or os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("MINIO_ROOT_USER")
        or "minioadmin"
    )
    S3_SECRET_KEY: str = (
        os.getenv("S3_SECRET_KEY")
        or os.getenv("MINIO_SECRET_KEY")
        or os.getenv("MINIO_ROOT_PASSWORD")
        or "minioadmin"
    )
    S3_BUCKET: str = (
        os.getenv("S3_BUCKET")
        or os.getenv("MINIO_BUCKET")
        or "assistant-teacher"
    )

    # Локальная LLM (llama.cpp server с Qwen2.5)
    LLM_URL: str = os.getenv(
        "LLM_URL",
        "http://generator:8002/v1/chat/completions",
    )

    class Config:
        env_file = ".env"


settings = Settings()

def _minio_endpoint(self) -> str:
    return self.S3_ENDPOINT

def _minio_access(self) -> str:
    return self.S3_ACCESS_KEY

def _minio_secret(self) -> str:
    return self.S3_SECRET_KEY

def _minio_bucket(self) -> str:
    return self.S3_BUCKET

setattr(Settings, "minio_endpoint", property(_minio_endpoint))
setattr(Settings, "minio_access", property(_minio_access))
setattr(Settings, "minio_secret", property(_minio_secret))
setattr(Settings, "minio_bucket", property(_minio_bucket))
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
