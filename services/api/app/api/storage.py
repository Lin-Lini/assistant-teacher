from minio import Minio
from .settings import settings
from io import BytesIO
import time

<<<<<<< HEAD
_client = Minio(
    settings.minio_endpoint.replace("http://", ""),
=======
_endpoint = settings.minio_endpoint
if _endpoint.startswith("http://"):
    _endpoint = _endpoint[len("http://"):]
elif _endpoint.startswith("https://"):
    _endpoint = _endpoint[len("https://"):]
_client = Minio(
    _endpoint,
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
    access_key=settings.minio_access,
    secret_key=settings.minio_secret,
    secure=False,
)

def _ensure_bucket():
    for _ in range(10):
        try:
            if not _client.bucket_exists(settings.minio_bucket):
                _client.make_bucket(settings.minio_bucket)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError("MinIO not ready")

_bucket_ready = False

def put_object(object_name: str, data: bytes, content_type: str = "application/octet-stream"):
    global _bucket_ready
    if not _bucket_ready:
      _ensure_bucket()
      _bucket_ready = True
    _client.put_object(settings.minio_bucket, object_name, BytesIO(data), len(data), content_type=content_type)
    return f"s3://{settings.minio_bucket}/{object_name}"