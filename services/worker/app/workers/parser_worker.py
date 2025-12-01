# services/worker/app/workers/parser_worker.py
import os
import io
import json
import time
import uuid
import logging
from typing import List

import boto3
import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import requests
from kafka import KafkaConsumer
<<<<<<< HEAD
=======
from kafka.errors import NoBrokersAvailable
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("parser-worker")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MATERIALS_PARSE_TOPIC = os.getenv("MATERIALS_PARSE_TOPIC", "materials.parse")
GROUP_ID = os.getenv("PARSER_GROUP_ID", "parser-group")

<<<<<<< HEAD
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "materials")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
=======
S3_ENDPOINT = (
    os.getenv("S3_ENDPOINT")
    or os.getenv("MINIO_ENDPOINT")
    or "http://minio:9000"
)
S3_ACCESS = (
    os.getenv("S3_ACCESS_KEY")
    or os.getenv("MINIO_ACCESS_KEY")
    or os.getenv("MINIO_ROOT_USER")
    or "minioadmin"
)
S3_SECRET = (
    os.getenv("S3_SECRET_KEY")
    or os.getenv("MINIO_SECRET_KEY")
    or os.getenv("MINIO_ROOT_PASSWORD")
    or "minioadmin"
)
S3_BUCKET = (
    os.getenv("S3_BUCKET")
    or os.getenv("MINIO_BUCKET")
    or "assistant-teacher"
)
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX_CHUNKS = os.getenv("ES_INDEX_CHUNKS", "chunks")

<<<<<<< HEAD
EMBED_URL = os.getenv("EMBEDDINGS_URL", "http://vectorizer:8080/embed")
=======
EMBED_URL = os.getenv("EMBEDDINGS_URL", "http://vectorizer:8001/embed")
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))

TESS_LANG = os.getenv("TESS_LANG", "eng")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "800"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "120"))

<<<<<<< HEAD
=======

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def chunk_text(text: str, size: int, overlap: int) -> List[str]:
    text = (text or "").strip()
    if not text:
        return []
    parts = []
    start = 0
    n = len(text)
    while start < n:
        end = min(n, start + size)
        parts.append(text[start:end])
        if end == n:
            break
        start = max(0, end - overlap)
    return parts

<<<<<<< HEAD
=======

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def page_text_with_ocr(doc: fitz.Document, page_num: int) -> str:
    page = doc.load_page(page_num)
    plain = page.get_text("text") or ""

<<<<<<< HEAD
    # OCR картинок на странице
=======
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
    ocr_blocks: List[str] = []
    try:
        for img in page.get_images(full=True):
            xref = img[0]
            base = doc.extract_image(xref)
            img_bytes = base["image"]
            pil = Image.open(io.BytesIO(img_bytes)).convert("RGB")
            txt = pytesseract.image_to_string(pil, lang=TESS_LANG)
            if txt and txt.strip():
                ocr_blocks.append(txt.strip())
    except Exception as e:
        log.warning("OCR failed on page %d: %s", page_num + 1, e)

    out = (plain + ("\n" + "\n".join(ocr_blocks) if ocr_blocks else "")).strip()
    return out

<<<<<<< HEAD
=======

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def embed_batch(chunks: List[str], batch: int = 32) -> List[List[float]]:
    vectors: List[List[float]] = []
    for i in range(0, len(chunks), batch):
        part = chunks[i:i + batch]
        resp = requests.post(EMBED_URL, json={"texts": part}, timeout=300)
        resp.raise_for_status()
        vectors.extend(resp.json()["vectors"])
    return vectors

<<<<<<< HEAD
=======

def make_consumer() -> KafkaConsumer:
    while True:
        try:
            c = KafkaConsumer(
                MATERIALS_PARSE_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                enable_auto_commit=True,
                auto_offset_reset="earliest",
                consumer_timeout_ms=1000,
                max_poll_interval_ms=900000,
            )
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP)
            return c
        except NoBrokersAvailable as e:
            log.warning("Kafka not available: %s, retry in 5s", e)
            time.sleep(5)


>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def main():
    es = Elasticsearch(ES_URL, request_timeout=60)
    s3 = boto3.client(
        "s3",
<<<<<<< HEAD
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
    consumer = KafkaConsumer(
        MATERIALS_PARSE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        consumer_timeout_ms=0,
    )
    log.info("Parser worker started. Topic=%s", MATERIALS_PARSE_TOPIC)

    for msg in consumer:
        try:
            payload = msg.value
            course_id = payload["course_id"]
            bucket = payload.get("bucket", MINIO_BUCKET)
            key = payload["key"]
            filename = payload.get("filename", os.path.basename(key))
            material_id = str(uuid.uuid4())

            log.info("Processing s3://%s/%s (course=%s)", bucket, key, course_id)
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = obj["Body"].read()

            with fitz.open(stream=data, filetype="pdf") as doc:
                for page_i in range(doc.page_count):
                    text = page_text_with_ocr(doc, page_i)
                    if not text or len(text) < 20:
                        continue
                    chunks = chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)
                    if not chunks:
                        continue
                    vectors = embed_batch(chunks)

                    for chunk_text_, vec in zip(chunks, vectors):
                        doc_id = str(uuid.uuid4())
                        es.index(index=ES_INDEX_CHUNKS, id=doc_id, document={
                            "course_id": course_id,
                            "material_id": material_id,
                            "filename": filename,
                            "page": page_i + 1,
                            "content": chunk_text_,
                            "vector": vec
                        })
            log.info("Done: %s", filename)
        except Exception as e:
            log.exception("Failed to process message: %s", e)
            time.sleep(1)
=======
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
    )
    consumer = make_consumer()
    log.info("Parser worker started. Topic=%s", MATERIALS_PARSE_TOPIC)

    while True:
        for msg in consumer:
            try:
                payload = msg.value
                course_id = payload["course_id"]
                bucket = payload.get("bucket", S3_BUCKET)
                key = payload["key"]
                filename = payload.get("filename", os.path.basename(key))
                material_id = str(uuid.uuid4())

                log.info("Processing s3://%s/%s (course=%s)", bucket, key, course_id)
                obj = s3.get_object(Bucket=bucket, Key=key)
                data = obj["Body"].read()

                with fitz.open(stream=data, filetype="pdf") as doc:
                    for page_i in range(doc.page_count):
                        text = page_text_with_ocr(doc, page_i)
                        if not text or len(text) < 20:
                            continue
                        chunks = chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)
                        if not chunks:
                            continue
                        vectors = embed_batch(chunks)

                        for chunk_text_, vec in zip(chunks, vectors):
                            doc_id = str(uuid.uuid4())
                            es.index(index=ES_INDEX_CHUNKS, id=doc_id, document={
                                "course_id": course_id,
                                "material_id": material_id,
                                "filename": filename,
                                "page": page_i + 1,
                                "content": chunk_text_,
                                "vector": vec,
                            })
                log.info("Done: %s", filename)
            except Exception as e:
                log.exception("Failed to process message: %s", e)
                time.sleep(1)

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))

if __name__ == "__main__":
    main()