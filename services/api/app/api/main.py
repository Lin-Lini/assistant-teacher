<<<<<<< HEAD
import io
import os
import re
import json
import uuid
import zipfile
from typing import List, Optional, Dict, Any

import boto3
import requests
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from elasticsearch import Elasticsearch
from kafka import KafkaProducer

APP_NAME = "assistant-teacher-api"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
MATERIALS_PARSE_TOPIC = os.getenv("MATERIALS_PARSE_TOPIC", "materials.parse")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "materials")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER", "minioadmin"))
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX_CHUNKS = os.getenv("ES_INDEX_CHUNKS", "chunks")

EMBED_URL = os.getenv("EMBEDDINGS_URL", "http://vectorizer:8001/embed")
TRANSLATE_URL = os.getenv("TRANSLATE_URL", "http://vectorizer:8001/translate")

app = FastAPI(title=APP_NAME)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

s3 = None
es: Optional[Elasticsearch] = None
producer: Optional[KafkaProducer] = None

def _init_s3():
    global s3
    if s3 is None:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        try:
            buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            if MINIO_BUCKET not in buckets:
                s3.create_bucket(Bucket=MINIO_BUCKET)
        except Exception:
            pass

def _init_es():
    global es
    if es is None:
        es = Elasticsearch(ES_URL, request_timeout=60)

def _init_kafka():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=10,
        )

@app.on_event("startup")
def _on_startup():
    _init_s3()
    _init_es()
    _init_kafka()

@app.get("/health")
def health():
    try:
        ping = es.ping() if es else False
    except Exception:
        ping = False
    return {"status": "ok", "es": ping}

_cyrillic_re = re.compile(r"[А-Яа-яЁё]")

def looks_russian(text: str) -> bool:
    return bool(_cyrillic_re.search(text or ""))

def translate(texts: List[str], source: str, target: str) -> List[str]:
    if not texts:
        return []
    try:
        resp = requests.post(TRANSLATE_URL, json={"source": source, "target": target, "texts": texts}, timeout=120)
        if resp.status_code == 204:
            return texts
        resp.raise_for_status()
        return resp.json()["texts"]
    except Exception:
        return texts

def embed(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []
    resp = requests.post(EMBED_URL, json={"texts": texts}, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    return data["vectors"]

def es_knn_search(course_id: str, q_text: str, q_vec: Optional[List[float]], size: int = 10) -> Dict[str, Any]:
    query: Dict[str, Any] = {
        "bool": {
            "filter": [{"term": {"course_id": course_id}}],
            "should": [{"match": {"content": {"query": q_text}}}],
            "minimum_should_match": 0
        }
    }
    body: Dict[str, Any] = {"size": size, "query": query}
    if q_vec is not None:
        body["knn"] = {
            "field": "vector",
            "query_vector": q_vec,
            "k": max(size, 20),
            "num_candidates": 500
        }
    return es.search(index=ES_INDEX_CHUNKS, body=body)

class RagRequest(BaseModel):
    query: str
    course_id: str
    k: int = 8
    max_sentences: int = 5
    lambda_mmr: float = 0.7
    style: str = "paragraph"

class RagAnswer(BaseModel):
    query_used: str
    answer: str
    sources: List[Dict[str, Any]]

@app.post("/materials/upload_zip")
async def upload_zip(file: UploadFile = File(...), course_id: str = Form(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Expected .zip")

    data = await file.read()
    zf = zipfile.ZipFile(io.BytesIO(data))
    uploaded = []

    for name in zf.namelist():
        if not name.lower().endswith(".pdf"):
            continue
        content = zf.read(name)
        basename = os.path.basename(name)
        key = f"materials/{course_id}/{uuid.uuid4()}_{basename}"
        s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=content, ContentType="application/pdf")
        msg = {"course_id": course_id, "bucket": MINIO_BUCKET, "key": key, "filename": basename}
        producer.send(MATERIALS_PARSE_TOPIC, msg)
        uploaded.append({"course_id": course_id, "s3": f"s3://{MINIO_BUCKET}/{key}", "filename": basename})

    producer.flush()
    return {"uploaded": uploaded, "count": len(uploaded)}

@app.get("/search")
def search(q: str, course_id: str, k: int = 10):
    query_original = q
    q_used = q
    try:
        q_vec = embed([q_used])[0]
    except Exception:
        q_vec = None

    try:
        res = es_knn_search(course_id, q_used, q_vec, size=k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ES search failed: {e}")

    hits = []
    for h in res["hits"]["hits"]:
        src = h["_source"]
        hits.append({
            "score": h.get("_score"),
            "course_id": src.get("course_id"),
            "filename": src.get("filename"),
            "page": src.get("page"),
            "content": (src.get("content") or "")[:5000],
        })
    return {"query_used": q_used, "original_query": query_original, "hits": hits}

@app.post("/rag/answer", response_model=RagAnswer)
def rag_answer(rq: RagRequest):
    is_ru = looks_russian(rq.query)
    q_en = rq.query
    try:
        q_vec = embed([q_en])[0]
    except Exception:
        q_vec = None

    res = es_knn_search(rq.course_id, q_en, q_vec, size=max(20, rq.k))
    seen = set()
    contexts: List[str] = []
    sources: List[Dict[str, Any]] = []

    for h in res["hits"]["hits"]:
        src = h["_source"]
        key = (src.get("filename"), src.get("page"))
        if key in seen:
            continue
        seen.add(key)
        txt = (src.get("content") or "").strip()
        if not txt:
            continue
        contexts.append(txt)
        sources.append({"filename": src.get("filename"), "page": src.get("page"), "snippet": txt[:300]})
        if len(contexts) >= rq.k:
            break

    def split_sentences(t: str) -> List[str]:
        import re as _re
        return [s.strip() for s in _re.split(r"(?<=[\.\?\!])\s+", t) if s.strip()]

    sentences: List[str] = []
    for ctx in contexts:
        sentences.extend(split_sentences(ctx))

    selected = sentences[:rq.max_sentences]
    answer_en = " ".join(selected) if selected else "No relevant context found."
    final = translate([answer_en], "en", "ru")[0] if is_ru else answer_en
    return {"query_used": q_en, "answer": final, "sources": sources}
=======
# services\api\app\api\main.py
import os, json, uuid
from typing import List, Optional, Dict, Any

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kafka import KafkaProducer

from .settings import settings as _settings
from .answers import router as answers_router
from .search import router as search_router
from .routes_rephrase import router as rephrase_router
from .routes_translate_llm import router as translate_llm_router
from .summary import router as summary_router
from .assistant import router as assistant_router

APP_NAME = "assistant-teacher-api"

KAFKA_BOOTSTRAP = (
    os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    or os.getenv("KAFKA_BOOTSTRAP")
    or "kafka:9092"
)
TOPIC_GEN_REQ = (
    os.getenv("TOPIC_GEN_REQ")
    or os.getenv("TOPIC_QUIZZES_GENERATE")
    or "quizzes.generate"
)
TOPIC_GEN_DONE = (
    os.getenv("TOPIC_GEN_DONE")
    or os.getenv("TOPIC_QUIZZES_GENERATED")
    or "quizzes.generated"
)
TOPIC_GRADE_REQ = (
    os.getenv("TOPIC_GRADE_REQ")
    or os.getenv("TOPIC_QUIZZES_GRADE")
    or "quizzes.grade"
)
TOPIC_GRADE_DONE = (
    os.getenv("TOPIC_GRADE_DONE")
    or os.getenv("TOPIC_QUIZZES_GRADED")
    or "quizzes.graded"
)

S3_ENDPOINT = _settings.S3_ENDPOINT
S3_ACCESS = _settings.S3_ACCESS_KEY
S3_SECRET = _settings.S3_SECRET_KEY
S3_BUCKET = _settings.S3_BUCKET

s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
)

app = FastAPI(title=APP_NAME)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(rephrase_router)
app.include_router(answers_router)
app.include_router(search_router)
app.include_router(translate_llm_router)
app.include_router(summary_router)
app.include_router(assistant_router)

class GenerateQuizRequest(BaseModel):
    course_id: str
    topics: Optional[List[str]] = None
    n: int = 5
    types: Optional[List[str]] = None  # ["mcq", "cloze", "tf", "short"]

class GenerateQuizResponse(BaseModel):
    job_id: str
    status: str = "queued"

class GradeRequest(BaseModel):
    job_id: str  # тот же job_id, по которому лежит квиз
    answers: List[Any]  # массив ответов пользователя (индексы, строки и т.п.)

class JobStatus(BaseModel):
    job_id: str
    status: str
    payload: Optional[Dict[str, Any]] = None


def _ensure_bucket():
    try:
        s3.meta.client.head_bucket(Bucket=S3_BUCKET)
    except ClientError:
        s3.create_bucket(Bucket=S3_BUCKET)


def _s3_key(*parts: str) -> str:
    return "/".join(parts)


def _s3_put_json(key: str, obj: Dict[str, Any]):
    s3.Object(S3_BUCKET, key).put(Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"), ContentType="application/json")


def _s3_get_json(key: str) -> Optional[Dict[str, Any]]:
    try:
        body = s3.Object(S3_BUCKET, key).get()["Body"].read()
        return json.loads(body)
    except ClientError:
        return None


@app.get("/health")
def health():
    _ensure_bucket()
    return {"status": "ok"}


@app.post("/quizzes/generate", response_model=GenerateQuizResponse)
def quizzes_generate(req: GenerateQuizRequest):
    _ensure_bucket()
    job_id = str(uuid.uuid4())
    msg = {"job_id": job_id, **req.model_dump()}
    producer.send(TOPIC_GEN_REQ, msg)
    producer.flush()
    # сразу помечаем статус в S3, чтобы UI мог показывать "pending"
    _s3_put_json(_s3_key("quizzes", job_id, "status.json"), {"status": "pending"})
    return GenerateQuizResponse(job_id=job_id)


@app.get("/quizzes/result/{job_id}", response_model=JobStatus)
def quizzes_result(job_id: str):
    _ensure_bucket()
    key = _s3_key("quizzes", job_id, "quiz.json")
    data = _s3_get_json(key)
    if data:
        return JobStatus(job_id=job_id, status="ready", payload=data)
    # если ещё не готово, но статус уже создан — отдаём pending
    st = _s3_get_json(_s3_key("quizzes", job_id, "status.json"))
    if st:
        return JobStatus(job_id=job_id, status=st.get("status", "pending"))
    raise HTTPException(status_code=404, detail="job not found")


@app.post("/quizzes/grade", response_model=GenerateQuizResponse)
def quizzes_grade(req: GradeRequest):
    _ensure_bucket()
    job_id = str(uuid.uuid4())
    msg = req.model_dump() | {"grade_job_id": job_id}
    producer.send(TOPIC_GRADE_REQ, msg)
    producer.flush()
    _s3_put_json(_s3_key("grades", job_id, "status.json"), {"status": "pending"})
    return GenerateQuizResponse(job_id=job_id)


@app.get("/quizzes/grade/{job_id}", response_model=JobStatus)
def quizzes_grade_result(job_id: str):
    _ensure_bucket()
    data = _s3_get_json(_s3_key("grades", job_id, "result.json"))
    if data:
        return JobStatus(job_id=job_id, status="ready", payload=data)
    st = _s3_get_json(_s3_key("grades", job_id, "status.json"))
    if st:
        return JobStatus(job_id=job_id, status=st.get("status", "pending"))
    raise HTTPException(status_code=404, detail="job not found")
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
