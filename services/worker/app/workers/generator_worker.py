# services/worker/app/workers/generator_worker.py
<<<<<<< HEAD
import os
import re
import json
import uuid
import random
import logging
from typing import List, Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("generator-worker")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_IN = os.getenv("QUIZZES_GENERATE_TOPIC", "quizzes.generate")
TOPIC_OUT = os.getenv("QUIZZES_GENERATED_TOPIC", "quizzes.generated")
GROUP_ID = os.getenv("GENERATOR_GROUP_ID", "generator-group")

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX_CHUNKS = os.getenv("ES_INDEX_CHUNKS", "chunks")
ES_INDEX_QUIZZES = os.getenv("ES_INDEX_QUIZZES", "quizzes")

WORD_RE = re.compile(r"[A-Za-z][A-Za-z\-]{3,}")
STOP = set("the a an and or in on at for from to is are was were be been being of with by as that this these those which who whom whose into onto above below about between over under it its their them they we you your i me my our ours us not no yes".split())

def pick_keywords(text: str, top_k: int = 10) -> List[str]:
    words = [w.lower() for w in WORD_RE.findall(text)]
    freq: Dict[str, int] = {}
    for w in words:
        if w in STOP:
            continue
        freq[w] = freq.get(w, 0) + 1
    items = sorted(freq.items(), key=lambda x: (-x[1], x[0]))
    return [w for w, _ in items[:top_k]]

def make_mcq_from_text(text: str, pool_words: List[str]) -> Dict[str, Any]:
    sentences = re.split(r"(?<=[\.\?\!])\s+", text.strip())
    sentences = [s for s in sentences if 8 <= len(s.split()) <= 25]
    if not sentences:
        return {}
    sent = random.choice(sentences)
    kws = [w for w in pick_keywords(sent, 8) if len(w) >= 4]
    if not kws:
        return {}
    answer = random.choice(kws)
    question = sent.replace(answer, "____", 1)
    distractors = [w for w in pool_words if w != answer]
    distractors = random.sample(distractors, k=min(3, len(distractors))) if distractors else []
    options = [answer] + distractors
    random.shuffle(options)
    return {"question": f"Fill in the blank: {question}", "options": options, "answer": answer}

def main():
    es = Elasticsearch(ES_URL, request_timeout=60)
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )
    log.info("Generator worker started. in=%s out=%s", TOPIC_IN, TOPIC_OUT)

    for msg in consumer:
        try:
            payload = msg.value
            course_id = payload["course_id"]
            n = int(payload.get("n", 5))
            query_text = (payload.get("query") or "").strip()

            body = {"size": max(50, n*10), "query": {"bool": {"filter": [{"term": {"course_id": course_id}}]}}}
            if query_text:
                body["query"]["bool"]["should"] = [{"match": {"content": {"query": query_text}}}]

            res = es.search(index=ES_INDEX_CHUNKS, body=body)
            docs = [h["_source"] for h in res["hits"]["hits"]]
            if not docs:
                log.info("No chunks for course=%s", course_id)
                continue

            pool_words: List[str] = []
            for d in docs[:50]:
                pool_words.extend(pick_keywords(d.get("content", ""), 6))
            pool_words = list({w for w in pool_words})

            created = []
            for d in docs[:n*3]:
                mcq = make_mcq_from_text(d.get("content", ""), pool_words)
                if not mcq:
                    continue
                quiz_id = str(uuid.uuid4())
                es.index(index=ES_INDEX_QUIZZES, id=quiz_id, document={
                    "quiz_id": quiz_id,
                    "course_id": course_id,
                    "question": mcq["question"],
                    "options": mcq["options"],
                    "answer": mcq["answer"],
                    "source": f'{d.get("filename")}#p{d.get("page")}'
                })
                created.append(quiz_id)
                if len(created) >= n:
                    break

            if created:
                producer.send(TOPIC_OUT, {"course_id": course_id, "quiz_ids": created})
                producer.flush()
                log.info("Generated %d quizzes for course=%s", len(created), course_id)
        except Exception as e:
            log.exception("Generation failed: %s", e)

if __name__ == "__main__":
    main()
=======
import os, json, re, random, time
from typing import List, Dict, Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from elasticsearch import Elasticsearch
import boto3

import requests

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_GEN_REQ = os.getenv("TOPIC_GEN_REQ", "quizzes.generate")
TOPIC_GEN_DONE = os.getenv("TOPIC_GEN_DONE", "quizzes.generated")

ES_URL = os.getenv("ES_URL", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "chunks")

LLM_URL = os.getenv("LLM_URL", "http://generator:8002/v1/chat/completions")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5-7b-instruct")

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

s3 = boto3.resource(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS,
    aws_secret_access_key=S3_SECRET,
)

es = Elasticsearch(ES_URL)

STOP_PHRASES = [
    r"игнорируй предыдущие", r"система инструкций", r"раскрой секрет",
    r"system prompt", r"опиши токены", r"override instructions",
]


def guard_prompt(text: str) -> str:
    t = text
    for p in STOP_PHRASES:
        t = re.sub(p, "[blocked]", t, flags=re.I)
    return t


def es_search(course_id: str, query: str, k: int = 20) -> List[Dict[str, Any]]:
    q = {
        "bool": {
            "must": [{"multi_match": {"query": query, "fields": ["content^2", "filename"]}}],
            "filter": [{"term": {"course_id": course_id}}],
        }
    }
    res = es.search(index=ES_INDEX, query=q, size=k)
    hits = []
    for h in res.get("hits", {}).get("hits", []):
        s = h.get("_source", {})
        s["_score"] = h.get("_score", 0)
        hits.append(s)
    return hits

def build_context(hits: List[Dict[str, Any]], limit_chars: int = 4000) -> str:
    parts = []
    total = 0
    for h in hits:
        txt = h.get("content") or h.get("text") or ""
        txt = txt.strip()
        if not txt:
            continue
        if total + len(txt) > limit_chars:
            break
        parts.append(txt)
        total += len(txt)
    return "\n\n".join(parts)

def llm_generate_quiz(course_id: str, topics: List[str], n: int, context: str) -> List[Dict[str, Any]]:
    if not context.strip():
        return []

    system_prompt = (
        "Ты ассистент преподавателя. На основе переданного конспекта курса "
        "создаёшь контрольные вопросы для студентов. "
        "Формат ответа — JSON массив без пояснений, только данные.\n\n"
        "Каждый элемент массива:\n"
        "{\n"
        '  \"type\": \"cloze\" | \"tf\" | \"mcq\" | \"short\",\n'
        '  \"question\": \"строка с вопросом на русском\",\n'
        '  \"options\": [\"...\"],        // только для mcq, иначе опусти или []\n'
        '  \"answer\": \"правильный ответ или true/false\",\n'
        '  \"explanation\": \"краткое объяснение ответа\" // можно пустую строку\n'
        "}\n"
    )

    user_prompt = (
        f"Курс: {course_id}\n"
        f"Темы: {', '.join(topics) if topics else 'общий экзамен по курсу'}\n\n"
        f"Конспект:\n{context}\n\n"
        f"Сгенерируй {n} хороших вопросов разного типа для проверки понимания материала. "
        "Не выходи за рамки конспекта."
    )

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 1500,
    }

    r = requests.post(LLM_URL, json=payload, timeout=(10, 600))
    r.raise_for_status()
    data = r.json()
    content = data["choices"][0]["message"]["content"].strip()

    try:
        items = json.loads(content)
        if not isinstance(items, list):
            raise ValueError("LLM must return JSON array")
        return items
    except Exception as e:
        print("Failed to parse LLM quiz JSON:", e)
        return []

def pick_sentences(text: str) -> List[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if 40 <= len(p) <= 300][:3]


def make_cloze(sent: str) -> Dict[str, Any]:
    words = re.findall(r"[A-Za-zА-Яа-яЁё0-9-]{5,}", sent)
    if not words:
        return {"type": "tf", "question": sent, "answer": True}
    w = random.choice(words)
    q = sent.replace(w, "____", 1)
    return {"type": "cloze", "question": q, "answer": w}


def generate_quiz(course_id: str, topics: List[str], n: int) -> Dict[str, Any]:
    random.seed(42)
    topics = topics or ["экзамен по курсу"]

    # 1) контекст из ES
    all_hits: List[Dict[str, Any]] = []
    for t in topics:
        hits = es_search(course_id, guard_prompt(t), k=50)
        all_hits.extend(hits)
    if not all_hits:
        items = [{"type": "tf", "question": "Курс содержит материалы?", "answer": True}]
        return {"course_id": course_id, "count": len(items), "items": items}

    context = build_context(all_hits)

    # 2) пробуем LLM
    items = llm_generate_quiz(course_id, topics, n, context)
    if not items:
        # fallback на старый тупой режим, чтобы не умереть
        items_fallback: List[Dict[str, Any]] = []
        for h in all_hits:
            txt = h.get("content") or h.get("text") or ""
            for s in pick_sentences(txt):
                items_fallback.append(make_cloze(s))
                if len(items_fallback) >= n:
                    break
            if len(items_fallback) >= n:
                break
        if not items_fallback:
            items_fallback = [{"type": "tf", "question": "Курс содержит материалы?", "answer": True}]
        return {"course_id": course_id, "count": len(items_fallback), "items": items_fallback}

    return {"course_id": course_id, "count": len(items), "items": items}

def _s3_key(*parts: str) -> str:
    return "/".join(parts)


def save_result(job_id: str, quiz: Dict[str, Any]):
    s3.Object(S3_BUCKET, _s3_key("quizzes", job_id, "quiz.json")).put(
        Body=json.dumps(quiz, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    s3.Object(S3_BUCKET, _s3_key("quizzes", job_id, "status.json")).put(
        Body=json.dumps({"status": "ready"}).encode("utf-8"),
        ContentType="application/json",
    )


def make_producer() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
            return p
        except NoBrokersAvailable as e:
            print(f"Kafka not available: {e}, retry in 5s")
            time.sleep(5)


def make_consumer() -> KafkaConsumer:
    while True:
        try:
            c = KafkaConsumer(
                TOPIC_GEN_REQ,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                group_id="quiz-generator",
                enable_auto_commit=True,
            )
            return c
        except NoBrokersAvailable as e:
            print(f"Kafka not available: {e}, retry in 5s")
            time.sleep(5)


def run():
    producer = make_producer()
    consumer = make_consumer()
    for msg in consumer:
        payload = msg.value
        job_id = payload["job_id"]
        course_id = payload["course_id"]
        topics = payload.get("topics") or []
        n = int(payload.get("n", 5))
        quiz = generate_quiz(course_id, topics, n)
        save_result(job_id, quiz)
        producer.send(TOPIC_GEN_DONE, {"job_id": job_id, "status": "ready"})
        producer.flush()


if __name__ == "__main__":
    run()
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
