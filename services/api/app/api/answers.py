from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List
import requests

from .settings import settings
from .security import guard_input, scan_secrets
from .translate import maybe_handle_translate_command

router = APIRouter(prefix="/answer", tags=["rag"])


class Source(BaseModel):
    filename: str
    page: int
    score: float
    snippet: str


class AnswerRequest(BaseModel):
    course_id: str
    query: str
    k: int = 5                  # сколько абзацев в контекст
    num_candidates: int = 256   # hnsw candidates
    with_rerank: bool = False   # запас под кросс-ранкер


class AnswerResponse(BaseModel):
    answer: str
    sources: List[Source]


def embed(text: str) -> list:
    r = requests.post(
        settings.EMBEDDINGS_URL,
        json={"texts": [text]},
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"embed error: {r.text[:200]}")
    return r.json()["vectors"][0]


def es_knn(vec: list, course_id: str, k: int, num_cand: int):
    payload = {
        "size": k,
        "knn": {
            "field": "vector",
            "query_vector": vec,
            "k": k,
            "num_candidates": num_cand,
        },
        "query": {
            "bool": {
                "filter": [
                    {"term": {"course_id": course_id}}
                ]
            }
        },
        "_source": {"includes": ["filename", "page", "content"], "excludes": ["vector"]},
        "highlight": {
            "fields": {"content": {}},
            "fragment_size": 160,
            "number_of_fragments": 1,
        },
    }
    r = requests.post(f"{settings.ES_URL}/chunks/_search", json=payload, timeout=30)
    if r.status_code != 200:
        raise HTTPException(502, f"es error: {r.text[:200]}")
    return r.json()["hits"]["hits"]


def build_context(hits):
    ctx = []
    out_sources = []
    for h in hits:
        src = h["_source"]
        snippet = (
            h.get("highlight", {})
             .get("content", [src["content"][:400]])[0]
        ).replace("\n", " ")
        ctx.append(snippet)
        out_sources.append(
            Source(
                filename=src["filename"],
                page=src["page"],
                score=float(h.get("_score", 0.0)),
                snippet=snippet,
            )
        )
    return "\n\n".join(ctx), out_sources


def call_llm_answer(question: str, context: str) -> str:
    # нормальный чат-запрос к llama.cpp /v1/chat/completions
    payload = {
        "model": "qwen2.5-7b-instruct",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты ассистент преподавателя. Отвечай по-русски, кратко и по делу. "
                    "Используй ТОЛЬКО переданный контекст, ничего не выдумывай."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Вопрос:\n{question}\n\n"
                    f"Контекст:\n{context}\n\n"
                    "Дай ясный и сжатый ответ для студента."
                ),
            },
        ],
        "temperature": 0.2,
        "max_tokens": 256,
    }
    r = requests.post(
        settings.LLM_URL,
        json=payload,
        timeout=(10, 800),  # (connect_timeout, read_timeout)
    )
    if r.status_code != 200:
        raise HTTPException(502, f"LLM upstream error: {r.text[:200]}")
    data = r.json()
    return data["choices"][0]["message"]["content"].strip()


@router.post("", response_model=AnswerResponse)
def answer(req: AnswerRequest):
    # 1) сначала проверяем, не просит ли пользователь перевод
    translated = maybe_handle_translate_command(req.query)
    if translated is not None:
        # подстрахуемся: вдруг в результате перевода всплыли ключи/секреты
        leaks = scan_secrets(translated)
        if leaks:
            translated = "Ответ скрыт по правилам безопасности (обнаружены возможные секреты в тексте)."
        return AnswerResponse(answer=translated, sources=[])

    # 2) обычный режим RAG-ответа с защитой
    safe_query = guard_input(req.query)

    vec = embed(safe_query)
    hits = es_knn(vec, req.course_id, req.k, req.num_candidates)
    context, sources = build_context(hits)

    out = call_llm_answer(safe_query, context)

    # 3) проверяем, не утекли ли секреты в финальный ответ
    leaks = scan_secrets(out)
    if leaks:
        out = "Ответ скрыт по правилам безопасности (обнаружены возможные секреты в исходных материалах)."

    return AnswerResponse(answer=out, sources=sources)
