from typing import Optional, List, Dict, Any, Tuple

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .settings import settings

router = APIRouter(prefix="/summary", tags=["summary"])


class LectureSummaryRequest(BaseModel):
    course_id: str
    material_id: Optional[str] = None
    filename: Optional[str] = None
    max_chars: int = 8000


class LectureSummaryByQueryRequest(BaseModel):
    course_id: str
    query: str
    max_chars: int = 8000


class LectureSummaryResponse(BaseModel):
    summary: str
    chunks_used: int
    pages_covered: List[int]
    material_id: Optional[str] = None
    filename: Optional[str] = None


def _fetch_chunks(req: LectureSummaryRequest) -> List[Dict[str, Any]]:
    if not req.material_id and not req.filename:
        raise HTTPException(status_code=400, detail="need material_id or filename")

    flt = [{"term": {"course_id": req.course_id}}]
    if req.material_id:
        flt.append({"term": {"material_id": req.material_id}})
    if req.filename:
        flt.append({"term": {"filename": req.filename}})

    body = {
        "size": 1000,
        "query": {"bool": {"filter": flt}},
        "sort": [{"page": {"order": "asc"}}],
        "_source": {"includes": ["page", "content"]},
    }

    r = requests.post(f"{settings.ES_URL}/chunks/_search", json=body, timeout=60)
    if r.status_code != 200:
        raise HTTPException(502, f"es error: {r.text[:200]}")
    return r.json().get("hits", {}).get("hits", [])


def _build_text(hits: List[Dict[str, Any]], max_chars: int):
    if not hits:
        return "", [], 0

    parts = []
    pages = set()

    for h in hits:
        src = h.get("_source", {})
        txt = (src.get("content") or "").strip()
        if not txt:
            continue
        parts.append(txt)
        pages.add(int(src.get("page", 0)))

    full = "\n\n".join(parts)
    if len(full) > max_chars:
        full = full[:max_chars]

    return full, sorted(pages), len(parts)


def _call_llm(text: str) -> str:
    if not text.strip():
        return "По этой лекции нет текста в базе."

    body = {
        "model": "qwen2.5-7b-instruct",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты ассистент преподавателя. Сделай краткое содержание лекции "
                    "по-русски, структурировано и без воды."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Ниже текст одной лекции фрагментами. "
                    "Сделай краткое содержание в 5–10 пунктов, не придумывай лишнего:\n\n"
                    f"{text}"
                ),
            },
        ],
        "temperature": 0.2,
        "max_tokens": 600,
    }

    r = requests.post(
        settings.LLM_URL,
        json=body,
        timeout=(10, 600),
    )
    if r.status_code != 200:
        raise HTTPException(502, f"llm error: {r.text[:200]}")
    return r.json()["choices"][0]["message"]["content"].strip()


def _search_best_lecture(course_id: str, query: str, size: int = 200) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    body = {
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": ["content^2", "filename^3"],
                        }
                    }
                ],
                "filter": [{"term": {"course_id": course_id}}],
            }
        },
        "_source": {"includes": ["material_id", "filename", "page", "content"]},
    }

    r = requests.post(f"{settings.ES_URL}/chunks/_search", json=body, timeout=60)
    if r.status_code != 200:
        raise HTTPException(502, f"es error: {r.text[:200]}")
    hits = r.json().get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="no lecture candidates found")

    groups: Dict[Tuple[Optional[str], Optional[str]], Dict[str, Any]] = {}

    for h in hits:
        src = h.get("_source", {})
        mid = src.get("material_id") or None
        fn = src.get("filename") or None
        key = (mid, fn)
        g = groups.setdefault(key, {"score": 0.0, "hits": []})
        g["score"] += float(h.get("_score") or 0.0)
        g["hits"].append(h)

    best_key, best_group = max(groups.items(), key=lambda kv: kv[1]["score"])
    best_mid, best_fn = best_key
    best_hits = best_group["hits"]
    return best_hits, best_mid, best_fn


@router.post("/lecture", response_model=LectureSummaryResponse)
def summarize_lecture(req: LectureSummaryRequest):
    hits = _fetch_chunks(req)
    text, pages, n = _build_text(hits, req.max_chars)
    summary = _call_llm(text)
    return LectureSummaryResponse(
        summary=summary,
        chunks_used=n,
        pages_covered=pages,
        material_id=req.material_id,
        filename=req.filename,
    )


@router.post("/by_query", response_model=LectureSummaryResponse)
def summarize_by_query(req: LectureSummaryByQueryRequest):
    hits, mid, fn = _search_best_lecture(req.course_id, req.query)
    text, pages, n = _build_text(hits, req.max_chars)
    summary = _call_llm(text)
    return LectureSummaryResponse(
        summary=summary,
        chunks_used=n,
        pages_covered=pages,
        material_id=mid,
        filename=fn,
    )
