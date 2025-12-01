from fastapi import APIRouter
from pydantic import BaseModel
from .settings import settings
import requests

router = APIRouter(prefix="/search", tags=["search"])


class TextSearch(BaseModel):
    course_id: str
    query: str
    size: int = 5


@router.post("/text")
def text_search(req: TextSearch):
    payload = {
        "size": req.size,
        "query": {
            "bool": {
                "must": [
                    {"multi_match": {"query": req.query, "fields": ["content"]}}
                ],
                "filter": [{"term": {"course_id": req.course_id}}],
            }
        },
        "_source": {"includes": ["filename", "page", "content"]},
        "highlight": {
            "fields": {"content": {}},
            "fragment_size": 160,
            "number_of_fragments": 1,
        },
    }
    r = requests.post(f"{settings.ES_URL}/chunks/_search", json=payload, timeout=30)
    return r.json()


class KnnSearch(BaseModel):
    course_id: str
    query: str
    k: int = 5
    num_candidates: int = 256


@router.post("/knn")
def knn_search(req: KnnSearch):
    vec = requests.post(
        settings.EMBEDDINGS_URL,
        json={"texts": [req.query]},
        timeout=30,
    ).json()["vectors"][0]

    payload = {
        "size": req.k,
        "knn": {
            "field": "vector",
            "query_vector": vec,
            "k": req.k,
            "num_candidates": req.num_candidates,
        },
        "query": {"bool": {"filter": [{"term": {"course_id": req.course_id}}]}},
        "_source": {"includes": ["filename", "page", "content"]},
        "highlight": {
            "fields": {"content": {}},
            "fragment_size": 160,
            "number_of_fragments": 1,
        },
    }
    r = requests.post(f"{settings.ES_URL}/chunks/_search", json=payload, timeout=30)
    return r.json()