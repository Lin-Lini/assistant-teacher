# services/api/app/api/assistant.py
import json
from typing import Any, Dict, Optional, List

import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .settings import settings
from .security import guard_input
from .translate import maybe_handle_translate_command
from .answers import AnswerRequest, answer as answer_handler
from .summary import (
    LectureSummaryByQueryRequest,
    summarize_by_query,
)
from .rephrase_client import rephrase as rephrase_client

router = APIRouter(
    prefix="/assistant",
    tags=["assistant"],
)


class AssistantRequest(BaseModel):
    course_id: Optional[str] = None
    query: str
    k: int = 5
    num_candidates: int = 256
    quiz_n: int = 5


class AssistantResponse(BaseModel):
    mode: str
    result: Any
    router_plan: Dict[str, Any]


def _llm_route(q: str, has_course: bool) -> Dict[str, Any]:
    sys_msg = (
        "Ты управляешь API ассистента преподавателя и возвращаешь только JSON.\n"
        "Всегда отвечай одним объектом JSON без пояснений.\n"
        "Структура: {\"mode\": str, \"params\": {…}}.\n"
        "Допустимые mode:\n"
        "  \"rag_answer\"        — ответить на вопрос по материалам курса.\n"
        "  \"summary_by_query\"  — сделать краткое содержание лекции по описанию/номеру.\n"
        "  \"quiz_generate\"     — сгенерировать контрольные вопросы/тест.\n"
        "  \"rephrase\"          — упростить/переформулировать текст.\n"
        "Поле params:\n"
        "  для \"rag_answer\": {\"use_query_as_topics\": bool}\n"
        "  для \"summary_by_query\": {}\n"
        "  для \"quiz_generate\": {\"n\": int, \"topics\": [str]}\n"
        "  для \"rephrase\": {}\n"
        "Если запрос похож на просьбу объяснить, ответить или найти в лекциях — выбери \"rag_answer\".\n"
        "Если про конспект/краткое содержание лекции/лекции по теме — \"summary_by_query\".\n"
        "Если про тест, квиз, проверочные вопросы, экзамен — \"quiz_generate\".\n"
        "Если просит просто переписать/объяснить текст проще — \"rephrase\".\n"
        f"У пользователя есть course_id: {'true' if has_course else 'false'}.\n"
        "Никаких комментариев, только валидный JSON."
    )

    body = {
        "model": "qwen2.5-7b-instruct",
        "messages": [
            {"role": "system", "content": sys_msg},
            {"role": "user", "content": q},
        ],
        "temperature": 0,
        "max_tokens": 256,
    }

    r = requests.post(
        settings.LLM_URL,
        json=body,
        timeout=(10, 600),
    )
    if r.status_code != 200:
        raise HTTPException(502, f"router error: {r.text[:200]}")
    data = r.json()
    txt = data["choices"][0]["message"]["content"].strip()
    try:
        obj = json.loads(txt)
        if not isinstance(obj, dict):
            raise ValueError("router must return JSON object")
        if "mode" not in obj:
            raise ValueError("router JSON must have 'mode'")
        if "params" not in obj or not isinstance(obj["params"], dict):
            obj["params"] = {}
        return obj
    except Exception:
        return {"mode": "rag_answer", "params": {}}


def _call_quiz_generate(course_id: str, topics: List[str], n: int) -> Dict[str, Any]:
    body = {
        "course_id": course_id,
        "topics": topics,
        "n": n,
    }
    r = requests.post(
        "http://localhost:8000/quizzes/generate",
        json=body,
        timeout=30,
    )
    if r.status_code != 200:
        raise HTTPException(502, f"quiz error: {r.text[:200]}")
    return r.json()


@router.post("", response_model=AssistantResponse)
def assistant(req: AssistantRequest):
    t = maybe_handle_translate_command(req.query)
    if t is not None:
        plan = {
            "mode": "translate_en_ru",
            "params": {"pattern": "переведи на русский"},
        }
        return AssistantResponse(
            mode="translate_en_ru",
            result={"text": t},
            router_plan=plan,
        )

    q = guard_input(req.query or "")
    plan = _llm_route(q, bool(req.course_id))
    mode = str(plan.get("mode") or "").strip()
    params = plan.get("params") or {}

    if mode == "rag_answer":
        if not req.course_id:
            raise HTTPException(400, "course_id required for rag_answer")
        a_req = AnswerRequest(
            course_id=req.course_id,
            query=q,
            k=req.k,
            num_candidates=req.num_candidates,
        )
        res = answer_handler(a_req)
        return AssistantResponse(mode=mode, result=res, router_plan=plan)

    if mode == "summary_by_query":
        if not req.course_id:
            raise HTTPException(400, "course_id required for summary_by_query")
        s_req = LectureSummaryByQueryRequest(
            course_id=req.course_id,
            query=q,
            max_chars=8000,
        )
        res = summarize_by_query(s_req)
        return AssistantResponse(mode=mode, result=res, router_plan=plan)

    if mode == "quiz_generate":
        if not req.course_id:
            raise HTTPException(400, "course_id required for quiz_generate")
        n = int(params.get("n") or req.quiz_n)
        if n <= 0:
            n = req.quiz_n
        topics = params.get("topics")
        if not isinstance(topics, list) or not topics:
            topics = [q]
        topics = [str(x) for x in topics]
        res = _call_quiz_generate(req.course_id, topics, n)
        return AssistantResponse(mode=mode, result=res, router_plan=plan)

    if mode == "rephrase":
        res_text = rephrase_client(q)
        return AssistantResponse(
            mode=mode,
            result={"text": res_text},
            router_plan=plan,
        )

    if not req.course_id:
        res_text = rephrase_client(q)
        fb = {"fallback": True, "orig_mode": mode, "orig_plan": plan}
        return AssistantResponse(
            mode="rephrase",
            result={"text": res_text},
            router_plan=fb,
        )

    a_req = AnswerRequest(
        course_id=req.course_id,
        query=q,
        k=req.k,
        num_candidates=req.num_candidates,
    )
    res = answer_handler(a_req)
    fb = {"fallback": True, "orig_mode": mode, "orig_plan": plan}
    return AssistantResponse(
        mode="rag_answer",
        result=res,
        router_plan=fb,
    )
