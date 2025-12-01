# services/api/app/api/routes_translate_llm.py
import os
import requests
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

LLM_URL = os.getenv("LLM_URL", "http://generator:8002/v1/chat/completions")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5-7b-instruct")

router = APIRouter(
    prefix="/translate",
    tags=["translate"],
)

class TranslateEnRuRequest(BaseModel):
    text: str

class TranslateEnRuResponse(BaseModel):
    text: str

@router.post("/en-ru", response_model=TranslateEnRuResponse)
def translate_en_ru(req: TranslateEnRuRequest):
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты профессиональный переводчик с английского на русский. "
                    "Переводи точно по смыслу, естественным русским языком. "
                    "Не добавляй комментариев, не объясняй, не меняй структуру текста."
                ),
            },
            {
                "role": "user",
                "content": f"Переведи на русский язык следующий текст:\n\n{req.text}",
            },
        ],
        "temperature": 0.1,
        "max_tokens": 800,
    }
    r = requests.post(LLM_URL, json=payload, timeout=(10, 600))
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"LLM error: {r.text[:200]}")
    data = r.json()
    return {"text": data["choices"][0]["message"]["content"].strip()}
