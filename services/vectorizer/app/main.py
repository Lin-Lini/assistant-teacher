# services/vectorizer/app/main.py
import os
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pathlib

APP_NAME = "assistant-teacher-vectorizer"

MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
MODEL_CACHE = os.getenv("MODEL_CACHE", "/models")
TRANSLATE_ENABLED = os.getenv("TRANSLATE_ENABLED", "0").lower() in ("1", "true", "yes")
DISABLE_MODEL_DOWNLOAD = os.getenv("DISABLE_MODEL_DOWNLOAD", "0").lower() in ("1", "true", "yes")

app = FastAPI(title=APP_NAME)

_model = None  # лениво инициализируем


def model_cached() -> bool:
    p = pathlib.Path(MODEL_CACHE)
    return p.exists() and any(p.iterdir())


def get_model():
    global _model
    if _model is None:
        if DISABLE_MODEL_DOWNLOAD and not model_cached():
            raise RuntimeError("Model cache is empty and downloads are disabled")
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(MODEL_NAME, cache_folder=MODEL_CACHE)
    return _model


class EmbedReq(BaseModel):
    texts: List[str]


class TranslateReq(BaseModel):
    source: str
    target: str
    texts: List[str]


@app.get("/health")
def health():
    info = {
        "status": "ok",
        "model": MODEL_NAME,
        "cache_dir": MODEL_CACHE,
        "cache_ready": model_cached(),
        "translate_enabled": TRANSLATE_ENABLED,
    }
    # Не падаем, даже если модель ещё не скачана
    try:
        m = get_model()
        info["dim"] = m.get_sentence_embedding_dimension()
    except Exception as e:
        info["note"] = f"model not loaded yet: {e}"
    return info


@app.post("/embed")
def embed(req: EmbedReq):
    if not req.texts:
        return {"vectors": []}
    try:
        m = get_model()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Vectorizer not ready: {e}")
    vecs = m.encode(req.texts, convert_to_numpy=True, normalize_embeddings=True).tolist()
<<<<<<< HEAD
    return {"vectors": vecs}
=======
    return {"vectors": vecs, "embeddings": vecs}
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))


@app.post("/translate")
def translate(req: TranslateReq):
    # Возвращаем 204, если перевод выключен — как и ожидал API
    if not TRANSLATE_ENABLED:
        return {"texts": req.texts}, 204
    # Здесь позже можно подключить реальный переводчик
    return {"texts": req.texts}