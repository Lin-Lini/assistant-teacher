import os
from typing import Literal, Optional

from langdetect import detect, LangDetectException
<<<<<<< HEAD
=======
import requests
import re 
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))

# Ленивая загрузка HF-пайплайнов
_trans_ru_en = None
_trans_en_ru = None

TranslationMode = Literal["auto", "off"]

<<<<<<< HEAD
=======
LLM_URL = os.getenv("LLM_URL", "http://generator:8002/v1/chat/completions")
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5-7b-instruct")

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def _get_mode(default: str = "auto") -> TranslationMode:
    mode = os.getenv("TRANSLATION_MODE", default).strip().lower()
    return "off" if mode == "off" else "auto"

def _load_ru_en():
    global _trans_ru_en
    if _trans_ru_en is None:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
        model_name = os.getenv("RU_EN_MODEL", "Helsinki-NLP/opus-mt-ru-en")
        tok = AutoTokenizer.from_pretrained(model_name)
        mdl = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        _trans_ru_en = pipeline("translation", model=mdl, tokenizer=tok)
    return _trans_ru_en

def _load_en_ru():
    global _trans_en_ru
    if _trans_en_ru is None:
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline
        model_name = os.getenv("EN_RU_MODEL", "Helsinki-NLP/opus-mt-en-ru")
        tok = AutoTokenizer.from_pretrained(model_name)
        mdl = AutoModelForSeq2SeqLM.from_pretrained(model_name)
        _trans_en_ru = pipeline("translation", model=mdl, tokenizer=tok)
    return _trans_en_ru

<<<<<<< HEAD
=======
def maybe_handle_translate_command(text: str) -> Optional[str]:
    """
    Если текст выглядит как команда 'переведи на русский ...',
    вернуть перевод. Иначе вернуть None.
    """
    pattern = re.compile(
        r"^\s*переведи\s+на\s+русский(?:\s+язык)?[:\s]+(.+)$",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.match(text)
    if not m:
        return None
    payload = m.group(1).strip()
    if not payload:
        return None
    return _llm_translate_en_ru(payload)

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def detect_lang(text: str) -> Optional[str]:
    try:
        return detect(text)
    except LangDetectException:
        return None

<<<<<<< HEAD
=======
def _llm_translate_en_ru(text: str) -> str:
    payload = {
        "model": LLM_MODEL,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты профессиональный переводчик с английского на русский. "
                    "Переводи точно по смыслу, естественным русским языком. "
                    "Не добавляй комментариев, не объясняй, не меняй формат текста, "
                    "просто дай перевод."
                ),
            },
            {
                "role": "user",
                "content": f"Переведи на русский язык следующий текст:\n\n{text}",
            },
        ],
        "temperature": 0.1,
        "max_tokens": 800,
    }
    r = requests.post(LLM_URL, json=payload, timeout=(10, 600))
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"].strip()

>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
def translate_query_if_needed(text: str, mode: Optional[str] = None):
    """Возвращает (query_for_search, original_lang)."""
    mode = (mode or _get_mode()).lower()
    lang = detect_lang(text) or "en"
    if mode == "off" or lang != "ru":
        return text, lang
    # ru -> en для поиска
    t = _load_ru_en()
    translated = t(text, max_length=512)[0]["translation_text"]
    return translated, "ru"

def translate_answer_if_needed(answer_en: str, original_lang: Optional[str], mode: Optional[str] = None):
<<<<<<< HEAD
    """Если исходный запрос был ru и режим auto — перевести ответ en->ru, иначе вернуть как есть."""
    mode = (mode or _get_mode()).lower()
    if mode == "off" or original_lang != "ru":
        return answer_en
    t = _load_en_ru()
    translated = t(answer_en, max_length=768)[0]["translation_text"]
    return translated
=======
    """Если исходный запрос был ru и режим auto — перевести ответ en->ru через LLM, иначе вернуть как есть."""
    mode = (mode or _get_mode()).lower()
    if mode == "off" or original_lang != "ru":
        return answer_en
    return _llm_translate_en_ru(answer_en)
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
