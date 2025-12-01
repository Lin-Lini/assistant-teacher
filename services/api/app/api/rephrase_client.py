import requests

from .settings import settings
from .security import guard_input, scan_secrets


LLM_URL = settings.LLM_URL


def rephrase(text: str) -> str:
    safe_text = guard_input(text or "")

    d = {
        "model": "qwen2.5-7b-instruct",
        "messages": [
            {
                "role": "system",
                "content": (
                    "Ты помощник преподавателя. Переписываешь текст так, "
                    "чтобы студенту было проще понять. Сохраняй смысл, ничего не добавляй."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Переформулируй текст ниже так, чтобы он стал короче и понятнее "
                    "для студента. Ответ оформляй в виде 3–5 маркеров:\n\n"
                    f"{safe_text}"
                ),
            },
        ],
        "temperature": 0.2,
        "max_tokens": 400,
    }
    r = requests.post(LLM_URL, json=d, timeout=60)
    r.raise_for_status()
    out = r.json()["choices"][0]["message"]["content"]

    leaks = scan_secrets(out)
    if leaks:
        return "Переформулировка скрыта по правилам безопасности (обнаружены возможные секреты в тексте)."

    return out
