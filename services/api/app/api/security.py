import re
from dataclasses import dataclass
from typing import List, Tuple


# Примитивные, но полезные паттерны
INJECTION_REGEXES = [
    re.compile(r"(?i)ignore (all )?previous instructions"),
    re.compile(r"(?i)system prompt"),
    re.compile(r"(?i)override instructions"),
    re.compile(r"(?i)prompt injection"),
    re.compile(r"(?i)проигнорируй (все )?предыдущие"),
    re.compile(r"(?i)системн(ые|ую) инструкц"),
]

SECRETS_PATTERNS = [
    re.compile(r"AKIA[0-9A-Z]{16}"),              # AWS access key
    re.compile(r"(?i)aws_secret_access_key"),     # AWS secret
    re.compile(r"(?i)BEGIN (RSA )?PRIVATE KEY"),  # приватные ключи
    re.compile(r"(?i)api[_-]?key[\"'=:\s]+[0-9A-Za-z\-]{10,}"),
]


@dataclass
class Match:
    category: str
    span: Tuple[int, int]
    excerpt: str


def _excerpt(text: str, s: int, e: int, pad: int = 40) -> str:
    s0 = max(0, s - pad)
    e0 = min(len(text), e + pad)
    return text[s0:e0].replace("\n", "\\n")


def scan_injections(text: str) -> List[Match]:
    out: List[Match] = []
    for rx in INJECTION_REGEXES:
        for m in rx.finditer(text):
            s, e = m.span()
            out.append(
                Match(
                    category="prompt_injection",
                    span=(s, e),
                    excerpt=_excerpt(text, s, e),
                )
            )
    return out


def scan_secrets(text: str) -> List[Match]:
    out: List[Match] = []
    for rx in SECRETS_PATTERNS:
        for m in rx.finditer(text):
            s, e = m.span()
            out.append(
                Match(
                    category="secret",
                    span=(s, e),
                    excerpt=_excerpt(text, s, e),
                )
            )
    return out


def guard_input(text: str) -> str:
    """Режем типичные инъекционные фразы до [blocked]."""
    if not text:
        return text
    out = text
    for rx in INJECTION_REGEXES:
        out = rx.sub("[blocked]", out)
    return out
