FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
COPY requirements.torch.txt /app/requirements.torch.txt

# pip — без кеша, побольше таймаут, без прогресс-бара
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=180 \
    PIP_PROGRESS_BAR=off

# Базовые утилиты
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# 1) Ставим torch/torchvision ТОЛЬКО из cpu-репозитория PyTorch
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu \
    -r requirements.torch.txt

# 2) Остальные пакеты — ТОЛЬКО из PyPI
RUN pip install --no-cache-dir --index-url https://pypi.org/simple \
    -r requirements.txt

# Кэши моделей в общий том
ENV MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2" \
    MODEL_CACHE="/models" \
    SENTENCE_TRANSFORMERS_HOME="/models" \
    TRANSFORMERS_CACHE="/models" \
    HF_HOME="/models" \
    HUGGINGFACE_HUB_CACHE="/models" \
    HF_HUB_ENABLE_HF_TRANSFER=0 \
    PYTHONUNBUFFERED=1

COPY app /app/app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]