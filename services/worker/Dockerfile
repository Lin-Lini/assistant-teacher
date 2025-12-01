FROM python:3.11-slim
<<<<<<< HEAD
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr poppler-utils curl gcc g++ \
    && rm -rf /var/lib/apt/lists/*
=======
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app /app/app
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app/app
<<<<<<< HEAD
# Команда задаётся в docker-compose (python -m app.workers.*)
=======
RUN apt-get update && apt-get install -y --no-install-recommends \
    tesseract-ocr tesseract-ocr-rus poppler-utils curl gcc g++ \
    && rm -rf /var/lib/apt/lists/*
>>>>>>> f2f682f (Обновлен проект (без моделей и кэша))
