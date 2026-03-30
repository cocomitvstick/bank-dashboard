FROM python:3.12-slim

# Системные зависимости для lxml
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libxml2-dev \
    libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Устанавливаем зависимости
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Сброс кэша Docker при каждом деплое
ARG CACHEBUST=20260330c
RUN echo "Cache bust: $CACHEBUST"

# Копируем бэкенд
COPY backend/ ./backend/

# Копируем фронтенд
COPY frontend/ ./frontend/

# Создаём директорию для кэша данных
RUN mkdir -p /app/backend/data/metrics

# Отключаем буферизацию Python чтобы видеть логи сразу
ENV PYTHONUNBUFFERED=1
ENV PORT=8000

EXPOSE 8000

# Запуск с диагностическим выводом
CMD ["python", "/app/backend/main.py"]
