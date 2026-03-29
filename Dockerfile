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

# Копируем бэкенд
COPY backend/ ./backend/

# Копируем фронтенд
COPY frontend/ ./frontend/

# Создаём директорию для кэша данных
RUN mkdir -p /app/backend/data/metrics

# Порт (Railway/Render используют переменную PORT)
ENV PORT=8000

EXPOSE ${PORT}

# Запуск
CMD ["sh", "-c", "cd /app/backend && python main.py"]
