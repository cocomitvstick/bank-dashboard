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

# Railway и другие платформы автоматически задают PORT через переменную окружения
EXPOSE 8000

# Запуск — порт читается из переменной окружения PORT (по умолчанию 8000)
CMD ["python", "/app/backend/main.py"]
