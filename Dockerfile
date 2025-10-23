# Используем Ubuntu как базу
FROM ubuntu:24.04

# Устанавливаем переменные окружения для Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Обновляем и устанавливаем необходимые утилиты
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Media processing tools
    rawtherapee \
    imagemagick \
    exiftool \
    ffmpeg \
    # Python and development tools
    python3 \
    python3-pip \
    python3-venv \
    # System utilities
    git \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Создаем симлинк python -> python3 для совместимости
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Создаем рабочую директорию для приложения
WORKDIR /app

# Копируем requirements.txt и устанавливаем Python зависимости
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Копируем исходный код приложения
COPY . .

# Устанавливаем права на выполнение для Python скриптов
RUN chmod +x *.py

# Рабочая директория для данных (где будут монтироваться медиа файлы)
WORKDIR /data

# Добавляем /app в PATH для удобного запуска скриптов и настраиваем поддержку цветов
ENV PATH="/app:$PATH"
ENV FORCE_COLOR=1

# По умолчанию – запуск bash, но поддерживаем прямой вызов команд
CMD ["bash"]