# Используем Ubuntu как базу
FROM ubuntu:24.04

# Обновляем и устанавливаем необходимые утилиты
RUN apt-get update && apt-get install -y --no-install-recommends \
    rawtherapee \
    imagemagick \
    exiftool \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /data

# По умолчанию – запуск bash, чтобы можно было заходить вручную
# Но также поддерживаем прямой вызов команд
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["bash"]