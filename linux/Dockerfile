# Используем официальный образ Ubuntu
FROM ubuntu:latest

# Устанавливаем базовые утилиты
RUN apt-get update && apt-get install -y \
    bash \
    curl \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Создаем Volume для сохранения данных
VOLUME /home

# Запускаем bash при старте
CMD ["bash"]
