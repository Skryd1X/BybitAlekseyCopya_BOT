FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# код
COPY . .

CMD ["python", "bot.py"]
