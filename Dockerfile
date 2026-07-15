FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY alembic ./alembic
COPY arbitrage_bot ./arbitrage_bot
COPY alembic.ini ./

EXPOSE 8000

CMD ["uvicorn", "arbitrage_bot.main:app", "--host", "0.0.0.0", "--port", "8000"]
