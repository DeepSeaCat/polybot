FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt requirements-dev.txt requirements-live.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY poly_copybot.py ./
COPY configs ./configs
COPY src ./src

EXPOSE 8088

CMD ["python", "poly_copybot.py", "run", "--config", "configs/copybot.example.json"]
