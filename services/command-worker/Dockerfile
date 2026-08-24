# syntax=docker/dockerfile:1

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY command_worker.py ./

RUN useradd --uid 10002 --create-home --shell /usr/sbin/nologin openontology \
 && chown -R openontology:openontology /app
USER openontology

EXPOSE 8082

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8082/healthz >/dev/null || exit 1

# One uvicorn worker: the consumer tasks and the licence cache live inside this
# process. Scale by raising OO_CONSUMER_WORKERS or by adding replicas — the
# Kafka consumer group distributes partitions across both.
CMD ["uvicorn", "command_worker:app", "--host", "0.0.0.0", "--port", "8082", "--workers", "1"]
