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

COPY graphrag_interceptor.py bridge.py service.py ./

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin openontology \
 && chown -R openontology:openontology /app
USER openontology

EXPOSE 8010

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8010/healthz >/dev/null || exit 1

# One worker. The two-agent loop is CPU-light and in-process, and the command
# cache is per-process — a second worker would replay from its own cache and
# double the inference bill for a redelivered mutation, which is the same class
# of bug the standard interceptor moved its plan store to Redis to fix.
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8010", "--workers", "1"]
