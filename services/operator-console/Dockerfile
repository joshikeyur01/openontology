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

COPY console.py ./
COPY static ./static

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin openontology \
 && chown -R openontology:openontology /app
USER openontology

EXPOSE 8083

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8083/healthz >/dev/null || exit 1

# One worker, deliberately. Each worker would open its own Kafka tail and hold
# its own ring buffer, so two workers would show two different histories
# depending on which answered — and the whole service is one SSE stream per
# browser plus a Redis read, which one process handles comfortably.
CMD ["uvicorn", "console:app", "--host", "0.0.0.0", "--port", "8083", "--workers", "1"]
