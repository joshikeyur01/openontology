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

COPY app ./app
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# prometheus_client's multiprocess mode needs a writable directory shared by
# every uvicorn worker. It is created here and owned by the runtime user so the
# non-root process can write to it; compose mounts a tmpfs over it, since these
# files are scratch that must not survive a restart.
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

RUN useradd --uid 10001 --create-home --shell /usr/sbin/nologin openontology \
 && chmod +x /usr/local/bin/entrypoint.sh \
 && mkdir -p "$PROMETHEUS_MULTIPROC_DIR" \
 && chown -R openontology:openontology /app "$PROMETHEUS_MULTIPROC_DIR"
USER openontology

EXPOSE 8000

HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 \
    CMD curl -fsS http://127.0.0.1:8000/healthz >/dev/null || exit 1

# Four workers. The quota window and the (tenant, event_id) idempotency record
# both live in Redis (OO_REDIS_URL), so they are shared across these processes
# and across replicas; set OO_REQUIRE_SHARED_STATE=true — as the compose file
# does — and a worker that cannot reach Redis refuses to start rather than
# quietly metering its own private quota.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]
