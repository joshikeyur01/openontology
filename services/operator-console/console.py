"""Read-only operator surface for the OpenOntology pipeline.

Grafana answers "how is the system behaving". This answers the question an
operator actually has in front of them: *what is wrong with which asset, and
what am I being told to do about it.* Those need per-asset detail, which is
exactly what a metrics backend must not carry — one series per asset is how a
Prometheus instance falls over — so it is served from live state instead.

Three sources, no database of its own:

* **Redis** holds current twin state, written by the engine. Read on demand.
* **ontology.mutations** and **ontology.commands** are consumed into bounded
  in-memory ring buffers. Losing them on restart is fine: they are a recent
  activity feed, and both topics retain the real history.

Strictly read-only. It consumes two topics and reads Redis; it never produces,
never writes a key, and holds no licence. Nothing an operator does here can
reach the asset — this is a window, not a control panel.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Iterable

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

logging.basicConfig(
    level=os.environ.get("OO_LOG_LEVEL", "INFO"),
    format='{"timestamp":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
)
logger = logging.getLogger("openontology.console")

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

KAFKA_BOOTSTRAP = os.environ.get("OO_KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
MUTATIONS_TOPIC = os.environ.get("OO_MUTATIONS_TOPIC", "ontology.mutations")
COMMANDS_TOPIC = os.environ.get("OO_COMMANDS_TOPIC", "ontology.commands")
REDIS_URL = os.environ.get("OO_REDIS_URL", "redis://redis:6379/0")

# Ring depth. Deliberately small: this is a live view, not an archive, and the
# whole point of holding it in memory is that it stays bounded no matter how
# long the process runs or how hard the fleet is alarming.
HISTORY = int(os.environ.get("OO_CONSOLE_HISTORY", "200"))

# How often the browser is pushed a fresh snapshot. Human-scale — the value of
# a faster tick is nil and the cost is a Redis scan per client per tick.
STREAM_INTERVAL = float(os.environ.get("OO_CONSOLE_STREAM_INTERVAL", "2.0"))


class ActivityLog:
    """Bounded, newest-first history of what crossed the two topics."""

    def __init__(self, capacity: int) -> None:
        self._mutations: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._commands: deque[dict[str, Any]] = deque(maxlen=capacity)
        # Monotonic, bumped on every append. The browser compares it against
        # what it last rendered, so a tick that changed nothing costs a
        # comparison rather than a re-render.
        self.revision = 0

    def add_mutation(self, record: dict[str, Any]) -> None:
        rule = record.get("rule") or {}
        context = record.get("ontology_context") or {}
        self._mutations.appendleft(
            {
                "event_id": record.get("event_id"),
                "asset_id": record.get("asset_id"),
                "asset_name": context.get("asset_name"),
                "transition": record.get("transition"),
                "severity": record.get("severity"),
                "sensor_id": rule.get("sensor_id"),
                "observed_value": rule.get("observed_value"),
                "threshold": rule.get("threshold"),
                "unit": rule.get("unit"),
                "exceeded_pct": rule.get("exceeded_pct"),
                "degraded": record.get("degraded", False),
                "degraded_reason": record.get("degraded_reason"),
                "criticality": context.get("criticality"),
                "site": context.get("site"),
                "source": context.get("source"),
                "emitted_at": record.get("emitted_at"),
            }
        )
        self.revision += 1

    def add_command(self, record: dict[str, Any]) -> None:
        self._commands.appendleft(
            {
                "command_id": record.get("command_id"),
                "plan_id": record.get("plan_id"),
                # The command schema calls this source_event_id — it is the id
                # of the mutation that caused the command, not an id of the
                # command itself. Reading "event_id" here silently produced
                # None for every record, so the join below never matched and
                # the focus card never appeared.
                "event_id": record.get("source_event_id") or record.get("event_id"),
                "asset_id": record.get("target_asset_id") or record.get("asset_id"),
                "component": record.get("target_component"),
                "action": record.get("action_type") or record.get("action"),
                "priority": record.get("execution_priority") or record.get("priority"),
                "sequence": record.get("sequence"),
                "assigned_to": record.get("assigned_to"),
                "requires_human_approval": record.get("requires_human_approval"),
                "expected_effect": record.get("expected_effect"),
                "rollback": record.get("rollback"),
                "confidence": record.get("confidence"),
                "tenant": record.get("tenant"),
                "issued_at": record.get("issued_at") or record.get("emitted_at"),
            }
        )
        self.revision += 1

    def mutations(self, limit: int) -> list[dict[str, Any]]:
        return list(self._mutations)[:limit]

    def commands(self, limit: int) -> list[dict[str, Any]]:
        return list(self._commands)[:limit]

    def plans_for(self, event_id: str | None) -> list[dict[str, Any]]:
        """Commands produced from one mutation, in execution order.

        This is the join that makes the page worth looking at: an alarm on the
        left, and the sequence it caused on the right.
        """
        if not event_id:
            return []
        matched = [c for c in self._commands if c.get("event_id") == event_id]
        return sorted(matched, key=lambda c: c.get("sequence") or 0)


class TwinReader:
    """Reads live twin state written by the engine.

    The keyspace is the engine's, and this only ever reads it:
      twin:<asset>:<sensor>       latest reading
      twinindex:<asset>           the asset's sensor set, so no key globbing
      twinalarm:<asset>:<sensor>  current alarm, absent once CLEARED
    """

    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    async def asset_ids(self) -> list[str]:
        """Every asset with live state, via SCAN over the index keys.

        SCAN rather than KEYS: this runs against the same Redis the ingestion
        hot path uses, and KEYS blocks the server for the length of the sweep.
        SCAN is cursored and yields, so a console refresh cannot stall telemetry
        being written.
        """
        assets: set[str] = set()
        async for key in self._client.scan_iter(match="twinindex:*", count=200):
            assets.add(key.split(":", 1)[1])
        return sorted(assets)

    async def snapshot(self, asset_id: str) -> dict[str, Any]:
        sensors = sorted(await self._client.smembers(f"twinindex:{asset_id}"))
        if not sensors:
            return {"asset_id": asset_id, "sensors": [], "alarm": None}

        # One pipeline for the whole asset rather than two round trips per
        # sensor. A fleet view is O(assets x sensors) reads and the latency
        # shows immediately without this.
        pipe = self._client.pipeline()
        for sensor in sensors:
            pipe.hgetall(f"twin:{asset_id}:{sensor}")
        for sensor in sensors:
            pipe.hgetall(f"twinalarm:{asset_id}:{sensor}")
        results = await pipe.execute()

        readings = results[: len(sensors)]
        alarms = results[len(sensors) :]
        now_ms = time.time() * 1000

        channels: list[dict[str, Any]] = []
        active: list[dict[str, Any]] = []

        for sensor, reading, alarm in zip(sensors, readings, alarms):
            if not reading:
                continue
            observed_ms = float(reading.get("observed_at_ms") or 0)
            channel = {
                "sensor_id": sensor,
                "value": _as_float(reading.get("value")),
                "unit": reading.get("unit") or "",
                "observed_at_ms": observed_ms,
                "age_seconds": round(max(0.0, (now_ms - observed_ms) / 1000), 1) if observed_ms else None,
                "alarm": None,
            }
            if alarm:
                state = {
                    "severity": alarm.get("severity"),
                    "transition": alarm.get("transition"),
                    "breach_count": int(alarm.get("breach_count") or 0),
                    "active_since": alarm.get("active_since"),
                    "value": _as_float(alarm.get("value")),
                }
                channel["alarm"] = state
                active.append({"sensor_id": sensor, **state})
            channels.append(channel)

        # An asset's headline state is its worst channel: one CRITICAL among
        # ten nominal readings is a CRITICAL asset.
        headline = None
        if active:
            headline = max(active, key=lambda a: 1 if a.get("severity") == "CRITICAL" else 0)

        return {
            "asset_id": asset_id,
            "sensors": channels,
            "alarm": headline,
            "alarm_count": len(active),
        }


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class TopicTail:
    """Tails a topic into the activity log.

    Joins no consumer group and commits no offsets: this is an observer, and it
    must never influence the lag or delivery of the real consumers. It starts at
    the end of the log, so the page shows what is happening now rather than
    replaying a month of retention on every restart.
    """

    def __init__(self, topic: str, handler, name: str) -> None:
        self._topic = topic
        self._handler = handler
        self._name = name
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task | None = None
        self.connected = False

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"tail-{self._name}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._consumer:
            await self._consumer.stop()

    async def _run(self) -> None:
        backoff = 1.0
        while True:
            try:
                self._consumer = AIOKafkaConsumer(
                    self._topic,
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    # No group_id: an unmanaged consumer, invisible to the
                    # broker's group coordinator and to consumer-lag metrics.
                    group_id=None,
                    auto_offset_reset="latest",
                    enable_auto_commit=False,
                    value_deserializer=lambda raw: raw,
                )
                await self._consumer.start()
                self.connected = True
                backoff = 1.0
                logger.info("tailing %s", self._topic)

                async for message in self._consumer:
                    try:
                        self._handler(json.loads(message.value))
                    except (json.JSONDecodeError, TypeError, AttributeError):
                        # A record this console cannot parse is not its problem
                        # to solve — the pipeline has dead-letter topics for
                        # that. Skipping keeps the view alive.
                        logger.warning("skipped an unparseable record on %s", self._topic)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.connected = False
                logger.warning("tail of %s failed: %s; retrying in %.0fs", self._topic, exc, backoff)
                with contextlib.suppress(Exception):
                    if self._consumer:
                        await self._consumer.stop()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    activity = ActivityLog(HISTORY)
    client = aioredis.from_url(REDIS_URL, decode_responses=True)

    app.state.started_at = time.time()
    app.state.activity = activity
    app.state.redis = client
    app.state.twins = TwinReader(client)
    app.state.tails = [
        TopicTail(MUTATIONS_TOPIC, activity.add_mutation, "mutations"),
        TopicTail(COMMANDS_TOPIC, activity.add_command, "commands"),
    ]

    for tail in app.state.tails:
        await tail.start()

    logger.info("operator console ready")
    yield

    for tail in app.state.tails:
        await tail.stop()
    await client.aclose()
    logger.info("operator console shutting down")


app = FastAPI(
    title="OpenOntology Operator Console",
    description="Read-only live view of twin state, alarms and issued commands.",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
)


async def build_state(request: Request, limit: int = 40) -> dict[str, Any]:
    activity: ActivityLog = request.app.state.activity
    twins: TwinReader = request.app.state.twins

    assets: list[dict[str, Any]] = []
    redis_ok = True
    try:
        for asset_id in await twins.asset_ids():
            assets.append(await twins.snapshot(asset_id))
    except Exception as exc:
        # Redis being unreachable degrades the page to its activity feed rather
        # than blanking it. The feed comes from Kafka and is still true.
        redis_ok = False
        logger.warning("twin state unavailable: %s", exc)

    # Alarming assets first, then by name, so the thing that needs attention is
    # never below the fold.
    def rank(asset: dict[str, Any]) -> tuple[int, str]:
        alarm = asset.get("alarm")
        if not alarm:
            return (2, asset["asset_id"])
        return (0 if alarm.get("severity") == "CRITICAL" else 1, asset["asset_id"])

    assets.sort(key=rank)

    mutations = activity.mutations(limit)
    commands = activity.commands(limit)

    return {
        "generated_at": time.time(),
        "revision": activity.revision,
        "healthy": redis_ok,
        "assets": assets,
        "mutations": mutations,
        "commands": commands,
        # The newest alarm with its resulting plan attached, which is the
        # single most useful thing on the page.
        "focus": _focus(activity, mutations),
        "counts": {
            "assets": len(assets),
            "alarming": sum(1 for a in assets if a.get("alarm")),
            "mutations": len(mutations),
            "commands": len(commands),
        },
    }


def _focus(activity: ActivityLog, mutations: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    for mutation in mutations:
        if mutation.get("transition") == "CLEARED":
            continue
        plan = activity.plans_for(mutation.get("event_id"))
        if plan:
            return {"mutation": mutation, "commands": plan}
    return None


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/state")
async def api_state(request: Request) -> JSONResponse:
    return JSONResponse(await build_state(request))


@app.get("/api/stream")
async def api_stream(request: Request) -> StreamingResponse:
    """Server-sent events: a snapshot every tick, and only when it changed.

    SSE rather than a WebSocket because the traffic is strictly one-way and SSE
    reconnects on its own, so a page left open through a `make up` recovers
    without anyone reloading it.
    """

    async def events() -> AsyncIterator[str]:
        last_revision = -1
        last_push = 0.0
        while True:
            if await request.is_disconnected():
                break

            state = await build_state(request)
            changed = state["revision"] != last_revision
            # A keepalive every 15s even when nothing changed, so proxies do
            # not time the connection out mid-quiet-period.
            if changed or (time.time() - last_push) > 15:
                last_revision = state["revision"]
                last_push = time.time()
                yield f"data: {json.dumps(state)}\n\n"

            await asyncio.sleep(STREAM_INTERVAL)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            # Without this, a reverse proxy will buffer the stream and the page
            # updates in bursts minutes apart, or not at all.
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/healthz")
async def healthz(request: Request) -> dict[str, Any]:
    return {
        "status": "ok",
        "service": "openontology-operator-console",
        "uptime_seconds": round(time.time() - request.app.state.started_at, 3),
    }


@app.get("/readyz")
async def readyz(request: Request) -> JSONResponse:
    redis_ok = False
    error: str | None = None
    try:
        await request.app.state.redis.ping()
        redis_ok = True
    except Exception as exc:
        error = str(exc)

    tails = {tail._name: tail.connected for tail in request.app.state.tails}

    # A console with no Redis shows no twin state, and one with no Kafka shows
    # no activity. Neither stops it serving, but both are worth reporting as
    # not-ready rather than presenting an empty page as an accurate one.
    ready = redis_ok and all(tails.values())
    body = {
        "status": "ready" if ready else "degraded",
        "redis": {"reachable": redis_ok, **({"error": error} if error else {})},
        "topics": tails,
        "history_depth": HISTORY,
    }
    return JSONResponse(status_code=200 if ready else 503, content=body)
