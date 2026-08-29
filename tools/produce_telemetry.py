#!/usr/bin/env python3
"""Generate newline-delimited telemetry for the telemetry.raw topic.

Standard library only, so it runs anywhere Python 3.8+ is installed. It writes
``<asset_id>|<json>`` lines to stdout; pipe them into any Kafka producer:

    python3 tools/produce_telemetry.py --count 240 \\
      | docker compose exec -T kafka kafka-console-producer \\
          --bootstrap-server localhost:29092 --topic telemetry.raw \\
          --property parse.key=true --property key.separator='|'

Keying by asset matters: unkeyed records round-robin across partitions, so a
channel's samples arrive interleaved and the engine's monotonic-timestamp guard
correctly discards most of them as stale. Keying pins each asset to one
partition, which is what a real gateway does.

The scenario ramps each asset from nominal into a threshold breach and back out
again, so a single run exercises RAISED, ESCALATED, SUSTAINED and CLEARED.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import sys
from datetime import datetime, timedelta, timezone

# Channels the resolution engine governs, plus context-only channels that make
# the telemetry snapshot genuinely multi-variable.
CHANNELS = {
    "vibration_index": {"unit": "mm/s", "nominal": 4.2, "noise": 0.35, "peak": 12.4, "governed": True},
    "temperature_celsius": {"unit": "degC", "nominal": 78.0, "noise": 1.8, "peak": 121.5, "governed": True},
    "oil_pressure_bar": {"unit": "bar", "nominal": 5.4, "noise": 0.12, "peak": 4.1, "governed": False},
    "shaft_speed_rpm": {"unit": "rpm", "nominal": 9800.0, "noise": 45.0, "peak": 10250.0, "governed": False},
}


def ramp(progress: float, anomaly_start: float, recovery_start: float) -> float:
    """Return 0..1 excursion weight for a point in the run."""
    if progress < anomaly_start:
        return 0.0
    if progress < recovery_start:
        span = max(recovery_start - anomaly_start, 1e-9)
        # Smooth climb so severity crosses HIGH before CRITICAL.
        return min(1.0, (progress - anomaly_start) / span)
    span = max(1.0 - recovery_start, 1e-9)
    return max(0.0, 1.0 - (progress - recovery_start) / span)


def sample(channel: str, spec: dict, weight: float, rng: random.Random) -> float:
    base = spec["nominal"] + rng.gauss(0.0, spec["noise"])
    excursion = (spec["peak"] - spec["nominal"]) * weight
    # A little periodicity keeps the trace from looking like a straight line.
    wobble = math.sin(weight * math.pi) * spec["noise"] * 0.5
    return round(base + excursion + wobble, 4)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--count", type=int, default=240, help="samples per asset (default: 240)")
    parser.add_argument(
        "--assets",
        default="TURBOFAN-A320-0417,HPP-PUMP-221",
        help="comma-separated asset identifiers (default: two fixture assets)",
    )
    parser.add_argument("--interval-seconds", type=float, default=2.0, help="simulated spacing between samples")
    parser.add_argument("--anomaly-at", type=float, default=0.45, help="fraction of the run where the excursion starts")
    parser.add_argument("--recovery-at", type=float, default=0.85, help="fraction of the run where recovery starts")
    parser.add_argument("--seed", type=int, default=20260807, help="RNG seed for reproducible traces")
    parser.add_argument("--nominal-only", action="store_true", help="never breach a threshold")
    parser.add_argument("--key-separator", default="|", help="separator between record key and value")
    parser.add_argument(
        "--no-key",
        action="store_true",
        help="emit bare JSON without a record key (records will round-robin across partitions)",
    )
    args = parser.parse_args()

    if not 0.0 <= args.anomaly_at < args.recovery_at <= 1.0:
        parser.error("--anomaly-at must be < --recovery-at and both within [0, 1]")

    assets = [asset.strip() for asset in args.assets.split(",") if asset.strip()]
    if not assets:
        parser.error("--assets must name at least one asset")

    rng = random.Random(args.seed)
    start = datetime.now(tz=timezone.utc) - timedelta(seconds=args.interval_seconds * args.count)

    emitted = 0
    for index in range(args.count):
        progress = index / max(args.count - 1, 1)
        weight = 0.0 if args.nominal_only else ramp(progress, args.anomaly_at, args.recovery_at)
        observed_at = start + timedelta(seconds=args.interval_seconds * index)

        for asset_id in assets:
            for channel, spec in CHANNELS.items():
                event = {
                    "asset_id": asset_id,
                    "sensor_id": channel,
                    "value": sample(channel, spec, weight, rng),
                    "unit": spec["unit"],
                    "timestamp": observed_at.isoformat().replace("+00:00", "Z"),
                }
                record = json.dumps(event, separators=(",", ":"))
                if not args.no_key:
                    # Key by asset so every sample for an asset lands on one
                    # partition and stays in observation order.
                    record = f"{asset_id}{args.key_separator}{record}"
                sys.stdout.write(record + "\n")
                emitted += 1

        if index % 20 == 0:
            sys.stdout.flush()

    sys.stdout.flush()
    print(f"emitted {emitted} telemetry events for {len(assets)} asset(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
