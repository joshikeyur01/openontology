#!/usr/bin/env python3
"""Prove the licence quota is a total, not a per-worker allowance.

Standard library only, so it runs anywhere Python 3.8+ is installed:

    python3 tools/quota_probe.py --requests 90 --expect-allowed 60

The interceptor image runs four uvicorn workers. With a process-local sliding
window each of them meters its own private allowance, so a 60 rpm subscription
is worth up to 240 requests a minute and the number the price list is built on
is wrong by the replica count. With the window in Redis the answer is 60 no
matter how many workers the load balancer spreads the requests across — and
because the requests are fired concurrently, it is also a check that the
admission decision is atomic rather than a read-then-write that every in-flight
request wins.

The probe hits ``/v1/license``: it passes through the same licensing middleware
and the same quota check as ``/v1/intercept``, but costs no inference. It reads
``/readyz`` first and refuses to report a pass unless the service says its state
really is shared, since four independent in-process limiters would otherwise
produce a plausible-looking number here.

Exit code 0 when the observed split matches the expectation, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

DEFAULT_BASE_URL = "http://localhost:8000"
#: The STANDARD demo subscription: 60 requests/minute, ai.intercept entitled.
DEFAULT_KEY = "oo-live-standard-demo-key"


def get(url: str, key: str, timeout: float) -> tuple[int, dict]:
    """Return ``(status, headers)`` for one metered request."""
    request = urllib.request.Request(url, headers={"X-License-Key": key})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers)
    except Exception as exc:  # network failure is a probe failure, not a 429
        print(f"  request failed: {exc}", file=sys.stderr)
        return 0, {}


def read_readyz(base_url: str, timeout: float) -> dict:
    try:
        with urllib.request.urlopen(f"{base_url}/readyz", timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            return json.loads(exc.read().decode("utf-8"))
        except Exception:
            return {}
    except Exception as exc:
        print(f"could not read /readyz: {exc}", file=sys.stderr)
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--key", default=DEFAULT_KEY, help="licence key to meter")
    parser.add_argument("--requests", type=int, default=90, help="requests to fire")
    parser.add_argument(
        "--expect-allowed",
        type=int,
        default=60,
        help="quota_per_minute of the key; the exact number that must succeed",
    )
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--allow-unshared",
        action="store_true",
        help="run even when /readyz reports process-local state (expected to fail)",
    )
    args = parser.parse_args()

    ready = read_readyz(args.base_url, args.timeout)
    backend = ready.get("state_backend", "unknown")
    print(f"state backend      : {backend}")
    print(f"redis              : {json.dumps(ready.get('redis', {}))}")

    if backend != "redis" and not args.allow_unshared:
        print(
            "\nFAIL: quota state is process-local; this probe cannot prove anything.\n"
            "      Set OO_REDIS_URL on the service, or pass --allow-unshared to watch it fail.",
            file=sys.stderr,
        )
        return 1

    url = f"{args.base_url}/v1/license"
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        results = list(pool.map(lambda _: get(url, args.key, args.timeout), range(args.requests)))

    statuses = Counter(status for status, _ in results)
    allowed = statuses[200]
    throttled = statuses[429]

    print(f"requests fired     : {args.requests}")
    print(f"200 OK             : {allowed}")
    print(f"429 quota exceeded : {throttled}")
    for status, count in sorted(statuses.items()):
        if status not in (200, 429):
            print(f"{status:<19}: {count}  <-- unexpected")

    retry_after = next((h.get("Retry-After") for status, h in results if status == 429), None)
    if retry_after:
        print(f"Retry-After        : {retry_after}s")

    if allowed == args.expect_allowed and allowed + throttled == args.requests:
        print(
            f"\nPASS: exactly {allowed} requests admitted across every worker, "
            f"not {allowed} per worker."
        )
        return 0

    print(
        f"\nFAIL: expected exactly {args.expect_allowed} admitted and "
        f"{args.requests - args.expect_allowed} throttled.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
