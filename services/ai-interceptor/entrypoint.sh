#!/bin/sh
# Container entrypoint for the AI-agent interceptor.
#
# Its only job is to make prometheus_client's multiprocess mode safe before
# uvicorn forks. The four workers each write metric samples into shared mmap
# files under PROMETHEUS_MULTIPROC_DIR, and a scrape sums whatever files it
# finds there. Files left behind by a previous boot are indistinguishable from
# a live worker's, so they would be summed in as well — counters would jump on
# every restart and never come back down.
#
# Emptying the directory here rather than in Python is deliberate: this runs
# once, before any worker exists. Doing it from application startup would mean
# four processes racing to delete each other's freshly created files.
set -eu

if [ -n "${PROMETHEUS_MULTIPROC_DIR:-}" ]; then
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
    # -f so an empty directory is not an error, and a glob rather than
    # `rm -rf $dir` so a bad-but-plausible value cannot delete a mounted volume.
    rm -f "$PROMETHEUS_MULTIPROC_DIR"/*.db 2>/dev/null || true
    echo "metrics: cleared $PROMETHEUS_MULTIPROC_DIR for multiprocess aggregation"
else
    echo "metrics: PROMETHEUS_MULTIPROC_DIR unset; per-worker counters only" >&2
fi

exec "$@"
