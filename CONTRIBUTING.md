# Contributing to OpenOntology

Thanks for taking the time. This file covers how to get a working environment,
what the tests expect, and the few conventions that are load-bearing rather than
matters of taste.

## Before you start: which licence covers your change

OpenOntology is open core and the licence depends on the directory:

| Directory | Licence |
|---|---|
| `services/ai-interceptor/`, `services/graphrag-interceptor/` | BUSL-1.1 |
| everything else | Apache-2.0 |

By sending a pull request you agree that your contribution is licensed under
whichever of those governs the files you touched. See [NOTICE](NOTICE) and
[docs/OPEN-CORE.md](docs/OPEN-CORE.md).

## Getting a working environment

The fastest path to a running system is [docs/QUICKSTART.md](docs/QUICKSTART.md)
— Docker only, no toolchains. For editing code you will want the language
toolchain for the part you are changing.

### Go engine

```bash
cd services/resolution-engine && go mod tidy && go build ./...
```

```bash
cd services/resolution-engine && go test -race ./...
```

`-race` is not optional here. The engine shards alarm state 64 ways and shares
one Redis pool across every consumer worker, so a data race is a plausible
failure mode rather than a theoretical one, and CI runs it.

The tests need no external services: Redis is faked with `miniredis` and the
graph resolver has a fixture provider.

### Python services

```bash
cd services/ai-interceptor && python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt
```

```bash
cd services/ai-interceptor && pytest tests -q
```

The shared-state tests need a real Redis, because the sliding window's
correctness is a property of a Lua script executing atomically inside Redis and
a Python reimplementation of it would only ever test itself. Without one they
skip themselves:

```bash
docker run -d --rm -p 6379:6379 redis:7.2-alpine
```

```bash
cd services/ai-interceptor && OO_TEST_REDIS_URL=redis://localhost:6379/0 OO_TEST_REQUIRE_REDIS=1 pytest tests -q
```

`OO_TEST_REQUIRE_REDIS=1` turns the skip into a failure. CI sets it, so a broken
service block cannot report green by skipping the tests that would catch it.

The command worker is a script first and a pytest module second:

```bash
cd services/command-worker && OO_INTERCEPTOR_MODE=stub OO_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python smoke_test.py
```

Run it both ways. Its `check()` helper collects failures into a list rather than
raising, so under pytest alone every test function returns cleanly whatever the
checks found — pytest catches import and collection breakage, the script's exit
code is the assertion that holds.

### The whole topology

```bash
make smoke
```

That is exactly what the compose CI job runs: up, wait for health, seed, assert
both output topics carry records, and assert the licence quota still holds
across all four uvicorn workers.

## Conventions that matter

**Comments explain why, not what.** The codebase leans heavily on this. A
comment restating the line below it will be asked about in review; a comment
explaining why a healthcheck uses `srvr` instead of `ruok`, or why the quota
fails open while idempotency fails closed, is the reason the file is readable a
year later.

**Failure policies are decisions, and decisions get written down.** If you add a
component that can fail, say in a comment what happens when it does and why that
is the right trade. The two idempotency layers deliberately disagree; that
disagreement is documented at both sites.

**Schemas are generated.** Never hand-edit `schemas/`. Change the Pydantic model
and run:

```bash
python tools/export_schemas.py
```

`schema_conformance_test.go` in the engine will fail if the Go producer and the
published contract disagree, in either direction.

**Do not import the commercial layer from the open core.** The seam is a network
boundary; keeping it that way is what makes the open-core claim verifiable. CI
enforces this by building and testing the open core with
`services/ai-interceptor` deleted.

**Go formatting is enforced.** `gofmt -l` failing the build is deliberate.

```bash
make fmt
```

## Sending a change

1. Open an issue first for anything larger than a bug fix, so design discussion
   happens before you have written the code.
2. Branch from `main`.
3. Keep commits scoped — one concern each, present-tense subject, and a body
   explaining *why* when it is not obvious. `git log` in this repository is
   meant to be readable as an architecture narrative.
4. Make sure `make smoke` passes and the relevant unit suites are green.
5. Open the PR against `main` and fill in the template.

Do not add AI or tooling attribution to commit messages, code comments or
documentation. Keep the code model-agnostic.

## Reporting security issues

Do not open a public issue. See [SECURITY.md](SECURITY.md).
