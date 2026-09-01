# What this changes

<!-- One or two sentences. The "why" belongs in the commit message; this is the
     summary a reviewer reads first. -->

Closes #

## Why

<!-- What was wrong, or what became possible. If this changes a failure policy,
     a default, or a boundary, say what the trade is — that is the part review
     will actually be about. -->

## How it was verified

<!-- Delete what does not apply. Say what you ran, not what you believe. -->

- [ ] `cd services/resolution-engine && go test -race ./...`
- [ ] `cd services/ai-interceptor && OO_TEST_REQUIRE_REDIS=1 pytest tests -q`
- [ ] `cd services/command-worker && python smoke_test.py`
- [ ] `make smoke` (full topology, end to end)
- [ ] Verified by hand — describe what you observed:

## Checklist

- [ ] Commits are scoped to one concern each, with a body explaining *why* where
      it is not obvious
- [ ] `make fmt` run if Go sources changed (`gofmt -l` failing is a hard CI stop)
- [ ] Schemas regenerated with `python tools/export_schemas.py` if a payload
      model changed — `schemas/` is generated, never hand-edited
- [ ] No import of the BUSL-1.1 half from the Apache-2.0 half; the seam stays a
      network boundary
- [ ] Docs updated if behaviour, configuration or a contract changed
- [ ] No AI or tooling attribution in commits, code or docs

## Licence

<!-- Tick the one that matches the files you touched. -->

- [ ] Apache-2.0 (open core — everything outside the two interceptor services)
- [ ] BUSL-1.1 (`services/ai-interceptor`, `services/graphrag-interceptor`)

By submitting this pull request I agree my contribution is licensed under the
licence governing the files it touches.
