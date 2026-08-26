#!/usr/bin/env python3
"""Emit the published payload contracts as JSON Schema.

The schemas under ``schemas/`` are generated, never hand-edited. They are
derived from the Pydantic models the commercial interceptor actually parses
with, which makes them the *consumer's* view of each contract: what a caller
must send for the paid API to accept it, rather than what the Go engine happens
to emit today. Those two agree, and ``schema_conformance_test.go`` in the engine
is what keeps them agreeing — it validates a freshly built mutation against the
committed schema, so a producer-side field rename fails the Go build rather than
silently drifting from the published contract.

Usage (from the repository root):

    python tools/export_schemas.py            # write schemas/
    python tools/export_schemas.py --check    # fail if they are out of date

``--check`` is what CI runs, so a model change that nobody regenerated is a red
build rather than a stale file nobody notices.

Only the *current* contract is generated. Superseded versions
(``openontology.mutation.v1``) stay in ``schemas/`` as frozen artifacts and are
deliberately not regenerated: a published version is immutable by definition,
and the models here have moved on. They remain because the interceptor still
accepts them, so a consumer on the old contract needs to be able to read it.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
INTERCEPTOR = REPO_ROOT / "services" / "ai-interceptor"
OUTPUT_DIR = REPO_ROOT / "schemas"

# The interceptor is not an installed package; it is a service directory. Adding
# it to the path here keeps the export script runnable from a bare checkout with
# only the interceptor's requirements installed.
sys.path.insert(0, str(INTERCEPTOR))

from app import models  # noqa: E402  (import follows the sys.path edit above)

BASE_ID = "https://openontology.dev/schemas"


def _document(model: type, schema_id: str, title: str, description: str) -> dict:
    """Render one model as a self-describing JSON Schema document."""
    schema = model.model_json_schema(mode="serialization")
    # The model's own title is its Python class name, which is an
    # implementation detail of the interceptor rather than the name of the
    # published contract. The metadata below is applied after the merge so the
    # contract's name wins over the class's.
    document = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"{BASE_ID}/{schema_id}.schema.json",
    }
    document.update(schema)
    document["title"] = title
    document["description"] = description
    return document


def documents() -> dict[str, dict]:
    return {
        "openontology.mutation.v2": _document(
            models.EnrichedContextPayload,
            "openontology.mutation.v2",
            "Enriched Context Payload",
            (
                "Published by the Go ontology resolution engine to the "
                "ontology.mutations topic when an anomaly rule changes an "
                "asset's alarm state. Keyed by asset_id so every mutation for "
                "one asset stays ordered on one partition. v2 adds the process "
                "flow around the asset: upstream_dependencies, "
                "downstream_impacts and blast_radius."
            ),
        ),
        "openontology.command-sequence.v1": _document(
            models.CommandSequence,
            "openontology.command-sequence.v1",
            "Actionable Command Sequence",
            (
                "Returned by POST /v1/intercept and republished by the command "
                "worker to the ontology.commands topic. Fields the server owns "
                "— plan_id, model, usage, latency — are set by the service and "
                "cannot be asserted by the model that produced the commands."
            ),
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed schemas differ from the models",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)
    stale: list[str] = []

    for name, document in documents().items():
        rendered = json.dumps(document, indent=2, sort_keys=False) + "\n"
        target = OUTPUT_DIR / f"{name}.schema.json"

        if args.check:
            current = target.read_text() if target.exists() else ""
            if current != rendered:
                stale.append(name)
            continue

        target.write_text(rendered)
        print(f"wrote {target.relative_to(REPO_ROOT)}")

    if stale:
        print(
            "these schemas are out of date with the models: "
            + ", ".join(sorted(stale))
            + "\nrun: python tools/export_schemas.py",
            file=sys.stderr,
        )
        return 1

    if args.check:
        print("schemas are up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
