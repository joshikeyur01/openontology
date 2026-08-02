#!/usr/bin/env bash
# Applies every versioned .cypher revision in this directory, in lexical order.
#
# Run as the neo4j-seed one-shot in docker-compose, and re-runnable by hand
# (`make graph-seed`) after editing a revision. Every statement in every file is
# CREATE ... IF NOT EXISTS or MERGE, so applying twice changes nothing.
set -euo pipefail

URI="${OO_NEO4J_URI:-bolt://neo4j:7687}"
USER="${OO_NEO4J_USER:-neo4j}"
PASSWORD="${OO_NEO4J_PASSWORD:?OO_NEO4J_PASSWORD must be set}"
DATABASE="${OO_NEO4J_DATABASE:-neo4j}"
REVISIONS_DIR="${OO_NEO4J_REVISIONS:-/seed}"

shell() {
  cypher-shell --address "$URI" --username "$USER" --password "$PASSWORD" \
    --database "$DATABASE" --fail-fast --format plain "$@"
}

# depends_on: service_healthy already gates on the server answering, but a
# healthy server can still be a few hundred milliseconds from accepting writes
# on a cold volume. Retrying here keeps a first `make up` from failing on a race.
for attempt in $(seq 1 30); do
  if shell "RETURN 1 AS ok" >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "neo4j at $URI did not accept a query after 30 attempts" >&2
    exit 1
  fi
  sleep 2
done

shopt -s nullglob
revisions=("$REVISIONS_DIR"/[0-9]*.cypher)
if [ ${#revisions[@]} -eq 0 ]; then
  echo "no .cypher revisions found in $REVISIONS_DIR" >&2
  exit 1
fi

for revision in "${revisions[@]}"; do
  echo "applying $(basename "$revision")"
  shell --file "$revision"
done

echo
echo "ontology loaded:"
shell "
MATCH (a:Asset)
OPTIONAL MATCH (a)-[:PART_OF*1..4]->(s:System)
OPTIONAL MATCH (a)-[:HAS_COMPONENT]->(c:Component)
OPTIONAL MATCH (:Operator)-[r:RESPONSIBLE_FOR]->(a)
RETURN a.id AS asset,
       a.criticality AS criticality,
       count(DISTINCT s) AS parent_systems,
       count(DISTINCT c) AS components,
       count(DISTINCT r) AS operators
ORDER BY asset
"
