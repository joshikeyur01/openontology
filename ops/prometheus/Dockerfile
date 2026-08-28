# syntax=docker/dockerfile:1

# Prometheus with the topology's scrape configuration baked in.
#
# The config is copied rather than bind-mounted, for the same reason
# ops/neo4j/Dockerfile copies its revisions: a bind mount fails on any checkout
# living under a synced directory (iCloud Drive, Dropbox, OneDrive), where the
# container's read of the file returns EDEADLK and Prometheus exits with
# "resource deadlock avoided" before it has scraped anything.
#
# Copying also makes the scrape config reproducible — the running Prometheus is
# configured by what the image was built from, not by whatever is on disk now.
# Changing a scrape target therefore needs `make up` (or `docker compose up -d
# --build prometheus`), which is the same contract as the seeder.
FROM prom/prometheus:v3.1.0

COPY prometheus.yml /etc/prometheus/prometheus.yml
