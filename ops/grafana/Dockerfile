# syntax=docker/dockerfile:1

# Grafana with its datasource, dashboard provider and dashboards baked in, so
# `make up` lands on a working dashboard with no clicking through the UI.
#
# Copied rather than bind-mounted for the same reason as ops/prometheus and
# ops/neo4j: a bind mount from a checkout under iCloud Drive, Dropbox or
# OneDrive returns EDEADLK inside the container, and Grafana would come up with
# no datasource and an empty folder rather than failing loudly.
#
# The trade is that editing a dashboard JSON needs a rebuild to take effect
# (`docker compose up -d --build grafana`). The provider is configured to keep
# watching its directory, so that remains a fast rebuild-and-reload rather than
# a restart, and the dashboards stay reviewable as diffs rather than living in
# Grafana's own database.
FROM grafana/grafana:11.5.1

COPY provisioning /etc/grafana/provisioning
# Deliberately not /var/lib/grafana/dashboards: the grafana-data named volume
# mounts over /var/lib/grafana, and on an existing volume that shadows anything
# the image put there. A dashboard edit would then appear to do nothing until
# someone ran `make clean`.
COPY dashboards /etc/grafana/dashboards
