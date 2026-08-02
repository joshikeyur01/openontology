# syntax=docker/dockerfile:1

# The ontology seeder: cypher-shell plus the versioned revisions it applies.
#
# The revisions are copied into the image rather than bind-mounted from the
# repo. A bind mount would be the obvious choice, but it makes the seeder fail
# on any checkout living under a synced directory (iCloud Drive, Dropbox),
# where the container's read of the file returns EDEADLK. Copying also makes
# `docker compose run neo4j-seed` reproducible: it applies the revisions the
# image was built from, not whatever is on disk right now.
FROM neo4j:5-community

COPY apply.sh /seed/apply.sh
COPY *.cypher /seed/

RUN chmod 0755 /seed/apply.sh

ENTRYPOINT ["bash", "/seed/apply.sh"]
