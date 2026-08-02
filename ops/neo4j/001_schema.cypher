// OpenOntology topology schema, revision 001.
//
// Applied by the neo4j-seed one-shot before any data is loaded. Everything here
// is IF NOT EXISTS, so re-running the seeder against a live graph is a no-op.

// Identity. The resolution engine looks assets up by (:Asset {id}) on the
// ingestion hot path, so this constraint is doing double duty: it rejects
// duplicate equipment records at write time, and its backing index is what
// keeps the lookup a seek rather than a label scan.
CREATE CONSTRAINT asset_id_unique IF NOT EXISTS
FOR (a:Asset) REQUIRE a.id IS UNIQUE;

CREATE CONSTRAINT system_id_unique IF NOT EXISTS
FOR (s:System) REQUIRE s.id IS UNIQUE;

CREATE CONSTRAINT component_id_unique IF NOT EXISTS
FOR (c:Component) REQUIRE c.id IS UNIQUE;

CREATE CONSTRAINT operator_id_unique IF NOT EXISTS
FOR (o:Operator) REQUIRE o.id IS UNIQUE;

// Site and criticality are how an operator slices the fleet ("show me every
// SAFETY_CRITICAL asset in Toulouse"), which is a scan without these.
CREATE INDEX asset_site IF NOT EXISTS FOR (a:Asset) ON (a.site);
CREATE INDEX asset_criticality IF NOT EXISTS FOR (a:Asset) ON (a.criticality);
