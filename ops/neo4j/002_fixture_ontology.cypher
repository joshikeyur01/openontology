// OpenOntology fixture ontology, revision 002.
//
// This is the same three-asset industrial/aerospace ontology the offline
// stand-in carries in defaultFixtures() (services/resolution-engine/graph.go),
// expressed as real nodes and relationships. Keeping the two in step is what
// makes GRAPH_PROVIDER=mock and GRAPH_PROVIDER=neo4j comparable: the same
// telemetry produces the same enriched mutation either way, and the only
// difference in the payload is ontology_context.source.
//
// Shape:
//
//   (:Operator)-[:RESPONSIBLE_FOR {escalation_order}]->(:Asset)
//   (:Asset)-[:HAS_COMPONENT]->(:Component)
//   (:Asset)-[:PART_OF]->(:System)-[:PART_OF]->(:System)
//
// Ancestry is a chain rather than a fan, so the traversal depth the resolver
// reports is the real containment depth: 1 is the immediate parent.
//
// Every write is a MERGE, so the seeder is safe to re-run against a live graph.

// ---------------------------------------------------------------------------
// TURBOFAN-A320-0417 — CFM56-5B on an A320, safety critical.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'TURBOFAN-A320-0417'})
SET a.name               = 'CFM56-5B Turbofan #0417',
    a.asset_class        = 'aero.propulsion.turbofan',
    a.site               = 'MRO-TOULOUSE-B2',
    a.criticality        = 'SAFETY_CRITICAL',
    a.model_number       = 'CFM56-5B4/P',
    a.current_status     = 'OPERATIONAL',
    a.installation_date  = datetime('2019-03-11T08:00:00Z'),
    a.maintenance_window = '2026-08-12T22:00:00Z/2026-08-13T04:00:00Z';

MERGE (s1:System {id: 'SYS-PROP-A320-0417'})
SET s1.name = 'Propulsion Subsystem (Engine 1)', s1.type = 'Subsystem';
MERGE (s2:System {id: 'AIRFRAME-A320-MSN4412'})
SET s2.name = 'Airframe A320 MSN4412', s2.type = 'System';
MERGE (s3:System {id: 'FLEET-EU-SHORTHAUL'})
SET s3.name = 'EU Short-Haul Fleet', s3.type = 'Fleet';

MATCH (a:Asset {id: 'TURBOFAN-A320-0417'}),
      (s1:System {id: 'SYS-PROP-A320-0417'}),
      (s2:System {id: 'AIRFRAME-A320-MSN4412'}),
      (s3:System {id: 'FLEET-EU-SHORTHAUL'})
MERGE (a)-[:PART_OF]->(s1)
MERGE (s1)-[:PART_OF]->(s2)
MERGE (s2)-[:PART_OF]->(s3);

// Component identifiers carry an ordinal because the resolver orders components
// by id, and a work order reads better in assembly order than alphabetically.
MATCH (a:Asset {id: 'TURBOFAN-A320-0417'})
UNWIND [
  {ordinal: '01', name: 'hpt_bearing_no3'},
  {ordinal: '02', name: 'lpc_fan_module'},
  {ordinal: '03', name: 'egt_harness'},
  {ordinal: '04', name: 'fadec_channel_a'}
] AS spec
MERGE (c:Component {id: 'CMP-TURBOFAN-A320-0417-' + spec.ordinal})
SET c.name = spec.name
MERGE (a)-[:HAS_COMPONENT]->(c);

MERGE (o:Operator {id: 'OP-4471'})
SET o.name = 'L. Moreau', o.role = 'Lead Powerplant Engineer', o.shift = 'B',
    o.contact = '+33-5-6100-4471', o.certification_level = 'EASA-B1', o.active_shift = true;
MERGE (o:Operator {id: 'OP-2210'})
SET o.name = 'S. Kaur', o.role = 'Reliability Engineer', o.shift = 'B',
    o.contact = '+33-5-6100-2210', o.certification_level = 'EASA-B2', o.active_shift = true;
MERGE (o:Operator {id: 'OPS-DUTY'})
SET o.name = 'MRO Duty Manager', o.role = 'Operations Duty', o.shift = '24x7',
    o.contact = '+33-5-6100-0000', o.active_shift = true;

MATCH (a:Asset {id: 'TURBOFAN-A320-0417'})
UNWIND [
  {operator: 'OP-4471', escalation_order: 1},
  {operator: 'OP-2210', escalation_order: 2},
  {operator: 'OPS-DUTY', escalation_order: 3}
] AS spec
MATCH (o:Operator {id: spec.operator})
MERGE (o)-[r:RESPONSIBLE_FOR]->(a)
SET r.escalation_order = spec.escalation_order;

// ---------------------------------------------------------------------------
// HPP-PUMP-221 — hydraulic power pack on extrusion line 4, Rotterdam.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'HPP-PUMP-221'})
SET a.name               = 'Hydraulic Power Pack Pump 221',
    a.asset_class        = 'industrial.hydraulics.pump',
    a.site               = 'PLANT-ROTTERDAM-L4',
    a.criticality        = 'HIGH',
    a.model_number       = 'A11VO-190',
    a.current_status     = 'OPERATIONAL',
    a.installation_date  = datetime('2021-06-02T09:30:00Z'),
    a.maintenance_window = '2026-08-09T02:00:00Z/2026-08-09T06:00:00Z';

MERGE (s1:System {id: 'SYS-HYD-L4'})
SET s1.name = 'Line 4 Hydraulic Loop', s1.type = 'Subsystem';
MERGE (s2:System {id: 'LINE-4'})
SET s2.name = 'Extrusion Line 4', s2.type = 'System';
MERGE (s3:System {id: 'PLANT-ROTTERDAM'})
SET s3.name = 'Rotterdam Plant', s3.type = 'Site';

MATCH (a:Asset {id: 'HPP-PUMP-221'}),
      (s1:System {id: 'SYS-HYD-L4'}),
      (s2:System {id: 'LINE-4'}),
      (s3:System {id: 'PLANT-ROTTERDAM'})
MERGE (a)-[:PART_OF]->(s1)
MERGE (s1)-[:PART_OF]->(s2)
MERGE (s2)-[:PART_OF]->(s3);

MATCH (a:Asset {id: 'HPP-PUMP-221'})
UNWIND [
  {ordinal: '01', name: 'drive_coupling'},
  {ordinal: '02', name: 'thrust_bearing'},
  {ordinal: '03', name: 'seal_pack'},
  {ordinal: '04', name: 'vfd_inverter'}
] AS spec
MERGE (c:Component {id: 'CMP-HPP-PUMP-221-' + spec.ordinal})
SET c.name = spec.name
MERGE (a)-[:HAS_COMPONENT]->(c);

MERGE (o:Operator {id: 'OP-8801'})
SET o.name = 'J. de Vries', o.role = 'Maintenance Technician', o.shift = 'A',
    o.contact = '+31-10-555-8801', o.certification_level = 'NEN-3140', o.active_shift = true;
MERGE (o:Operator {id: 'OP-8815'})
SET o.name = 'M. Okafor', o.role = 'Line Supervisor', o.shift = 'A',
    o.contact = '+31-10-555-8815', o.active_shift = true;

MATCH (a:Asset {id: 'HPP-PUMP-221'})
UNWIND [
  {operator: 'OP-8801', escalation_order: 1},
  {operator: 'OP-8815', escalation_order: 2}
] AS spec
MATCH (o:Operator {id: spec.operator})
MERGE (o)-[r:RESPONSIBLE_FOR]->(a)
SET r.escalation_order = spec.escalation_order;

// ---------------------------------------------------------------------------
// CNC-MILL-07 — 5-axis mill in the Greenville machine shop.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'CNC-MILL-07'})
SET a.name               = '5-Axis CNC Mill 07',
    a.asset_class        = 'industrial.machining.cnc',
    a.site               = 'PLANT-GREENVILLE-C1',
    a.criticality        = 'MEDIUM',
    a.model_number       = 'DMU-50',
    a.current_status     = 'OPERATIONAL',
    a.installation_date  = datetime('2022-11-18T14:15:00Z'),
    a.maintenance_window = '2026-08-15T05:00:00Z/2026-08-15T09:00:00Z';

MERGE (s1:System {id: 'CELL-C1-03'})
SET s1.name = 'Machining Cell C1-03', s1.type = 'Subsystem';
MERGE (s2:System {id: 'SHOP-GREENVILLE'})
SET s2.name = 'Greenville Machine Shop', s2.type = 'System';

MATCH (a:Asset {id: 'CNC-MILL-07'}),
      (s1:System {id: 'CELL-C1-03'}),
      (s2:System {id: 'SHOP-GREENVILLE'})
MERGE (a)-[:PART_OF]->(s1)
MERGE (s1)-[:PART_OF]->(s2);

MATCH (a:Asset {id: 'CNC-MILL-07'})
UNWIND [
  {ordinal: '01', name: 'spindle_head'},
  {ordinal: '02', name: 'tool_changer'},
  {ordinal: '03', name: 'x_axis_ballscrew'},
  {ordinal: '04', name: 'coolant_manifold'}
] AS spec
MERGE (c:Component {id: 'CMP-CNC-MILL-07-' + spec.ordinal})
SET c.name = spec.name
MERGE (a)-[:HAS_COMPONENT]->(c);

MERGE (o:Operator {id: 'OP-1042'})
SET o.name = 'R. Alvarez', o.role = 'CNC Operator', o.shift = 'C',
    o.contact = '+1-864-555-1042', o.active_shift = true;

MATCH (a:Asset {id: 'CNC-MILL-07'}), (o:Operator {id: 'OP-1042'})
MERGE (o)-[r:RESPONSIBLE_FOR]->(a)
SET r.escalation_order = 1;

// ---------------------------------------------------------------------------
// Provenance. `MATCH (r:OntologyRevision) RETURN r ORDER BY r.revision` answers
// "what is actually loaded in this graph" without diffing node counts.
// ---------------------------------------------------------------------------
MERGE (r:OntologyRevision {revision: '002'})
SET r.description = 'Fixture ontology: TURBOFAN-A320-0417, HPP-PUMP-221, CNC-MILL-07',
    r.applied_at  = datetime();
