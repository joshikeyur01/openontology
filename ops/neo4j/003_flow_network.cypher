// OpenOntology flow network, revision 003.
//
// Revision 002 seeds *containment*: which system an asset belongs to, what
// parts it has, who is accountable for it. That answers "where is this asset".
// It does not answer the question an operator asks the moment something breaks:
// "if I shut this down, what stops?"
//
// This revision adds the process flow — the directed network of what feeds and
// what controls what. Two relationship types, and the difference matters:
//
//   (:Asset)-[:FEEDS]->(:Asset)     material or energy moves downstream.
//                                   Losing the source starves the target.
//   (:Asset)-[:CONTROLS]->(:Asset)  a control relationship. Losing the
//                                   controller does not starve the target; it
//                                   leaves it running unsupervised, which is
//                                   frequently worse.
//
// internal/graph has traversed exactly these two types since it was written
// (CypherResolveAssetContext, `(a)-[:FEEDS|CONTROLS*1..3]->(d:Asset)`), but
// nothing seeded them, so the blast-radius projection had no data behind it and
// every resolution returned an empty radius. This is that missing half.
//
// Direction is the whole contract. `(a)-[:FEEDS]->(b)` means a feeds b, so b is
// downstream of a and inside a's blast radius. Getting an edge backwards
// inverts a containment decision, which is why each block below names the
// physical relationship in a comment rather than relying on the reader to infer
// it from the identifiers.
//
// Every write is a MERGE, so the seeder is safe to re-run against a live graph.

// ---------------------------------------------------------------------------
// Upstream supply for the two seeded process assets.
//
// These are real plant equipment, not abstractions: the pump is fed by a
// suction drum through a strainer, and it is supervised by a controller.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'FEED-DRUM-D101'})
SET a.name           = 'Feed Drum D-101',
    a.asset_class    = 'process.vessel.drum',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'HIGH',
    a.model_number   = 'DRM-D101-CS',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'SUCTION-STRAINER-S14'})
SET a.name           = 'Suction Strainer S-14',
    a.asset_class    = 'process.filtration.strainer',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'MEDIUM',
    a.model_number   = 'STR-S14-316L',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'PLC-PUMP-CTRL-02'})
SET a.name           = 'Pump Controller PLC-02',
    a.asset_class    = 'control.plc',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'HIGH',
    a.model_number   = 'PLC-S7-1516',
    a.current_status = 'OPERATIONAL';

// ---------------------------------------------------------------------------
// Downstream consumers of the pump. This is the blast radius: everything here
// stops, or runs unsupplied, if HPP-PUMP-221 is isolated.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'HX-SHELL-TUBE-E220'})
SET a.name           = 'Shell & Tube Exchanger E-220',
    a.asset_class    = 'process.thermal.exchanger',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'HIGH',
    a.model_number   = 'HX-E220-BEM',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'REACTOR-R310'})
SET a.name           = 'Polymerisation Reactor R-310',
    a.asset_class    = 'process.reaction.cstr',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'SAFETY_CRITICAL',
    a.model_number   = 'CSTR-R310-GL',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'PRODUCT-COOLER-C450'})
SET a.name           = 'Product Cooler C-450',
    a.asset_class    = 'process.thermal.cooler',
    a.site           = 'ROTTERDAM-PLANT-3',
    a.criticality    = 'MEDIUM',
    a.model_number   = 'CLR-C450-AIR',
    a.current_status = 'OPERATIONAL';

// --- the pump's flow network ------------------------------------------------
// D-101 -> S-14 -> HPP-PUMP-221 -> E-220 -> R-310 -> C-450, with the PLC
// supervising the pump rather than supplying it.
MATCH (drum:Asset     {id: 'FEED-DRUM-D101'}),
      (strainer:Asset {id: 'SUCTION-STRAINER-S14'}),
      (pump:Asset     {id: 'HPP-PUMP-221'})
MERGE (drum)-[:FEEDS]->(strainer)
MERGE (strainer)-[:FEEDS]->(pump);

MATCH (plc:Asset  {id: 'PLC-PUMP-CTRL-02'}),
      (pump:Asset {id: 'HPP-PUMP-221'})
MERGE (plc)-[:CONTROLS]->(pump);

MATCH (pump:Asset    {id: 'HPP-PUMP-221'}),
      (hx:Asset      {id: 'HX-SHELL-TUBE-E220'}),
      (reactor:Asset {id: 'REACTOR-R310'}),
      (cooler:Asset  {id: 'PRODUCT-COOLER-C450'})
MERGE (pump)-[:FEEDS]->(hx)
MERGE (hx)-[:FEEDS]->(reactor)
MERGE (reactor)-[:FEEDS]->(cooler);

// ---------------------------------------------------------------------------
// The turbofan's flow network.
//
// Deliberately shallower than the pump's. An engine on a wing is not a process
// train, and inventing a six-deep cascade for it would be fiction. What is real
// is the fuel supply upstream and the bleed-air and hydraulic consumers
// downstream, plus the FADEC that controls it.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'FUEL-PUMP-HP-0417'})
SET a.name           = 'HP Fuel Pump #0417',
    a.asset_class    = 'aero.fuel.pump',
    a.site           = 'MRO-TOULOUSE-B2',
    a.criticality    = 'SAFETY_CRITICAL',
    a.model_number   = 'FP-HP-0417',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'FADEC-CH-A-0417'})
SET a.name           = 'FADEC Channel A #0417',
    a.asset_class    = 'aero.control.fadec',
    a.site           = 'MRO-TOULOUSE-B2',
    a.criticality    = 'SAFETY_CRITICAL',
    a.model_number   = 'FADEC-A-0417',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'BLEED-AIR-MANIFOLD-1'})
SET a.name           = 'Bleed Air Manifold 1',
    a.asset_class    = 'aero.pneumatic.manifold',
    a.site           = 'MRO-TOULOUSE-B2',
    a.criticality    = 'HIGH',
    a.model_number   = 'BAM-1-A320',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'HYD-PUMP-GREEN-1'})
SET a.name           = 'Green Hydraulic Pump 1',
    a.asset_class    = 'aero.hydraulic.pump',
    a.site           = 'MRO-TOULOUSE-B2',
    a.criticality    = 'SAFETY_CRITICAL',
    a.model_number   = 'HYD-G1-A320',
    a.current_status = 'OPERATIONAL';

MATCH (fuel:Asset     {id: 'FUEL-PUMP-HP-0417'}),
      (turbofan:Asset {id: 'TURBOFAN-A320-0417'})
MERGE (fuel)-[:FEEDS]->(turbofan);

MATCH (fadec:Asset    {id: 'FADEC-CH-A-0417'}),
      (turbofan:Asset {id: 'TURBOFAN-A320-0417'})
MERGE (fadec)-[:CONTROLS]->(turbofan);

MATCH (turbofan:Asset {id: 'TURBOFAN-A320-0417'}),
      (bleed:Asset    {id: 'BLEED-AIR-MANIFOLD-1'}),
      (hyd:Asset       {id: 'HYD-PUMP-GREEN-1'})
MERGE (turbofan)-[:FEEDS]->(bleed)
MERGE (turbofan)-[:FEEDS]->(hyd);

// ---------------------------------------------------------------------------
// The mill drives a conveyor and is supplied by a coolant skid. One hop each
// way — a machine tool's blast radius genuinely is small, and showing that is
// as useful as showing the reactor train's is large.
// ---------------------------------------------------------------------------
MERGE (a:Asset {id: 'COOLANT-SKID-K3'})
SET a.name           = 'Coolant Skid K3',
    a.asset_class    = 'shop.coolant.skid',
    a.site           = 'MANCHESTER-SHOPFLOOR-1',
    a.criticality    = 'MEDIUM',
    a.model_number   = 'CSK-K3',
    a.current_status = 'OPERATIONAL';

MERGE (a:Asset {id: 'CONVEYOR-CV12'})
SET a.name           = 'Outfeed Conveyor CV-12',
    a.asset_class    = 'shop.handling.conveyor',
    a.site           = 'MANCHESTER-SHOPFLOOR-1',
    a.criticality    = 'LOW',
    a.model_number   = 'CV-12-BELT',
    a.current_status = 'OPERATIONAL';

MATCH (skid:Asset {id: 'COOLANT-SKID-K3'}),
      (mill:Asset {id: 'CNC-MILL-07'}),
      (cv:Asset   {id: 'CONVEYOR-CV12'})
MERGE (skid)-[:FEEDS]->(mill)
MERGE (mill)-[:FEEDS]->(cv);
