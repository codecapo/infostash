# 0003. Medallion layers are Iceberg namespaces

- **Status:** Accepted
- **Date:** 2026-10-27 (recorded 2026-10-29)

## Context

Every table belongs to one layer: Raw (data as it arrived), Clean (typed,
deduplicated, checked) or Ready (business tables). Other engines reading our
tables need to see the layer, and users must be able to name a table in SQL
without extra syntax.

## Decision

Each layer is an Iceberg namespace, and a table is `<namespace>.<name>`:

| Layer (UI and docs) | Namespace | Also accepted in SQL |
|---|---|---|
| Raw | `bronze` | `raw` |
| Clean | `silver` | `clean` |
| Ready | `gold` | `ready` |

The mapping lives in `Layer::namespace` and `Layer::from_namespace` in
`crates/model`. Rows that fail a table's checks go to
`<namespace>.<name>__quarantine` in the same layer (`TableRef::quarantine`).

## Consequences

- Any Iceberg engine sees `bronze.orders`, `silver.orders` and
  `gold.daily_revenue` with no Infostash-specific metadata.
- Bronze, silver and gold are widely known names; Raw, Clean and Ready are
  easier for analysts. Accepting both in SQL costs one alias table.
- Only three layers and one level of namespace. Multi-tenant or per-domain
  namespaces (`sales.silver.orders`) would need a new ADR.
- Names ending in `__quarantine` are reserved.
