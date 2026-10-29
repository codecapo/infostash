# 0002. Pin unreleased iceberg-rust from git

- **Status:** Accepted
- **Date:** 2026-10-27 (recorded 2026-10-29)

## Context

We want DataFusion 55 and Arrow 59. The latest released `iceberg` crate
does not support them; support is merged upstream but unreleased
(apache/iceberg-rust#3290). The DataFusion integration has also moved out of
iceberg-rust into its own repository, apache/datafusion-iceberg.

## Decision

Depend on both from git, pinned to exact commits in the root `Cargo.toml`:

- `iceberg` from apache/iceberg-rust
- `iceberg-datafusion` from apache/datafusion-iceberg

The `iceberg` commit must equal the one datafusion-iceberg pins, or Cargo
builds two copies of `iceberg` and their types don't match. Arrow and Parquet
versions must match the ones `iceberg` uses.

## Consequences

- We get current DataFusion and Arrow now instead of waiting for a release.
- Upgrades are manual: bump both commits together and check the
  `iceberg` commit in datafusion-iceberg's `Cargo.toml`. Dependabot can't do it.
- **crates.io rejects crates with git dependencies.** We can't publish our
  crates until both projects release versions we can depend on. Binaries and
  container images are not affected.
- When an `iceberg` release supports our DataFusion version, switch to it and
  mark this ADR superseded.
