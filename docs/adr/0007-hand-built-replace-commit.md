# 0007. Build the replace commit ourselves on the SQL catalog

- **Status:** Accepted
- **Date:** 2026-11-09

## Context

Merging on `@unique_key` (#35) and rebuilding with `@full_refresh` (#90)
both replace data files in a table. The pinned iceberg-rust (rev `5b82ab9`)
cannot commit such a replace. Its `Transaction` offers `fast_append` and
actions for the format version, properties, schema, location, statistics,
sort order and snapshot expiry, but no overwrite or delete action, and a
`TableCommit` can only be built inside the crate, so a snapshot built
outside it cannot go through `Catalog::update_table` either. Its
copy-on-write rewrite module plans and rewrites data files but does not
commit. Upstream main had not changed this as of 2026-11-09; the pull
request adding an overwrite action (apache/iceberg-rust#3300) was open.

[ADR 0006](0006-ordered-job-commits.md) requires one atomic commit per
output table, carrying the job id, that fails rather than rebases when the
head has moved. Any replace route must keep that.

Three routes were open, to be tried in this order: build the snapshot from
public spec writers and commit it with a compare-and-swap of our own; fork
iceberg-rust through a `[patch]`; or give up replacing files in 0.1.0 and
make Clean tables append-only with read-time deduplication. The spike for
this decision (#72) had one day to commit a replace on the first route that
could.

Before any prototype code, `cargo tree -d` was run on the workspace. It
lists neither `iceberg` nor `sqlx`, so the workspace builds one copy of
each and the route can use iceberg's types and the catalog's database
driver without a second copy. The duplicates it lists are lower-level
crates such as `zstd`, `hashbrown`, `rand`, `itertools` and `syn`, none of
which the route uses directly.

The first route committed a replace within the day. A throwaway prototype
in `crates/storage` showed that two replaces prepared on one head give one
commit and one conflict, with the loser's files referenced by no snapshot,
and that exactly one wins when the two commit concurrently; that a replace
racing an append that lands first is a conflict; that the table keeps its
UUID, snapshot history and metadata log; that a reader holding the old
metadata still reads the old files; that iceberg-rust's own `fast_append`
commits on top of the hand-made snapshot and `appends_between` then reads
the appended rows incrementally; that the refusal in `appends_between`
fires on the real overwrite; and that a full refresh with no rows still
commits a snapshot that removes every live file.

The cheapest fork was probed alongside, to learn whether our snapshot
commits through `Catalog::update_table`, the path a REST catalog needs: a
copy of the pin with one line changed, making the `TableCommit` builder
public, applied through `[patch]`. The workspace still built one copy of
`iceberg`, and the patched crate built in under a minute. The same
snapshot committed through `update_table` with the UUID and head
requirements. A second commit prepared from the same head failed with the
catalog's `CatalogCommitConflicts`, which iceberg-rust marks as retryable,
and left the table as it was. Without the requirements, the catalog's own
swap did not stop a commit built on an old head: a stale snapshot carrying
a stale sequence number was refused by iceberg-rust's metadata builder,
and one given a fresh sequence number landed over the other writer's
commit, a lost update. On that path the requirements guard the head, as
they do for `fast_append`, which adds the same two. The fork that #72
would have accepted, one that processes delete entries itself, was not
attempted: upstream is adding that, and the first route had already
committed.

## Decision

We build the replace commit ourselves, in `crates/storage`, on the SQL
catalog, and keep ADR 0006's rule: one commit per table that fails rather
than rebases.

We build the snapshot from public iceberg-rust APIs. We rewrite each
manifest of the base snapshot that lists a removed file, marking that file
`DELETED` and carrying its other live files as `EXISTING` entries that keep
their snapshot id and sequence numbers; `DELETED` entries of earlier
snapshots are not copied, since such an entry names the snapshot that
deleted the file. We reuse every other manifest as it is, as `fast_append`
does, including one left with only `DELETED` entries. The new files go in
one manifest of `ADDED` entries. We write the manifest list and an
`overwrite` snapshot whose `added-*`, `deleted-*` and `total-*`
summary fields we compute ourselves, apply the snapshot, the `main`
reference and the table properties through `TableMetadataBuilder`, and
write the next `metadata.json` with `TableMetadata::write_to` at the path
`MetadataLocation::with_next_version` gives. `infostash.job-id` and the
checkpoint properties go in that same commit, placed as `append_if_head`
places them.

We commit by checking the table UUID and the `main` head with
`TableRequirement::check` against freshly loaded metadata, then running
the SQL catalog's own commit statement, less its record type filter, on a
`sqlx` SQLite pool of our own, opened on the same `catalog.db`: one
`UPDATE` of `iceberg_tables` that sets `metadata_location` and
`previous_metadata_location` where the row with the table's key still
points at the metadata the replace was built on. The filter adds nothing
here: the key is the table's primary key, so at most one row matches, and
that row must still hold the metadata location we loaded through the
catalog, which a view row never does. Without it the statement fits both
layouts of `iceberg_tables` the catalog reads and appends to, with and
without the `iceberg_type` column. Zero rows updated is `Error::Conflict`
when a re-read shows the head moved, with no retry and no rebase; an
unchanged row means the statement no longer fits the catalog and is a
storage error. The swap is what makes the commit safe, also against a
concurrent writer; the requirement check runs first to give the clearer
error and to catch a changed UUID.

We do not take the fork. The probed variant only opens a hook, costs a
fork to host and carry across every pin bump, and saves only the catalog
half of the commit, since the snapshot is built the same way on both
routes. Append-only was not needed, so RFC 0001's merge semantics and the
README's description of Clean tables stand.

## Consequences

- Clean tables can be deduplicated and models fully refreshed in 0.1.0.
  #88 builds `Warehouse::replace_if_head` to this record; #35, #89 and #90
  follow it.
- The route replaces whole data files in unpartitioned tables and writes
  no delete files. Partition-scoped replaces (#36), compaction and the
  deletion vectors `docs/architecture.md` names for updates are later work
  on the same commit.
- This works on SQLite only, because `crates/storage` enables only the
  SQLite driver of `sqlx` and the statement is the SQL catalog's. We bypass
  the `Catalog` trait, depend on the catalog's private `iceberg_tables`
  layout and `catalog_name` key, and keep a connection pool next to the
  catalog's. A layout change upstream makes the statement fail or match no
  row, and nothing is corrupted; a new column the catalog keeps in step
  with the location would be left stale, so every pin bump (ADR 0002) must
  compare the catalog's commit statement with ours.
- Phase 1's REST catalog ([ROADMAP](../../ROADMAP.md)) cannot run our
  statement. The commit must then move to an upstream overwrite action, or
  to `Catalog::update_table` once a `TableCommit` can be built outside
  iceberg-rust. On the SQL catalog the probe committed our snapshot through
  that path with the same two requirements; a REST catalog's retryable
  conflict must then be mapped to `Error::Conflict` with no retry. The
  snapshot-building half carries over; the commit half is a redo.
- A replace that loses leaves its data files on disk, as a refused append
  does, and also its manifests, manifest list and metadata file, which it
  writes before the swap. Removing unreferenced files (#40) must cover all
  of them.
- #88 must test each head guard on its own: the swap with the requirement
  check removed, and the UUID check with the table dropped and recreated
  between prepare and commit. It must also test a replace on a
  `catalog.db` whose `iceberg_tables` has no `iceberg_type` column, the
  older layout the catalog reads and appends to but cannot create tables
  on: create the table on a fresh catalog, drop the column, then reopen.
- Once #88 imports `sqlx`, the exemption that keeps it off the
  unused-dependency check in `crates/storage/Cargo.toml` goes.
- Incremental readers of a replaced table need a rule of their own:
  [ADR 0008](0008-incremental-readers-of-replaced-tables.md).
