# Architecture decision records

An ADR records one decision that has already been made: what we chose, why,
and what it costs us. They stop the same question being argued twice and
tell newcomers why the code looks the way it does.

ADRs and RFCs do different jobs:

| | RFC (`docs/rfcs/`) | ADR (`docs/adr/`) |
|---|---|---|
| When | Before building something users will depend on | After a decision is made |
| Length | As long as the design needs | One page |
| Review | One week of comment, two maintainers approve | Normal pull request |

An accepted RFC can be summarised as an ADR. Write a new ADR when you make a
choice that is hard to reverse or that a reader would otherwise question:
a dependency, a storage layout, an invariant.

ADRs are never edited after they are accepted, except for typos, links and
the status line. To change a decision, write a new ADR and change the old
one's status to **Superseded by NNNN**.

## Format

Copy this into `docs/adr/NNNN-short-name.md` with the next free number:

```markdown
# NNNN. Title in the active voice

- **Status:** Proposed | Accepted | Superseded by NNNN
- **Date:** YYYY-MM-DD

## Context

What forces us to decide, and what we know.

## Decision

What we chose, in one or two paragraphs.

## Consequences

What gets easier, what gets harder, and what we must now do.
```

## Index

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-iceberg-datafusion-arrow.md) | Build on Apache Iceberg, DataFusion and Arrow | Accepted |
| [0002](0002-pin-iceberg-rust-from-git.md) | Pin unreleased iceberg-rust from git | Accepted |
| [0003](0003-layers-as-namespaces.md) | Medallion layers are Iceberg namespaces | Accepted |
| [0004](0004-stage-then-atomic-commit.md) | Tasks stage files; only the job commits | Accepted |
| [0005](0005-apache-2-with-dco.md) | Apache 2.0 with a DCO, not a CLA | Accepted |
