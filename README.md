# data-in-motion

> Working name. The CLI is called `dim` for now.

An open-source data platform written in Rust. Load data, clean it, and turn it
into business-ready tables with plain SQL, then ask questions of it in plain
English. It runs on AWS, GCP, Azure or your own servers, stores everything in
open formats in your own storage, and shuts compute off when nobody is using it.

**Status: pre-alpha.** We are building the first working slice. Nothing here is
ready for production. Star or watch the repo to follow along, and see
[ROADMAP.md](ROADMAP.md) for what comes next.

## What it does

- **Medallion pipelines:** data moves through three layers. **Raw** is the data
  as it arrived, **Clean** is typed, deduplicated and checked, and **Ready**
  holds the tables reports use. Each layer is a folder of SQL files.
- **Incremental by default:** each run processes only the rows that changed.
- **Bad rows don't break runs:** rows that fail checks go to a quarantine table.
- **Open tables:** everything is stored as Apache Iceberg on object storage, so
  Spark, Trino, Snowflake and Databricks can read the same tables.
- **Scales to zero:** compute runs only while there is work to do.
- **Agents:** a chat agent answers questions from defined business metrics, and
  a builder agent drafts pipelines on a branch for a person to approve.
- **Open connectors:** write a source in YAML, Rust or any language via Wasm.

## Quick start (planned for the first release)

```sh
dim init my-project
cd my-project
dim run
dim query "select * from gold.daily_revenue"
```

## Build from source

You need [rustup](https://rustup.rs). The exact Rust version is pinned in
`rust-toolchain.toml`, and rustup installs it automatically.

```sh
git clone https://github.com/data-in-motion/data-in-motion
cd data-in-motion
cargo build
cargo test
```

## Get involved

- Read [CONTRIBUTING.md](CONTRIBUTING.md) to set up and send your first change.
- Read [GOVERNANCE.md](GOVERNANCE.md) for how decisions are made and what stays open source.
- Read [docs/architecture.md](docs/architecture.md) for how the system fits together.
- Report security issues privately, as described in [SECURITY.md](SECURITY.md).
- Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

[Apache License 2.0](LICENSE).
