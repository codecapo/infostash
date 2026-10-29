# Releasing

How a version goes from `main` to users. Modeled on Apache DataFusion and
Apache Iceberg.

## Branches

- `main` is the only long-lived branch. It must always build and pass CI.
- Work happens on short branches named `<type>/<topic>`, for example
  `feat/csv-connector` or `fix/quarantine-schema`, and merges through a pull
  request.
- Pull requests are **squash merged**. The pull request title becomes the one
  commit on `main`, so it must follow
  [Conventional Commits](https://www.conventionalcommits.org) (CI checks it).
- A release branch named `<major>.<minor>.x` (for example `0.1.x`) is cut from
  `main` only when that version is released. Patch fixes land on `main` first,
  then are cherry-picked to the release branch.

## Cutting a release

1. Open an issue named "Release 0.N.0" and list what must be in it.
2. On `main`, move the `Unreleased` items in `CHANGELOG.md` under the new
   version and date, and bump `version` in the root `Cargo.toml`. Merge that
   as `chore: release 0.N.0`.
3. Create the branch and the signed tag:

   ```sh
   git switch -c 0.N.x main
   git tag -s v0.N.0 -m "Infostash 0.N.0"
   git push origin 0.N.x v0.N.0
   ```

4. Publish the GitHub release with the changelog section as its notes.
   Binaries, the container image and crates follow once the release
   workflow exists (see the launch checklist). That workflow will also
   publish an SBOM and signed build provenance, and run `cargo semver-checks`
   against the previous release.

## Patch releases

1. Merge the fix to `main` as usual.
2. Cherry-pick it onto `0.N.x` with `git cherry-pick -x`, through a pull
   request against that branch.
3. Update the changelog on the branch, tag `v0.N.1`, push, and publish.

## Versions

Semantic Versioning. Before 1.0, minor versions may break things; the
release notes say what changed and how to migrate.
