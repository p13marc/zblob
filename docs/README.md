# zblob documentation

`zblob` is generic, resumable, chunked blob and directory transfer over
[Zenoh](https://zenoh.io), with BLAKE3 verified streaming, range-set resume,
and content-addressed dedup. Start with the [README](../README.md) for the
elevator pitch and runnable examples; this directory is the deeper material.

## Start here

- **[architecture.md](architecture.md)** — the map: the three transfer tiers,
  the primitives they share, and the invariants the design relies on. Read this
  first.

## Design papers

The *why* behind the parts that look surprising from outside:

- **[integrity-model.md](integrity-model.md)** — BLAKE3 + bao verified
  streaming: why every reply is independently verifiable, out of order, before
  it touches disk, and what that forces to be true (a partial holder can serve
  no verified tier-1 slice).
- **[wire-protocol.md](wire-protocol.md)** — the keyspace grammar, the
  positional postcard wire and `WIRE_VERSION`, and the wire-v3 traps that each
  look like an oversight until you know why they are load-bearing.
- **[design-decisions.md](design-decisions.md)** — settled questions that are
  *not* bugs, the alternatives that were considered and rejected on evidence,
  the measured defaults, and the prior art the design borrows from.

## Guides

- **[router-storage.md](router-storage.md)** — run a Zenoh router as the
  fleet-wide Tier-2 chunk store: serverless transfers, fleet-wide dedup,
  survival across producer restarts.
- **[MIGRATION-v3.md](MIGRATION-v3.md)** — porting a consumer from wire v2 to
  wire v3 (every snippet is compiled by `tests/migration_guide.rs`).

## Project

- **[graduation.md](graduation.md)** — how `zblob` graduated from the ZenSight
  monorepo.
- **[../CHANGELOG.md](../CHANGELOG.md)** — release history, including the full
  v1 → v2 → v3 migration notes.
- **[../examples/](../examples/)** — runnable end-to-end blob and tree
  transfers (CI compiles and runs them).
