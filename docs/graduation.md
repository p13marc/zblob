# Graduation history

This crate was incubated inside the [ZenSight](https://github.com/p13marc/zensight)
monorepo as the in-tree `zenoh-blob` workspace crate (epic zensight#193,
issue zensight#202). It was extracted with full history via
`git filter-repo --subdirectory-filter` and renamed **zblob** on graduation
(2026-07-18) — the `zenoh-` prefix was dropped to make clear this is a community
crate, not an Eclipse Zenoh deliverable.

Versioning restarted at `0.1.0` for the first public release; the monorepo-era
history retains ZenSight's workspace version numbers in its commit messages.

ZenSight consumes this crate as a git (later crates.io) dependency; its adapter
layer imports `zblob::…` by crate name only.
