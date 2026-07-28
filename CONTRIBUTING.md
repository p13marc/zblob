# Contributing to zblob

Thanks for your interest! A few ground rules keep the crate healthy:

## Development

```bash
cargo test                    # default features
cargo test --all-features     # zstd + encryption + fanout + tracing
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all
```

All four commands must pass — CI enforces them (plus MSRV 1.97, docs with
`-D warnings`, cargo-audit, and a publish dry-run). Keep `Cargo.lock`
committed.

## Rules of the road

- **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`, `test:`,
  `ci:`); breaking changes marked with `!` and explained in the body.
- **Wire discipline:** any change to a serialized struct's shape must bump
  `wire::WIRE_VERSION` — postcard is positional, so silent shape changes are
  data corruption. Add explicit tests for the new shape.
- **Every public item is documented.** Module docs carry design invariants —
  read them before changing behavior, update them when you do.
- **Tests, not sleeps.** Integration tests use the in-process loopback session
  helpers in `tests/common/`; servers are ready when `spawn().await` returns.
  New defect fixes come with a test that fails without the fix.
- **Untrusted input stays bounded.** Anything decoded from the network needs
  validation and allocation caps; add a mini-fuzz case in `tests/minifuzz.rs`
  (and a `fuzz/` target if it's a new parser).

## Security

See [SECURITY.md](SECURITY.md) — please report vulnerabilities privately.
