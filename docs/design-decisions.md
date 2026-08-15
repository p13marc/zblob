# Design decisions and non-goals

The questions that were settled while building `zblob`, and the alternatives
that were considered and rejected on evidence. It exists so the next reader
(or the next you) does not re-open a closed question, and so the rationale that
lived only in the review notes is not lost. Where a claim rests on a
measurement or a test, the pointer is given.

## Things that look like bugs and are not

Each of these was proposed as a fix, investigated, and rejected because the
"bug" is the correct behaviour. Do not re-litigate without new measurements.

- **An unknown id does not cost a query timeout.** ~1 ms, measured (see
  `an_unknown_id_fails_fast_not_on_the_timeout`). A Zenoh query finalizes when
  its matching queryables complete, and completing *without replying* is
  immediate. Silence is how a server says "not mine", and it is what lets
  several servers share one prefix — no negative-reply message is needed or
  wanted.
- **Tier-1 availability is all-or-nothing, by construction.** A bao slice
  carries the sibling hashes proving it against the root, and computing those
  requires the whole blob — so a partial holder can serve no verified slice at
  all. Advertising an in-flight push's resume bitfield as "availability" would
  be a lie. Partial possession is real on **tier 2**, and that is what
  `StoreClient::probe` reports. (See [integrity-model.md](integrity-model.md).)
- **The default transfer chunk size is measured, not chosen.** See
  `DEFAULT_CHUNK_SIZE`'s doc table and `tests/chunk_size.rs`. Zenoh fragments
  anything over 64 KiB and a dropped fragment discards the whole message, so
  the chunk size trades verification/overhead against re-fetch cost on lossy
  links — the value is the outcome of that measurement, not a round number.

## Measured, not assumed

**A `TreeIndex` costs ~76–113 bytes per chunk — 0.05–0.10% of payload.** This
number decided the index-as-blob design.

| tree | payload | chunks | index |
|---|---|---|---|
| 10 × 1 MiB files | 10 MiB | 69 | 5.1 KiB |
| 200 × 256 KiB files | 50 MiB | 402 | 33 KiB |
| 2000 × 64 KiB files | 125 MiB | 1169 | 129 KiB |

Two consequences. First, metadata dedup would save ~0.1% per snapshot, not a
tier of overhead — not worth a layer. Second, the 64 MiB single-message
ceiling is not reached until ~670k chunks ≈ **40 GiB of payload** at default
parameters, far outside the target fleet. What *does* bite is that Zenoh
fragments anything over 64 KiB and drops the whole message on a lost fragment,
so a 130 KiB index is 2 fragments and a 1.6 MiB index (~1 GiB snapshot) is 26 —
re-fetched in full on every loss. That argues for making a *large* index
resumable, **not** for putting every index behind a descriptor (which would add
a round trip to every fetch to fix a problem the common case does not have).
Hence: small trees are served whole; only an index past the threshold is
sharded and served as an `IndexDescriptor`, distinguished by its `ENC_*` tag.
See [wire-protocol.md](wire-protocol.md).

## The half-applied-rule defect (recorded so it stays fixed)

Filter on the encoding tag *before* decoding is a crate-wide rule — a foreign
sample must be rejected for what it is, not by failing deep inside a transfer
(the "opaque error mid-transfer" mode v2 removed). The fanout **receiver** once
applied it in its manifest phase but not its slice phase, decoding any payload
that parsed positionally; a co-publisher on the fanout key could inject frames
past the manifest. The bao proof still protected the bytes, but the rule was
half-applied. Fixed 2026-08-15; pinned by
`tests/hostile_fanout.rs::phase_b_ignores_frames_phase_a_would_reject`, whose
control shows the same frames succeed when correctly tagged. The lesson: a
"filter before decode" that is stated once and applied in one of two phases is
worse than none, because it reads as covered.

## Explicitly rejected alternatives

So the next reader does not re-open these:

| Idea | Verdict |
|---|---|
| QUIC-style interleaved bao byte-stream (iroh's transport) | No — Zenoh's unit is the query reply; per-chunk bao slices give the same property without a streaming state machine. |
| Generic merkle-DAG / IPLD layer | No — two node types (index, chunk) suffice; iroh's own retreat from IPLD is the field evidence. |
| casync-style boundary-less archive stream | No — it forbids fetching one file out of a tree. |
| Content-defined chunking on the tier-1 artifact path | No — a rolling-hash pass on a sensor, for a blob sent once. |
| SuperCDC / UltraCDC / VectorCDC | No — no maintained Rust crates, single-digit-% gains, and chunking is not the bottleneck. |
| Convergent encryption | No — the fleet shares keys anyway, so its one benefit is moot while its confirmation-attack leaks remain. |
| RaptorQ / erasure coding | No — we have request/response feedback; retrying a content-addressed chunk is strictly simpler. |
| `AdvancedPublisher` retransmission for bulk pull | No — it solves live-stream recovery, not idempotent bulk pull. |
| Sealed frames on fleet-reachable `store` keys | Not now — the keyspace RFC's ban stays. If a shared sealed store is ever wanted, the design is borg-style keyed chunk IDs (`blake3::keyed_hash(fleet_key, plaintext)` as the address) plus a per-blob content key wrapped in an encrypted manifest — a *new algo tag*, not a container tag. |

## A dependency pin that is really a correctness decision

`fastcdc` is pinned to `4.0.1`, not just the major. `4.0.0` replaced a rounded
`log2` with a floored one, which silently moved cut points for any
non-power-of-two `avg_size` — so a consumer resolving `4.0.0` would compute a
*different tree root for identical bytes*, the one thing a content-addressed
store must never do. `4.0.1` restores the `3.2.1` boundaries. It also
downgraded the size-bound `assert!`s to `debug_assert!`s, which is why
`CdcParams::validate` must be complete on its own rather than backstopped by
the crate.

CDC parameters are also **seedable** (`CdcParams::with_seed`) for a reason:
extracting per-user chunking parameters from a backup service is a demonstrated
attack (Alexeev et al., 2025), so an unseeded CDC over shared or sealed content
is a real leak, not a theoretical one.

## Prior art and influences

`zblob` is an independent implementation, not a fork; it borrows ideas from:

- **[iroh-blobs](https://github.com/n0-computer/iroh-blobs)** — BLAKE3/bao
  verified streaming, range-set (`ChunkRanges`) requests, `HashSeq`, tag-based
  GC, and the choice of postcard. Its
  [retreat from IPLD](https://n0.computer/blog/a-new-direction-for-iroh/) is the
  evidence behind keeping the node model to two types.
- **[FastCDC](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)**
  (USENIX ATC'16) — the content-defined chunking, via the
  [`fastcdc`](https://github.com/nlfiedler/fastcdc-rs) crate; see the pin note
  above.
- **[casync](https://0pointer.net/blog/casync-a-tool-for-distributing-file-system-images.html)**
  / **[desync](https://github.com/folbricht/desync)** — content-addressed
  trees and seeding.
- **[restic](https://restic.readthedocs.io/en/latest/100_references.html)** —
  the per-chunk compression container and seeded chunking; and
  **[borg](https://borgbackup.readthedocs.io/en/stable/internals/security.html)**
  — keyed chunk IDs (the escape hatch above).
- **[BitTorrent v2](https://www.bittorrent.org/beps/bep_0052.html)** —
  per-file merkle trees over 16 KiB blocks (the same verification granularity
  `zblob` uses).
- **Zenoh 1.8/1.9** — reply-QoS inheritance
  ([Kiyohime](https://zenoh.io/blog/2026-03-18-zenoh-kiyohime/)) and the
  storage-manager plugin, which the automatic-backpressure and router-storage
  designs depend on.

For the mechanisms these decisions shaped, see
[architecture.md](architecture.md), [integrity-model.md](integrity-model.md),
and [wire-protocol.md](wire-protocol.md).
