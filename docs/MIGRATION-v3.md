# Migrating to zblob 0.3 (wire v3)

For zensight, tcgui and zenkey, all of which pin `zblob = "0.2.0"`.

**v2 and v3 peers do not interoperate.** Every `ENC_*` tag is re-spelled and
`WIRE_VERSION` is 3, so a mixed deployment fails closed rather than
half-decoding. Plan the rollout as a cut, not a rolling upgrade.

**Nothing goes cold.** Chunk addresses are unchanged — still BLAKE3 of the
uncompressed bytes, still the same `<prefix>/<algo>/<hex>` key, still the same
container framing. Existing `DirStore`s and router-hosted storages stay warm
across the upgrade. This is the opposite of the sha256→blake3 cut, which
orphaned every cached chunk.

---

## 1. Prefixes are typed (every constructor changes)

`String` prefixes became `ServePrefix` (concrete only) and `QueryPrefix`
(single-segment wildcards allowed, `**` refused). This is the largest source
of diff, and it is mechanical.

```rust
// before
let server = BlobServer::new(session.clone(), prefix.clone());
let client = BlobClient::new(session, prefix);

// after
let serve = zblob::ServePrefix::new(prefix)?;      // fails on a wildcard
let query = zblob::QueryPrefix::from(&serve);      // free
let server = BlobServer::new(session.clone(), serve);
let client = BlobClient::new(session, query);
```

Affected: `BlobServer::{new,builder}`, `BlobClient::{new,builder}`,
`TreeServer::{new,builder}`, `TreeClient::{new,builder}`, `StoreClient`,
`publish_*`, and the `fanout` entry points.

**Delete the string hygiene this replaces.** In zensight:

- `artifact_fetch.rs`'s `blob_prefix.contains('*')` refusal — a `ServePrefix`
  cannot be built from a wildcard, and `QueryPrefix::is_concrete()` answers the
  question where one needs asking (uploads).
- the same check in `view/specialized/netring.rs`.
- the source-grepping guard test in `zensight-common/src/keyexpr.rs`, which was
  written after a real defect and is now enforced by the type system.

## 2. Stop hand-decoding `zblob::wire`

`zenkey-fleet/src/blob/bus.rs` decodes `Availability` and `Manifest` through
`wire::decode` against the `ENC_*` constants, and recomputes
`chunk_count = total_len.div_ceil(chunk_size)`. zensight recomputes the same
expression in `app.rs`. All of it has a typed equivalent now:

```rust
// one entry per holder, each naming the origin that answered
let holders: Vec<BlobProbe> = client.probe(id).await?;
for h in &holders {
    h.origin;                     // a QueryPrefix — fetch from this holder
    h.manifest.chunk_count()?;    // the div_ceil, once, here
    h.availability.as_ref();      // None if it answered `manifest` but not `have`
}
```

`bus.rs`'s `not_probed` arm for tier-2 targets can go too — see §4.

## 3. Tier 2 is readable from outside the crate

`bus.rs:261-270` refuses `store/…` and `tree/…` targets because there was no
public single-chunk fetch and the container framing was `pub(crate)`. Both are
fixed:

```rust
let store = StoreClient::new(session, store_prefix);
let bytes = store.fetch_chunk(&hash).await?;              // verified against the address
let bytes = store.fetch_chunk_sized(&hash, len).await?;   // when an index gave the length
let many  = store.fetch_many(&refs).await?;               // one query, many chunks

// and for a caller holding a container it fetched by hand:
let content = zblob::unframe_chunk(&container)?;
```

Inspecting a snapshot no longer needs a `ContentStore`:

```rust
let index = tree_client.fetch_index_by_root(&root).await?;  // pinned by construction
index.file_count(); index.total_size(); index.needed_chunk_refs();
```

That resolves zenkey#111's "is tree fetch worth an on-disk store?" — for
*inspection*, no store at all.

## 4. Tier 2 has a probe

`probe-then-fetch` is now total across all three key families:

```rust
let holders = store_client.probe(&hashes).await?;    // one bit per address asked
let snaps   = tree_client.probe_snapshot(id).await?; // have_index + chunks_present/total
```

Both replies are functions of the *question*, never of the objects, so fanning
them across origins is legitimate — which is what makes them a possession
verdict rather than a capability claim.

## 5. Signature changes to fix at the call site

| before | after |
|---|---|
| `TreeServer::register(index)` | returns `Result` (it may shard a large index into the store) |
| `publish_snapshot(.., compression, settle)` | `(.., compression, SettleCoverage, settle)` — use `SettleCoverage::All` if the producer is about to exit |
| `publish_store` inside `publish_snapshot` | now `publish_snapshot_chunks`; `publish_store` still exists but publishes the *whole store* |
| `BlobError::HashMismatch` | split into `ChunkLengthMismatch` and `CorruptStore` |
| `StoreKey(bytes)` | `StoreKey::new(bytes)`; no longer `Clone` (it zeroizes on drop) |
| `SnapshotTags::get` → `TreeIndex` | → `TagRecord { root, chunks }` |
| `build_tree` | unchanged; `build_tree_from(.., parent)` is the incremental form |

## 6. Behaviour changes worth knowing about

- **`Overwrite::Refuse` now refuses before transferring.** A refused download
  no longer leaves a finished `.part` behind, because no bytes were fetched.
  (It still keeps one in the TOCTOU case, where the destination appeared
  mid-transfer.)
- **Registering an id twice with different content is an error.** Identical
  content stays idempotent. Use `unregister` then register to replace.
- **A registered file that changes on disk is diagnosed**, instead of making
  every slice fail the client's verification forever.
- **Materialization refuses to replace a directory** and **masks
  setuid/setgid/sticky**, unless `MaterializePolicy` opts in. zensight's
  artifact extraction should review whether it wants either.
- **The default chunk size is 256 KiB**, down from 512 KiB — measured, see
  `DEFAULT_CHUNK_SIZE`'s docs. Existing manifests pin their own value, so only
  newly registered blobs change.
- **`TransferStats` gained `queries`.** Watch it to confirm batching is being
  answered rather than silently falling back to per-chunk fetches.

## 7. What zensight should adopt beyond the mechanical port

- **`DirStore` on sensors.** `ArtifactChannel` uses `MemoryStore`, which caps a
  sensor at one live snapshot (hence the `store.clear()` before building the
  next) and loses every chunk on restart. `examples/durable_store.rs` is the
  shape to copy: several snapshots, one store, tags, sweeping.
- **`build_tree_from`.** A sensor re-snapshotting a mostly-static directory
  currently re-hashes all of it. Read the mtime caveat in its docs first.
- **`download_staged`.** Replaces the staging dance in `app.rs` and
  `artifact_fetch.rs` — same convention, one call.
- **`TempTags` on downloads.** If anything sweeps a store that a download is
  writing into, pass the registry via `TreeClientBuilder::temp_tags`.

## 8. RFC side (zenkey)

Wire v3 touches RFC 07 §§2.2–2.5. The amendments are **v1.17** — the set was
already at v1.16, so the "v1.9" in `marcpardo/zenkey#141` and its children is
stale. What actually changed:

- **Reserve `batch` and `have` as tier-2 endpoint tokens** under `<algo>`, and
  `have` under a tree key. Unambiguous against `<hex>`, but they should be
  named the way tier 1's six are.
- **§2.5 gains the tier-2 probe**, making probe-then-fetch total across tiers.
  RFC 08's probe-prefix type extends to tier 2.
- **§2.3 gains the index descriptor** — a *large* index is served as
  content-addressed chunks; identity rules are unchanged.
- **§2.2's table should separate request keys from reply keys.** A slice is
  requested on `<prefix>/<id>/**` and *replied* on `<prefix>/<id>/slice/<i>`;
  the batch endpoint is requested on `…/batch` and replied on each chunk's own
  key, which requires `accept_replies(Any)`.
- **fanout stays in the table or leaves it** — still zero adoption, and every
  registry entry excludes it. Its framing is now correct either way.
- **No negative-reply message.** `marcpardo/zblob#53` proposed one so that an
  unknown id would not cost a query timeout. Measured: it costs about a
  millisecond. See that issue for the numbers.
