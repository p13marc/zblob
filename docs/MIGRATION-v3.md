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

## 5. Transfers are call builders

Every transfer entry point now returns a builder that runs when awaited,
matching `zenoh::Session::get`. The two things a transfer cannot do without
stay positional; progress, cancellation, overwrite policy and striping move
onto the builder — so the `&()` and `&CancelToken::new()` that appeared at
nearly every call site simply disappear.

```rust,ignore
// before
client.download_to(&req, &dest, &sink, &cancel).await?;
client.download_to(&req, &dest, &(), &CancelToken::new()).await?;
client.download_striped(&req, &dest, &holders, &sink, &cancel).await?;
client.upload_file(spec, path, Some(token), &sink, &cancel).await?;
tree.download_tree(&req, &dest, &store, &sink, &cancel).await?;

// after
client.download_to(&req, &dest).progress(&sink).cancel(&cancel).await?;
client.download_to(&req, &dest).await?;
client.download_to(&req, &dest).striped(&holders).progress(&sink).cancel(&cancel).await?;
client.upload_file(spec, path).token(token).progress(&sink).cancel(&cancel).await?;
tree.download_tree(&req, &dest, &store).progress(&sink).cancel(&cancel).await?;
```

The builders are `#[must_use]`: building one and forgetting to await it is
the shape's only new hazard, and it is a compile warning.

`Overwrite` is now settable per transfer (`.overwrite(policy)`) as well as
per client — whether replacing an existing file is acceptable is a property
of the transfer, not of the connection.

## 6. Publishing goes through `Publisher`

The five `publish_*` free functions are gone. A publisher is configured once
and then asked to publish things; a bare `Publisher` handles chunks, and
`.snapshots(tree_prefix)` upgrades it to a `SnapshotPublisher` that also
handles indices.

```rust,ignore
// before
zblob::publish_snapshot(
    &session, &store_prefix, &tree_prefix, &index, &store,
    ChunkCompression::default(), SettleCoverage::All, settle,
).await?;

// after
Publisher::new(&session, store_prefix)
    .snapshots(tree_prefix)
    .coverage(SettleCoverage::All)
    .settle(settle)
    .publish(&index, &store)
    .await?;
```

`publish_chunk` → `Publisher::chunk`, `publish_snapshot_chunks` →
`SnapshotPublisher::chunks_for`, `publish_store` → `Publisher::store`,
`publish_index` → `SnapshotPublisher::index`.

## 7. Three fields became types

All three are **wire-transparent** — postcard sees the same bytes — so this is
a source break only, and `WIRE_VERSION` does not move for it.

| before | after | why |
|---|---|---|
| `Manifest.id: String`, `TreeIndex.id: String` | `BlobId` | the id goes verbatim into a key expression, and the rules were enforced by a validator somebody had to remember to call |
| `TreeIndex.algo: String`, `IndexDescriptor.algo: String` | `HashAlgo` | compared against a constant in two validators; eight key builders took an `algo: &str` every caller passed the same value to |
| `Manifest.ext: Vec<(u16, Vec<u8>)>` | `Ext` | nothing bounded it, on a field that arrives off the network |

Constructing one is `BlobId::new(s)?` / `"x".parse()?`; reading one is
`as_str()`, `Deref<str>`, or `==` against a `&str` directly. A `HashMap`
keyed by `BlobId` can be looked up by `&str`.

`Ext`'s accessors move onto the type: `wire::ext_u32(&ext, id)` becomes
`ext.get_u32(id)`, and there are `set_u32`/`set_u64`/`set` to build one.

## 8. Key builders moved to `zblob::keys`

The seventeen key builders and parsers left the crate root:
`zblob::manifest_key` → `zblob::keys::manifest_key`, and so on for
`slice_key`, `slice_selector`, `availability_key`, `push_*_key`, `store_key`,
`store_batch_key`, `store_have_key`, `tree_key`, `tree_have_key`,
`parse_id`, `parse_ranges`, `parse_tier2_tail`, `format_ranges`,
`MAX_RANGE_SPANS`, `STORE_BATCH`, `STORE_HAVE`.

Two change shape: `parse_id` borrows (`Option<&str>`, not `Option<String>`),
and `parse_tier2_tail` returns `Option<Tier2Tail>` rather than
`Option<Vec<&str>>`. The `<algo>` parameter of `store_key` and friends is now
`HashAlgo` rather than `&str`.

`frame_chunk`/`unframe_chunk` stay at the root — they are not keys.

## 9. `BlobError` splits, and classifies

`Protocol(String)` carried 56 of the crate's error sites; matching on it, or
on its message text, was the only way to tell a traversal attempt from a
misconfigured prefix. Five variants now say which — `UnsafePath`,
`InvalidPrefix`, `MalformedMessage`, `Usage`, `NotSettled` — plus `Task` for
a panicked background task, which used to be stringified into `Protocol`.

Prefer the classifiers over matching variants:

```rust,ignore
if err.is_retriable() { /* transport; try again */ }
if err.is_cancelled() { /* the caller's own decision */ }
match err.kind() {
    ErrorKind::Integrity | ErrorKind::Protocol => { /* a peer's fault */ }
    ErrorKind::Usage => { /* ours */ }
    _ => {}
}
```

`Zenoh` and `Encode` now carry their cause rather than a string, so
`std::error::Error::source()` reaches it.

## 10. Signature changes to fix at the call site

| before | after |
|---|---|
| `TreeServer::register(index)` | returns `Result` (it may shard a large index into the store) |
| `publish_snapshot(..)` | `Publisher` — see §6 |
| `Arc<zenoh::Session>` in all five constructors | `&zenoh::Session` (`Session` is already an `Arc` inside, so the old shape was an `Arc<Arc<..>>`) |
| `ContentStore::has -> bool`, `get -> Option<Vec<u8>>` | both return `io::Result`, and `for_each_hash` is required; `has_many`/`get_many`/`put_many` default to looping |
| `accept_push(policy, spool_dir)` + three `push_*` builder methods | `accept_push(PushConfig::new(policy, dir).max_concurrent(n))` — the old knobs silently did nothing unless called *after* `accept_push` |
| `ENC_*: &str` | `WireTag` — compare with `ENC_SLICE.matches(sample.encoding())`, produce with `.encoding(&ENC_SLICE)` |
| `BlobError::HashMismatch` | split into `ChunkLengthMismatch` and `CorruptStore` |
| `StoreKey(bytes)` | `StoreKey::new(bytes)`; no longer `Clone` (it zeroizes on drop) |
| `SnapshotTags::get` → `TreeIndex` | → `TagRecord { root, chunks }` |
| `build_tree` | unchanged; `build_tree_from(.., parent)` is the incremental form |

## 11. Behaviour changes worth knowing about

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
- **`TransferStats` gained `queries`**, and it now counts on the ordinary
  single-origin path too (it reported 0 there in the first 0.3 build). Watch
  it to confirm batching is being answered rather than silently falling back
  to per-chunk fetches. `TransferStats` is also summable now, for reporting a
  multi-call transfer.
- **`cancel()` is observed while waiting on the network**, not between
  replies. It used to be checked only *after* a blocking receive, so its
  observed latency against a peer that stops answering was the query timeout
  — 5.00 s of a 5 s budget, measured, versus 0.17 s now. A UI that cancels a
  stalled transfer will feel different.
- **A `fanout` receiver gives up after `stall_timeout` without *progress***,
  rather than without traffic. A publisher streaming frames the receiver
  rejects used to reset the timer forever.
- **`TreeIndex::validate` no longer checks the id or the algorithm** — those
  cannot be wrong by the time it runs, since neither type decodes from
  anything this crate could not use.

## 12. What zensight should adopt beyond the mechanical port

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
- **`TreeClient::fetch_file`.** Pulling one path out of a snapshot no longer
  requires downloading the tree to a scratch directory and reading one file
  out of it. Chunks verify identically and go through the same store, so this
  shares a cache with a later full download.
- **`progress_channel`.** Both GUIs wrote the same adapter — progress arrives
  on a synchronous `emit` inside the transfer and has to reach a widget on
  another task. The crate now ships it, and it drops events rather than
  blocking, so a slow repaint cannot stall a download.
- **`upload_source`.** The push counterpart of `register_source`: anything
  implementing `BlobSource` (an in-memory buffer, a generated report) uploads
  without being staged in a file first. Same builder as `upload_file`:

  ```rust,ignore
  client
      .upload_source(spec, Arc::new(MemoryBlobSource::new(bytes)))
      .token(token)
      .await?;
  ```
- **Server introspection.** `registered()`, `manifest(id)`, `serves(id)` on
  both servers, so a caller no longer keeps a shadow copy of the registry.
- **`TreeIndex` navigation.** `entry`/`entries`/`files`/`file_chunks`, for
  showing or diffing a snapshot without matching `Entry`'s five variants.

## 13. RFC side (zenkey)

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
