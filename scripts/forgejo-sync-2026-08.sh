#!/usr/bin/env bash
#
# Sync the zblob Forgejo backlog with the 2026-08-12 review of
# docs/analysis-2026-08.md.
#
#   * retitles the two epics into the merged 0.3.0 = wire v3 line
#   * corrects #41 (RFC v1.9 -> v1.17), #42 (fetch_index is already public)
#     and #48 (the batch reply-key shape does not work as filed)
#   * creates twelve issues for the twenty defects the report missed
#
# Reads need no auth; writes need a token with scope `write:issue` from
# https://git.marcpardo.eu/user/settings/applications
#
# Usage:
#   ./scripts/forgejo-sync-2026-08.sh                 # dry run: print every action
#   FORGEJO_TOKEN=xxx ./scripts/forgejo-sync-2026-08.sh --apply
#
# Idempotent: an issue whose exact title already exists is skipped, and the
# PATCHes are the same every run. Safe to re-run.

set -euo pipefail

API="${FORGEJO_API:-http://10.10.0.30:3000/api/v1}"
REPO="${FORGEJO_REPO:-marcpardo/zblob}"
APPLY=0
[[ "${1:-}" == "--apply" ]] && APPLY=1

L_BUG=246
L_SECURITY=461
L_ENHANCEMENT=249
L_PERFORMANCE=462

if (( APPLY )); then
  : "${FORGEJO_TOKEN:?set FORGEJO_TOKEN (scope write:issue) to apply}"
  AUTH=(-H "Authorization: token ${FORGEJO_TOKEN}")
else
  AUTH=()
  echo "### DRY RUN — nothing will be written. Re-run with --apply to write. ###"
  echo
fi

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

existing_titles=$(curl -sf --max-time 30 \
  "${API}/repos/${REPO}/issues?state=all&type=issues&limit=200" | jq -r '.[].title')

api() { # api METHOD PATH JSON
  local method=$1 path=$2 body=$3
  if (( APPLY )); then
    curl -sf --max-time 30 -X "$method" "${AUTH[@]}" \
      -H 'Content-Type: application/json' -d "$body" "${API}${path}" \
      | jq -r '"  -> #\(.number) \(.title)"'
  else
    echo "  ${method} ${path}"
    echo "$body" | jq -r '"    title: \(.title // "(unchanged)")"'
  fi
}

retitle() { # retitle NUMBER TITLE
  echo "retitle #$1"
  api PATCH "/repos/${REPO}/issues/$1" "$(jq -n --arg t "$2" '{title:$t}')"
}

set_body() { # set_body NUMBER  (body on stdin)
  local n=$1 body
  body=$(cat)
  echo "update body of #$n"
  api PATCH "/repos/${REPO}/issues/${n}" "$(jq -n --arg b "$body" '{body:$b}')"
}

comment() { # comment NUMBER  (text on stdin)
  local n=$1 body
  body=$(cat)
  echo "comment on #$n"
  api POST "/repos/${REPO}/issues/${n}/comments" "$(jq -n --arg b "$body" '{body:$b}')"
}

create() { # create TITLE LABEL_ID...  (body on stdin)
  local title=$1; shift
  local body labels
  body=$(cat)
  labels=$(printf '%s\n' "$@" | jq -sc 'map(tonumber)')
  if grep -qxF "$title" <<<"$existing_titles"; then
    echo "skip (already exists): ${title}"
    return
  fi
  echo "create: ${title}"
  api POST "/repos/${REPO}/issues" \
    "$(jq -n --arg t "$title" --arg b "$body" --argjson l "$labels" \
        '{title:$t, body:$b, labels:$l}')"
}

# ---------------------------------------------------------------- epics ----

retitle 40 'Epic: 0.3.0 = wire v3 — the public read surface (merged into #41)'
retitle 41 'Epic: 0.3.0 — wire v3: batched, probeable, fleet-scale, and safe against a hostile index'

comment 40 <<'EOF'
**Merged into #41 — this epic no longer ships on its own.**

The 0.3/0.4 split assumed the API-additive half should ship first so the wire-v3
design would get field feedback from three working tier-2 consumers. All three
consumers belong to the same author, so what the split actually buys is two
consumer migrations instead of one. Everything below now lands in the single
breaking **0.3.0 = wire v3** tracked by #41.

The children keep their scope, with two corrections found in review:

- **#42 overstates its own gap.** `TreeClient::fetch_index(&self, id: &str)` is
  *already public* (`tree.rs:848`), and since `keyed_by_root()` sets `id` to the
  hex root, root-keyed fetch works today. What is genuinely missing is a
  `Hash`-typed variant that pins by construction, plus the `StoreClient`.
- The `cargo semver-checks` acceptance box does not apply any more — 0.3.0 is
  deliberately breaking. The gate is still worth adding to
  `publish-crates.yml` for the releases after it.
EOF

# ------------------------------------------------------- corrected issues ----

comment 41 <<'EOF'
**Three corrections from the 2026-08-12 review, before implementation starts.**

**1. The RFC amendments are v1.17, not v1.9.** The zenkey RFC set was already at
**v1.16** (2026-08-12) when the source report was written; v1.9 shipped
2026-08-08. `marcpardo/zenkey#141` carries the same stale number in its title
and needs retitling. The amendment decomposition (zenkey #142–#147) is correct.

**2. #48's batch reply-key shape does not work as filed.** See the comment on
#48 — `…/<algo>/batch` and `…/<algo>/<hex>` are disjoint key expressions and
Zenoh rejects the reply server-side. The fix is `accept_replies(Any)`, and the
wildcard fallback the issue floats must be rejected outright.

**3. This epic now also carries the security work.** The review found twenty
defects the source report missed, four of them security defects in the
*published* crate, three of which sit on the tier-2 materialization path
zensight uses in production. They are filed separately and listed under
"Children (security and hardening)" below. They are not optional extras: an
0.3.0 that ships the wire work and leaves a hostile index able to `rm -rf` a
subtree of the destination would be a worse release than 0.2.
EOF

comment 42 <<'EOF'
**Correction: `fetch_index` is already public.**

`TreeClient::fetch_index(&self, id: &str) -> Result<TreeIndex>` exists at
`tree.rs:848` and already does the full job — `validate()`, id match, optional
root pinning, skip-bad-replies. And because `keyed_by_root()` (`tree.rs:258`)
sets `id` to the hex root, `fetch_index(&root.to_string())` is root-keyed fetch
today. The "already exists internally as the index half of `download_tree`;
exposing it" framing is wrong.

What is actually missing, and what this issue should deliver:

1. **`fetch_index_by_root(&self, root: &Hash)`** — a `Hash`-typed entry point
   that pins by construction rather than by the caller remembering to pass
   `expected_root`. The stringly form stays for non-content-addressed ids
   (zensight registers snapshots by id today).
2. **`StoreClient`** — unchanged from the issue text, and still the right call:
   a caller holding a bare `store/<algo>/<hash>` address should not have to
   construct a `TreeClient` with a dummy tree prefix, and #48's batch fetch and
   #49's probe both need a home keyed to the third key family.

The acceptance criteria are otherwise unchanged.
EOF

comment 48 <<'EOF'
**The reply-key shape in this issue does not work. Here is what does.**

The issue proposes GET on `<store_prefix>/<algo>/batch` with replies keyed
`<store_prefix>/<algo>/<hex>`, and flags the reply-key question as something to
"verify early". Verified: **those two key expressions are disjoint**, and Zenoh
enforces intersection on the *server*, not the client —

```rust
// zenoh-1.9.0/zenoh/src/api/queryable.rs:553
if !self._accepts_any_replies() && !self.key_expr().intersects(&sample.key_expr) {
    bail!("Attempted to reply on `{}`, which does not intersect with query `{}`, \
           despite query only allowing replies on matching key expressions", ...)
}
```

So under the default `ReplyKeyExpr::MatchingQuery` every batch reply fails on
the serving origin, loudly, once per hash.

**The fix: `.accept_replies(ReplyKeyExpr::Any)` on the GET.** Stable since
Zenoh **1.8.0** (it was `unstable` before), carried as the `_anyke` selector
parameter, and readable server-side via `Query::accepts_replies()` — so the
server can refuse to batch-reply to a client that did not ask for it. The
client then validates each reply key is under its own store prefix, which is
nearly free because it verifies the content hash anyway.

**The wildcard fallback this issue floats must be rejected outright, not
"decided before writing the RFC amendment".** A GET on
`<store_prefix>/<algo>/**` would make every router-hosted Zenoh storage **dump
its entire content store** in a single query — and `docs/router-storage.md`
makes storages a first-class tier of this crate. Replying under
`…/batch/<hex>` is also wrong: it destroys the single-chunk cacheability that
is the stated reason for using the ordinary store key.

**Two consequences the issue does not state:**

- **A router storage never answers `batch` at all.** It serves by key, and no
  key `…/<algo>/batch` exists in it, so it stays silent — which is safe, but it
  means the client **must fall back to per-chunk GETs for every hash the batch
  round left unanswered**. That fallback is not a nicety; it is what keeps the
  serverless publish → producer exits → fetch path working. Add it to the
  acceptance criteria.
- **Do not use a declared `Querier` yet.** The issue is right that the
  chunk-fetch loop is exactly a Querier's shape, and wrong that it is a 1.9
  feature — it landed in 1.1.0 and stabilized in 1.5.0. But the
  *drop-with-pending-query* deadlock fix
  ([eclipse-zenoh#2635](https://github.com/eclipse-zenoh/zenoh/pull/2635)) is on
  `main` and **not in 1.9.0**. Use a plain `session.get()` with
  `accept_replies(Any)` and revisit when the fix ships.

Finally, one deliberate divergence worth recording: iroh's equivalent
`GetMany` **aborts as soon as the provider lacks required data**. This issue's
"a holder answers only the hashes it has; silence for the rest" is the better
rule for a bus with many partial holders. Keep it, and say why.
EOF

comment 53 <<'EOF'
**Closing: the premise is false. Measured.**

This issue rests on `serve_one`'s comment — `// unknown id → client times out
→ NotFound.` — and on the same comment in `serve_index_query`. Both are wrong,
and the negative reply designed to work around them is not needed.

A Zenoh query finalizes once **every matching queryable has completed**, and a
queryable that drops the `Query` without replying completes *immediately*. So
silence does not cost the timeout. Measured against a 30-second
`query_timeout`:

| shape | elapsed |
|---|---|
| a server is listening on the prefix, but does not own the id | ~1.0 ms |
| nothing is listening on the prefix at all | ~0.4 ms |
| a wildcard-origin fan-out across two servers, neither holding it | ~1.4 ms |
| the same fan-out through `probe()` | ~1.3 ms |

That is the whole justification for `ENC_NACK` gone. And the feature was not
free: the issue itself notes it would need the rule "a nack is authoritative
only when no positive reply arrives", precisely because several servers may
share a prefix — a subtlety introduced to solve a problem that does not exist.

What was actually wrong here is the two comments, which are now corrected, and
the absence of a test pinning the behaviour. Both are fixed: the measurement
above is `an_unknown_id_fails_fast_not_on_the_timeout` in `tests/coverage.rs`,
so anyone who believes the old story again has something to run. `BlobServer`'s
docs now state it too, since silence-means-not-mine is what makes sharing a
prefix work at all.

No RFC amendment is needed for tier 1's endpoints as a result.
EOF

# ----------------------------------------------------------- new issues ----

create 'Materialization is destructive and mode-unsafe: a hostile index can delete subtrees and set setuid' \
  "$L_BUG" "$L_SECURITY" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S1+S2).
Both are on the tier-2 materialization path zensight uses today.

## S1 — `remove_existing` recursively deletes pre-existing directories

```rust
// tree.rs:1233-1241
fn remove_existing(p: &Path) {
    if let Ok(meta) = std::fs::symlink_metadata(p) {
        if meta.is_dir() {
            let _ = std::fs::remove_dir_all(p);      // <—
        } else {
            let _ = std::fs::remove_file(p);
        }
    }
}
```

It runs for every `File` (`tree.rs:1148`), `Hardlink` (`:1190`) and `Symlink`
(`:1207`) entry. So an index containing a single `Entry::File { path:
"Documents", .. }` **recursively deletes `<dest_root>/Documents`** before
writing a file there.

`download_tree`'s docs describe materialization as in place and non-atomic
(`tree.rs:928-935`) — "an error mid-materialization leaves a mix of old and new
entries". They do not say it deletes. Neither does `SECURITY.md`. A consumer
reading those docs would reasonably point `dest_root` at an existing directory
tree, which is exactly the rsync/casync model being invoked.

`TreeIndex::validate()` (`tree.rs:270-320`) also does not reject **duplicate
entry paths**, so entries within one index can destroy each other's output.

**Fix.** Refuse to replace a directory with a non-directory by default —
`BlobError::Protocol("refusing to replace directory … with a file")` — and put
the destructive behaviour behind an explicit opt-in on the download request,
the way `Overwrite` already gates the tier-1 destination. Reject duplicate
paths in `validate()`. State the resulting semantics in the `download_tree`
docs and in `SECURITY.md`.

## S2 — index-supplied mode bits are applied raw

```rust
// tree.rs:1244-1249
fn set_mode(path: &Path, mode: u32) {
    if mode != 0 {
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode));
    }
}
```

`mode` comes straight off the wire and is applied to files (`tree.rs:1165`) and
directories (`:1225`) with no mask. An index with attacker-chosen file content
and `mode = 0o104755`, extracted by a privileged process, produces a
**setuid-root binary**. `validate()` never inspects `mode`.

tar and rsync both gate setuid/setgid restoration behind an explicit flag, for
this reason.

**Fix.** Mask to `0o0777` by default (dropping setuid/setgid/sticky), with an
explicit opt-in for callers that genuinely restore privileged trees. Document
the mask where the mtime-restoration promise is documented.

## Acceptance

- [ ] An index whose entry path names an existing destination directory is
      refused, not silently `rm -rf`'d — and the refusal is testable.
- [ ] An index with duplicate entry paths fails `validate()`.
- [ ] A `0o104755` file entry materializes without the setuid bit by default.
- [ ] Tests land in `tests/tree_security.rs` at layer 3, each asserting its own
      discriminating power (the honest control passes, the hostile case fails).
- [ ] `SECURITY.md` and the `download_tree` docs state the destructive-write
      and mode-restoration semantics.
EOF

create 'Symlink confinement is lexical and a symlink chain within one index defeats it' \
  "$L_BUG" "$L_SECURITY" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S3).

`sanitize_symlink_target` (`paths.rs:40-69`) resolves a target **lexically**,
counting every `Normal` component as +1 depth and every `ParentDir` as −1,
refusing the target if depth ever goes negative.

That is sound for one link in isolation. It is not sound when an intermediate
component is *itself a symlink created by the same index*, because the kernel
resolves that component before continuing.

## Proof

```text
Entry::Symlink { path: "sub/link", target: ".."                        }
Entry::Symlink { path: "e",        target: "sub/link/../../etc/passwd" }
```

For the second entry the depth walk is `sub`(1) `link`(2) `..`(1) `..`(0)
`etc`(1) `passwd`(2) — never negative, so it passes. But `sub/link` resolves to
the tree root, so `sub/link/..` is the root's *parent*, and `e` points at
`<root>/../etc/passwd`.

Both links are created in pass 3 (`tree.rs:1196-1210`) in index order, so the
chain exists as soon as materialization finishes.

Nothing is written *through* the escaping link during extraction — symlinks are
materialized last and nothing writes after them, which is a real defence and it
holds. But the promise in `tree.rs:25-26` ("symlinks materialize last with
confined targets") and in `SECURITY.md:18-19` is not kept for the *resulting
tree*: whatever reads or writes that tree afterwards follows the link out.

`tests/tree_security.rs:156-193` covers only the single-level case, which is
why this survived.

## Fix

Resolve targets against the set of symlinks the index itself declares, not
lexically in isolation: build the map of declared links, then resolve each
target iteratively with substitution and a bounded iteration count (refusing a
cycle), asserting containment at every step. A post-materialization
`canonicalize`-and-check pass is a good backstop but cannot stand alone —
a dangling link has nothing to canonicalize.

## Acceptance

- [ ] The two-entry chain above is refused by `validate()`.
- [ ] A legitimate multi-level relative symlink that stays inside the root is
      still accepted (the discriminating-power control).
- [ ] A symlink cycle terminates with an error rather than looping.
- [ ] The test lives in `tests/tree_security.rs` next to the single-level case
      it generalizes.
EOF

create 'Encrypted DirStore: XChaCha20-Poly1305 nonce reuse when a chunk is re-packed' \
  "$L_BUG" "$L_SECURITY" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S4).
Requires the `encryption` feature, which **no consumer compiles** — so the
practical exposure today is nil. It is still a total break of the construction
and must not survive into 0.3.0.

## The defect

```rust
// crypt.rs:53-57
fn nonce_for(key: &StoreKey, hash: &Hash) -> [u8; 24] {
    let nk = blake3::derive_key(NONCE_CONTEXT, &key.0);
    let full = blake3::keyed_hash(&nk, hash.as_bytes());
    full.as_bytes()[..24].try_into().expect("24 bytes")
}
```

The nonce is a function of `(key, chunk hash)` **only**. The sealed plaintext is
not the chunk — it is the *container* (`store.rs:256-262`: `pack()` first, then
`seal()`), and the container depends on the store's `ChunkCompression`.

`DirStore::put` has no "already present, skip" guard: it re-packs, re-seals and
`persist()`s over the existing file unconditionally. So:

1. `put(h, bytes)` on a store with `ChunkCompression::None` → seals `0x00‖bytes`
2. reopen the same store `.with_compression(Zstd { level: 3 })`, `put(h, bytes)`
   → seals `0x01‖len‖frame` **under the same (key, nonce)**

Two different plaintexts under one XChaCha20 keystream: XOR of the plaintexts is
recoverable, and the Poly1305 one-time key is reused, which permits forging an
authenticator for any sealed chunk. Changing only the zstd *level* is enough.

The deterministic nonce was a deliberate choice — it makes re-putting an
identical chunk byte-identical, which the module docs call out as desirable for
idempotent stores. The choice is fine; deriving it from the wrong input is not.

## Fix

Bind the nonce to a digest of the container being sealed, not to the chunk
address alone:

```rust
fn nonce_for(key: &StoreKey, hash: &Hash, container: &[u8]) -> [u8; 24]
```

This keeps every property that motivated determinism — re-sealing an *identical*
container yields an identical file, so idempotent re-puts stay idempotent — and
gives distinct containers distinct nonces. Reading is unaffected: the nonce is
already stored in the frame (`crypt.rs:71-75`) and `open` reads it rather than
recomputing (`crypt.rs:84-93`), so existing sealed stores keep opening.

While in this file:

- `StoreKey` is `#[derive(Clone)] pub struct StoreKey(pub [u8; 32])` with no
  scrubbing (`crypt.rs:36-37`). Add `zeroize` with `ZeroizeOnDrop`, drop
  `Clone`, and make the field private with an explicit constructor. Note that
  `ZeroizeOnDrop` is unsound on a `Copy` type — do not add `Copy`.
- The *derived* subkeys (`crypt.rs:49-51,54-57`) are unscrubbed stack arrays on
  every `seal`/`open`. Wrap them in `Zeroizing`.

## Acceptance

- [ ] A test seals one chunk under two different `ChunkCompression` settings
      and asserts the two frames carry **different** nonces.
- [ ] A test asserts re-putting a byte-identical container still produces a
      byte-identical file (the idempotence property the design wants).
- [ ] The test lives in `tests/store_contract.rs`, which already runs against
      every `ContentStore` configuration — this is exactly the case that suite
      exists to catch.
- [ ] `StoreKey` no longer implements `Clone` and zeroizes on drop.
EOF

create 'publish_* can silently drop what it publishes, and publishes the wrong set' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S5+S6).
Both defeat the one promise the serverless tier makes: *PUT, confirm, exit*.

## S5 — the PUTs are droppable

`publish_chunk` (`publish.rs:48-55`) and `publish_index` (`:86-91`) are plain
`session.put(...)` with no congestion control. Publications default to
`CongestionControl::Drop` —

```rust
// zenoh-protocol-1.9.0/src/core/mod.rs:632
pub const DEFAULT: Self = Self::Drop;
```

— which is precisely why the fanout tier sets `Block` explicitly, with a comment
saying so (`fanout.rs:148-152`). The crate's own documented fact 1
(`lib.rs:36-44`) states the rule and `publish.rs` is the one place that does not
follow it.

So a `publish_store` of a large snapshot into a router storage can shed chunks
under backpressure. And the read-back settle phase does not catch it: it probes
the index plus a **deterministic ~7-key sample** (`publish.rs:113-126`), so a
snapshot missing thousands of chunks in between settles green and
`publish_snapshot` returns `Ok`. The producer then exits, per the documented
workflow, and the loss is discovered by a consumer much later as an
unresolvable `NotFound`.

Second, smaller problem in the same phase: `probe_key` (`publish.rs:141-155`)
accepts *any* responder. If a `TreeServer` is running on the same prefix — the
normal case while developing — it answers the probe and "the storage settled"
is a lie.

## S6 — `publish_store` publishes the whole store

```rust
// publish.rs:60-74
for hash in store.hashes()? { ... publish_chunk(...) ... }
```

`publish_snapshot` therefore pushes **every chunk in the local store**,
including chunks belonging to other snapshots and other tenants, into what is
typically a *shared* router storage. It should iterate
`index.needed_chunks()`.

## Fix

- `.congestion_control(CongestionControl::Block)` on both PUTs.
- `publish_store` takes the index (or a hash set) and publishes only that set;
  keep a `publish_store_all` if publishing a whole store is genuinely wanted,
  but it must not be what `publish_snapshot` calls.
- Widen the settle sample, or make its coverage a documented parameter rather
  than a hard-coded 7 — "we verified 7 of 10,000 keys" should not read as
  "the snapshot is available".
- Document that a co-located server makes the settle probe meaningless.

## Acceptance

- [ ] `tests/storage.rs` gains a case where the publish path is congested and
      asserts either full delivery or a loud failure — never a green `Ok` with
      chunks missing. This is the regression that matters.
- [ ] A test asserts `publish_snapshot` does not publish a chunk that is in the
      store but not in the index.
EOF

create 'Unbounded remote-driven allocation: chunk replies, tree totals, query queue, availability bits' \
  "$L_BUG" "$L_SECURITY" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7,
S7+S8+S14+S15). Four places where a remote peer picks how much memory or work
this process commits. Tier 1 bounds its equivalents; tier 2 mostly does not.

## S7 — no size bound before unframing a chunk reply

`try_unpack`'s raw arm is unbounded:

```rust
// compress.rs:87-98
Some((&TAG_RAW, rest)) => Ok(rest.to_vec()),          // any length
Some((&TAG_ZSTD, rest)) => { ... if declared > MAX_UNPACKED { Corrupt } ... }
```

`MAX_UNPACKED` (16 MiB) guards **only** the zstd branch. `fetch_one_chunk`
(`tree.rs:1100`) copies `sample.payload().to_bytes()` and unframes it before the
hash check at `:1104`, and it never receives the expected length — even though
the caller has it in `ChunkRef::len`, and `TreeIndex::validate` has already
bounded it by `cdc.max` (`tree.rs:298-305`). With `fetch_concurrency` 16 this is
straightforward remote memory amplification.

Same shape on the tier-1 slice path (`client.rs:788`) and in fanout
(`fanout.rs:267`).

## S8 — tier 2 has no total-size cap

`TreeClientConfig` (`tree.rs:754-759`) bounds `max_index_bytes` and nothing
else. A 64 MiB index can carry ~1.6 M `ChunkRef`s, each up to `cdc.max`
(16 MiB, `chunk.rs:159`) — tens of TiB, fetched and `put` with no ceiling. Into
a `MemoryStore` that is direct RAM exhaustion. Tier 1 has had
`max_blob_size` since v2 (`client.rs:96`, default 1 TiB) for exactly this.

## S14 — the in-flight permit is acquired inside the spawned task

Both serve loops `tokio::spawn` per inbound query (`server.rs:466-472`,
`tree.rs:660-679`) and acquire the `max_inflight` permit *inside* the task
(`server.rs:497-500`, `tree.rs:687,714`). The semaphore bounds concurrent
*work*; it does not bound queued *tasks*, each of which holds its `Query` — and
a `push/slice` query holds a full chunk payload.

The permit is also taken before `PushPolicy` is consulted (`server.rs:648`), so
an unauthorized peer can occupy every permit by flooding push offers.

## S15 — `Availability` bitfields are unvalidated and unmasked

```rust
// wire.rs:73-75
pub fn count(&self) -> u32 { self.bits.iter().map(|b| b.count_ones()).sum() }
```

No masking of the padding bits in the final byte, so a bitfield can over-report
— `ResumeState::received()` (`resume.rs:108-121`) masks precisely because that
matters. And nothing validates `bits.len() == chunk_count.div_ceil(8)` on decode
(`client.rs:363-369`), so a responder can answer `chunk_count = 1` with
megabytes of `bits`; `fetch_availability` accumulates one per reply into an
unbounded `Vec`.

## Fix

- Pass the expected length into the chunk fetch and reject an over-long reply
  **before** unframing; enforce `MAX_UNPACKED` on the raw arm too.
- Add `max_tree_bytes` (and a chunk-count cap) to `TreeClientConfig`, checked
  against the index before any fetching starts.
- Acquire the permit in the serve *loop*, before spawning; consult `PushPolicy`
  before committing resources.
- Mask `Availability::count()`; validate `bits.len()` on decode; cap the
  responder list.

## Acceptance

- [ ] Property test: `try_unpack` never allocates more than the cap for any
      generated input, on both arms.
- [ ] `tests/hostile_peer.rs` gains an over-long-reply responder and asserts it
      is skipped, not fatal, and that the honest control still succeeds.
- [ ] An index whose declared total exceeds `max_tree_bytes` is refused before
      the first chunk GET.
- [ ] A property test asserts `Availability::count() <= chunk_count` for every
      generated bitfield.
EOF

create 'Tier 2 has no end-to-end content verification, and verify_on_read defaults off' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S10).

Tier 1's design premise is that every byte is verified against the bao root
*before it touches disk*, with no end-of-transfer hash pass needed. Tier 2 has
no analogue.

- Chunks are verified **on receipt** (`tree.rs:1104`, re-hash against the
  address) — good, and that part is sound.
- But `reconstruct_tree` reads them back out of the store and checks only the
  length:

  ```rust
  // tree.rs:1151-1158
  let bytes = store.get(&c.hash).ok_or(...)?;
  if bytes.len() as u32 != c.len { return Err(BlobError::HashMismatch); }
  ```

- `root_hash` covers the **entry list**, not chunk contents — `store.rs:132-135`
  says so explicitly.
- `DirStore::verify_on_read` defaults to **false** (`store.rs:107`).

So on the default configuration, local disk rot between fetch and
materialization — or a `ContentStore` implementation that quietly breaks its
`has`/`get` contract, and there is a third-party one in zensight's redb store —
materializes wrong bytes under a snapshot the caller believes was verified, with
no error anywhere.

Note this is also the *only* content check on the reconstruct path, and it
raises `BlobError::HashMismatch` for what is a length mismatch (see the error
naming issue).

## Options

1. **Verify on materialization**: re-hash each chunk in `reconstruct_tree`.
   Simple and total; costs one BLAKE3 pass over the tree (BLAKE3 is ~GB/s, so
   this is likely acceptable, but measure it).
2. **Default `verify_on_read` to true** for `DirStore` and document the cost.
   Cheaper for the resume case (chunks already present are verified once) but
   depends on the store implementation honouring it — which a third-party impl
   need not.
3. Both, with (1) as the guarantee and (2) as the healing mechanism.

Recommendation: **(1)**, because it is a property of the crate rather than of
whichever `ContentStore` the caller supplied — and the whole point of the
`ContentStore` contract discussion in `store.rs:205-233` is that implementers
get subtle things wrong.

## Acceptance

- [ ] Decision recorded here with its measurement before implementation.
- [ ] A test corrupts a chunk in the store between fetch and materialization and
      asserts the materialization fails rather than writing wrong bytes.
- [ ] The honest control (uncorrupted store) still materializes — discriminating
      power.
- [ ] Benchmark of the added verification pass on a realistic tree.
EOF

create 'A foreign reply_err aborts an upload the real server already accepted' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S9).

The crate's third documented invariant (`lib.rs:49-53`, `CLAUDE.md`):

> **Any peer can answer, so one bad reply must not be fatal.** Replies that fail
> decoding, validation, id matching, or root pinning are *skipped* rather than
> aborting the query, so a hostile or stale responder cannot deny a fetch that
> an honest replica still answers.

The push client does not honour it. Both the offer loop (`client.rs:449-453`)
and the per-slice loop (`client.rs:539-544`) turn **any** `reply_err` from
**any** responder into a returned `BlobError::PushDenied`.

This is not hypothetical. zensight's netring sensor deliberately runs a second
`BlobServer` on the same `@blob/artifact` prefix
(`zensight-sensor-netring/src/disk.rs:17-19`), relying on servers ignoring ids
they do not own. A server with push disabled answers an offer with
"push not enabled on this server" (`server.rs:663`) — and that error would abort
an upload the *other* server on the same prefix already accepted.

Related, from the same read:

- `client.rs:531` re-races every slice against the whole prefix; nothing binds
  subsequent slices to the server that accepted the offer. With multiple
  spooling servers, one server's wanted-set drives an upload while all of them
  hold spool state.
- `upload_file` validates its prefix with `validate_query_prefix`
  (`client.rs:401`), which permits wildcards — but an upload has exactly one
  destination, and push replies echo `query.key_expr()` verbatim
  (`server.rs:721,792,937`), which would be a wildcard expression.

## Fix

- Skip foreign `reply_err`s and keep the first one for diagnostics, exactly as
  every `fetch_*` loop does. Fail only when *no* responder accepted.
- Bind the slice phase to the origin that accepted the offer.
- Refuse wildcard prefixes for push outright (this is `ServePrefix` /
  `QueryPrefix` work — see the typed-prefix issue).

## Acceptance

- [ ] Two servers on one prefix, one with push disabled: the upload completes.
      This is the regression that would bite zensight.
- [ ] An upload against a prefix where *no* server accepts still fails, and
      fails with the first responder's diagnostic rather than a generic timeout.
- [ ] A wildcard push prefix does not typecheck.
EOF

create 'build_tree can produce snapshots that no client will ever accept' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S11).

`build_tree` (`tree.rs:336-356`) returns its `TreeIndex` **without calling
`TreeIndex::validate()`**, and `walk` records symlink targets verbatim from
`read_link` (`tree.rs:418-424`) with no `sanitize_symlink_target`.

Every client validates on receipt (`tree.rs:890`), and `validate()` refuses
absolute or escaping symlink targets (`tree.rs:307-310`). So a source directory
containing an ordinary absolute symlink — `/etc/localtime`, a build artifact
pointing at an installed path, anything — produces a snapshot that:

- builds without error,
- registers with a `TreeServer` without error,
- publishes into a router storage without error,
- and **is rejected by every consumer, forever**.

The producer has no way to learn this. The failure surfaces on the consumer as
a validation error about a tree it did not build.

This is precisely the failure shape the leading-`@` id rule documents at length
for tier 1 (`manifest.rs:96-105`): *"an id like `@thing` would register
successfully and then never be servable — every download would time out as
`NotFound` with nothing in any log to explain it. Reject it at the door
instead."* The same door is missing here.

## Fix

Call `validate()` at the end of `build_tree` and return its error. The cost is
one pass over the entry list, on a function that has just hashed the entire
tree.

Then decide the policy question it exposes, and document it: what *should*
happen to a source tree containing an absolute symlink? Refusing loudly is
consistent with the existing treatment of FIFOs, sockets and devices
(`tree.rs:462-468`) and with non-UTF-8 names — "refusing loudly beats silently
producing a snapshot that claims to be the tree but isn't". Skipping with a
diagnostic is the other defensible answer. Silently building an unusable
snapshot is not.

## Acceptance

- [ ] `build_tree` over a directory containing an absolute symlink fails at
      build time with a message naming the offending path.
- [ ] `build_tree`'s output always satisfies `validate()` — asserted as a
      property test over generated directory shapes, not a single example.
EOF

create 'Assorted correctness: truncating chunk count, missing TempTag, late Overwrite::Refuse, orphaned outboards' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7,
S12+S13+S16+S17). Independent, each small.

## S12 — `TransferChunks::count()` truncates instead of erroring

```rust
// chunk.rs:85-87
self.total_len.div_ceil(self.chunk_size as u64) as u32
```

With `chunk_size = MIN_CHUNK_SIZE` and a `total_len` that is a multiple of
`2^32 × 65536`, this returns **0**. `client.rs:707-712` root-checks only the
`total_len == 0` case, so a zero-count non-empty blob completes `fill_holes`
immediately and an all-zero file of the claimed length is renamed into place as
"verified".

Unreachable at the default `max_blob_size` (1 TiB → count ≤ 2^24) but reachable
through `BlobClientBuilder::max_blob_size`, and `upload_file` already calls
`manifest.validate(u64::MAX)` (`client.rs:425`). Make it a checked construction.

## S13 — `download_tree` never takes a `TempTag`

`gc.rs:126-153` provides `TempTags`/`TempTag` for exactly one purpose, stated in
the module docs: *"A sweep racing a download must not collect chunks the index
references but the store only half-has; take a temp tag on
`index.needed_chunks()` before fetching, drop it after."*

`TreeClient::download_tree` (`tree.rs:936-975`) never does. So a concurrent
`gc::sweep` deletes chunks a running download has already fetched, and the
download then fails at `tree.rs:1152-1153` with `NotFound`. The mechanism is
documented, implemented, tested, and has no caller.

`sweep` is also TOCTOU against `SnapshotTags::set` (live set snapshotted at
`gc.rs:176`, removals at `:184-189`) and against `TreeServer::register` unless
the caller remembers `extra_roots`. Worth documenting even if not fixed.

## S16 — `Overwrite::Refuse` is checked after the transfer

`client.rs:690` tests `try_exists(dest)` *after* `fill_holes` and `sync_data`.
Worse, the `.part` is preallocated to the remote-supplied `manifest.total_len`
first (`client.rs:652-653`), so a remote manifest sizes the disk for a transfer
that is then refused. It is also a TOCTOU window against the following
`rename`.

The fanout tier has the same bug at `fanout.rs:418-420` **and then deletes the
`.part`** (`:425-428`), which directly contradicts `Overwrite::Refuse`'s own
documentation (`client.rs:40-42`, `error.rs:80-81`: "keeping the finished
`.part` … nothing is lost").

## S17 — outboards and spool files are never reclaimed

`register_file` writes the outboard to `<source path>.obao4`
(`server.rs:342-348`, and `finalize_push` at `:966-972`) — in the *caller's*
directory, unasked, where the directory may be read-only or shared. `unregister`
(`server.rs:425-427`) removes only the registry entry. Push spool `<id>.blob`
files are likewise never reclaimed (documented at `server.rs:228`, but with no
quota knob beside `max_blob_size`/`max_concurrent`).

Make the outboard location configurable (defaulting to a temp dir, not the
source's), remove it on `unregister`, and give the spool a quota.

## Acceptance

- [ ] `TransferChunks::new` rejects a geometry whose chunk count would not fit
      in `u32`; property test over generated `(chunk_size, total_len)`.
- [ ] A sweep concurrent with a download does not break the download.
- [ ] `Overwrite::Refuse` against an existing destination fails **before** any
      bytes cross the wire, in both the client and fanout paths, and the
      fanout path stops deleting the `.part`.
- [ ] `unregister` reclaims the outboard it created.
EOF

create 'fanout: unbounded publisher cache and a 256 MiB unverified receive buffer' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S18).
Complements #54 (fanout's normative status) — that issue decides whether the
tier is promised; this one is true whichever way it goes.

## Publisher: the whole blob stays resident

```rust
// fanout.rs:157
.cache(CacheConfig::default().max_samples(count as usize + 1))
```

Every slice sample is retained for the `FanoutHandle`'s lifetime, so the
publisher holds O(blob size + outboard) in RAM for as long as the handle lives —
on the producer, which in this fleet is frequently an embedded sensor. The
cache exists so late joiners can replay history, which is the tier's whole
point, so the bound has to become a *policy* (a byte budget, a time window)
rather than "all of it".

## Receiver: 256 MiB of unverified frames from an unauthenticated publisher

`fanout.rs:88` sets `EARLY_SLICE_MAX_BYTES` to 256 MiB, and `fanout.rs:316-322`
buffers up to that (and `EARLY_SLICE_MAX_FRAMES` = 512) of slices that arrive
*before* the manifest — which is to say, before anything can be verified at all.
The manifest need never arrive. A quarter of a gigabyte is a very large bound
for a receiver-side defence against an unauthenticated publisher.

`fanout.rs:277` also hard-codes `m.validate(1 << 40)` rather than taking a
configurable cap the way `BlobClient` does.

## Also, from #54's list, and grouped here because it is the same code path

fanout is the only part of the crate that breaks the crate's own wire rules:
bare `(u16, FanoutFrame)` tuples instead of version-first structs
(`fanout.rs:166-172,190-197`), and **no `ENC_*` tag on samples**, so the
receiver relies on decode failure to reject foreign samples
(`fanout.rs:266-272`) — the "opaque decode error deep in a transfer" failure
mode v2 removed everywhere else. A wire bump is the moment to fix it.

## Acceptance

- [ ] The publisher cache is bounded by a documented policy, and the doc says
      what a late joiner loses when it evicts.
- [ ] `EARLY_SLICE_MAX_BYTES` is either justified in a comment against a stated
      threat model or lowered to something a receiver can afford.
- [ ] The manifest size cap is configurable.
- [ ] Samples carry an `ENC_FANOUT` tag and version-first structs.
EOF

create 'Pin fastcdc to 4.0.1 — 4.0.0 silently changes chunk boundaries' \
  "$L_BUG" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S19).
One line, but it is a content-addressing hazard.

`Cargo.toml:29` says `fastcdc = "4"`. Our own `Cargo.lock` pins 4.0.1 and CI
builds `--locked`, so *this* repo is fine — but a consumer resolving its own
lock can get **4.0.0**, and 4.0.0 is not equivalent to 4.0.1. From the
upstream changelog:

> The 4.0.0 cleanup replaced the private `logarithm2()` helper (rounded log2)
> with `usize::ilog2()` (floored log2), which silently changed cut points for
> any `avg_size` that is not a power of two. Power-of-two sizes were
> unaffected. Cut points now match 3.2.1 again.

Chunk boundaries determine chunk hashes, which determine `root_hash`. A
consumer that resolves 4.0.0 with a non-power-of-two `CdcParams::avg` therefore
computes **a different tree root for byte-identical content** — silently, and
in the one part of the system whose entire job is that identical bytes get
identical names. Dedup against every other holder simply stops working, with no
error.

Our defaults are powers of two, so the default path is unaffected. `CdcParams`
is caller-tunable, so the exposure is real for anyone who tunes it.

4.0.0 also downgraded chunk-size bounds checks from `assert!` to
`debug_assert!`, so invalid sizes no longer panic in release builds — meaning
`CdcParams::validate` must be trusted to be complete rather than backstopped.

## Fix

- `fastcdc = "4.0.1"` in `Cargo.toml`.
- A comment saying why the patch version is pinned, so nobody relaxes it.
- Re-check `CdcParams::validate` covers everything the crate used to `assert!`.

Worth tracking upstream: unreleased `master` adds `v2020::FastCDC::rechunk`
(reuse masks and gear tables across buffers — directly relevant to the tier-2
build loop) and a 7–14 % throughput win from array-typed GEAR lookups.
EOF

create 'Incremental build_tree: reuse a parent snapshot instead of re-chunking every file' \
  "$L_ENHANCEMENT" "$L_PERFORMANCE" <<'EOF'
Found by the 2026-08-12 review of 0.2 (`docs/analysis-2026-08.md` §7, S20).
The largest producer-side win available, and the mirror of a consumer-side
facility that already exists.

## The gap

`build_tree` (`tree.rs:336-356`, `walk` at `:394-471`) walks the whole directory
and runs every file through the content-defined chunker, every time. There is no
way to say "here is last snapshot's index; most of this tree has not changed".

The consumer side of exactly this problem is already solved: `seed.rs` grows the
local `have` set from prior copies before any network traffic, and the module
docs name the classic case — *"a previous version of the destination is the
classic seed"*. The producer has no counterpart.

For this fleet the producer is the expensive side: an embedded sensor
snapshotting a directory repeatedly, re-hashing gigabytes to discover that
almost nothing changed.

## The shape

restic's parent-snapshot trick: for each file, if `(path, size, mtime)` matches
the parent index's entry, reuse that entry's `ChunkRef`s verbatim instead of
re-reading and re-chunking the file.

```rust
pub fn build_tree_from(
    root: &Path,
    id: impl Into<String>,
    cdc: &CdcParams,
    store: &dyn ContentStore,
    parent: Option<&TreeIndex>,
) -> Result<TreeIndex>;
```

`build_tree` becomes `build_tree_from(root, id, cdc, store, None)`.

## Correctness constraints

- **The CDC parameters must match.** Reusing chunk refs cut with different
  parameters would silently produce an index whose chunks do not tile as the
  new parameters would cut them. Refuse a parent whose `cdc` differs — and note
  the parameters are recorded in the index precisely so this is checkable.
- **`mtime` is excluded from `root_hash` by design** (`tree.rs:144-146`), so it
  is *not* part of identity — but it is exactly the right heuristic for "has
  this file changed", which is what it is used for here. Say so in the doc,
  because the two facts look contradictory.
- **mtime-based change detection is a heuristic, not a proof** (same-second
  writes, clock changes, deliberate mtime forging). It is what restic, rsync
  and tar all use, and the reused refs are still content-addressed — so a wrong
  guess produces a snapshot that does not match the source, not corruption.
  Offer a `verify` mode that re-reads anyway, and document the trade honestly.
- The reused chunks must still be **in the store**, or the snapshot is
  unservable. Check presence when reusing, and fall back to re-chunking.

## Acceptance

- [ ] A snapshot of a tree where one file changed re-chunks one file, asserted
      by counting `store.put` calls — not by timing.
- [ ] A parent with mismatched `CdcParams` is refused.
- [ ] A reused entry whose chunks are missing from the store falls back to
      re-chunking rather than producing an unservable index.
- [ ] The resulting `root_hash` is **identical** to what a full `build_tree`
      would have produced for the same tree. This is the property that matters
      and it belongs in a property test over generated edits.
EOF

echo
echo "done."
(( APPLY )) || echo "(dry run — re-run with --apply and FORGEJO_TOKEN set)"
