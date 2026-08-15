#!/usr/bin/env bash
#
# Sync the zblob Forgejo backlog with the state of `main` after the 0.3.0
# work and its pre-release review (2026-08-12 .. 2026-08-13).
#
#   * retitles the two epics into the merged 0.3.0 = wire v3 line
#   * corrects #41 (RFC v1.9 -> v1.17), #42 (fetch_index is already public)
#     and #48 (the batch reply-key shape does not work as filed)
#   * files the twenty defects the report missed — **created closed**, because
#     they were found and fixed in the same cycle. They are filed anyway so the
#     tracker records that they existed and how they were resolved; three are
#     security defects, which is exactly what someone audits a tracker for.
#   * closes #39-#55, each with what shipped and where
#   * opens the work that is genuinely still outstanding
#
# Run it once, after the 0.3.0 branch is pushed. It is idempotent.
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

all_issues=$(curl -sf --max-time 30 \
  "${API}/repos/${REPO}/issues?state=all&type=issues&limit=200")
existing_titles=$(jq -r '.[].title' <<<"$all_issues")
# Numbers already closed, so a re-run does not re-comment on them.
closed_numbers=$(jq -r '.[] | select(.state == "closed") | .number' <<<"$all_issues")

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

# A defect found *and fixed* in the same cycle. Filing it open would be false
# work; not filing it at all loses the record that it ever existed — which for
# a security defect is the thing a tracker is for. So: file it, then close it.
create_closed() { # create_closed TITLE LABEL_ID...  (body on stdin)
  local title=$1; shift
  local body labels num
  body=$(cat)
  labels=$(printf '%s\n' "$@" | jq -sc 'map(tonumber)')
  if grep -qxF "$title" <<<"$existing_titles"; then
    echo "skip (already exists): ${title}"
    return
  fi
  echo "create+close: ${title}"
  if (( APPLY )); then
    num=$(curl -sf --max-time 30 -X POST "${AUTH[@]}" \
      -H 'Content-Type: application/json' \
      -d "$(jq -n --arg t "$title" --arg b "$body" --argjson l "$labels" \
             '{title:$t, body:$b, labels:$l}')" \
      "${API}/repos/${REPO}/issues" | jq -r '.number')
    echo "  -> #${num}"
    curl -sf --max-time 30 -X PATCH "${AUTH[@]}" \
      -H 'Content-Type: application/json' \
      -d '{"state":"closed"}' \
      "${API}/repos/${REPO}/issues/${num}" >/dev/null
    echo "  -> closed #${num}"
  else
    echo "  POST /repos/${REPO}/issues (then PATCH state=closed)"
  fi
}

close() { # close NUMBER  (closing note on stdin)
  local n=$1 body
  body=$(cat)
  if grep -qx "$n" <<<"$closed_numbers"; then
    echo "skip (already closed): #$n"
    return
  fi
  echo "close #$n"
  api POST "/repos/${REPO}/issues/${n}/comments" "$(jq -n --arg b "$body" '{body:$b}')"
  api PATCH "/repos/${REPO}/issues/${n}" '{"state":"closed"}'
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

create_closed 'Materialization is destructive and mode-unsafe: a hostile index can delete subtrees and set setuid' \
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

create_closed 'Symlink confinement is lexical and a symlink chain within one index defeats it' \
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

create_closed 'Encrypted DirStore: XChaCha20-Poly1305 nonce reuse when a chunk is re-packed' \
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

create_closed 'publish_* can silently drop what it publishes, and publishes the wrong set' \
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

create_closed 'Unbounded remote-driven allocation: chunk replies, tree totals, query queue, availability bits' \
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

create_closed 'Tier 2 has no end-to-end content verification, and verify_on_read defaults off' \
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

create_closed 'A foreign reply_err aborts an upload the real server already accepted' \
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

create_closed 'build_tree can produce snapshots that no client will ever accept' \
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

create_closed 'Assorted correctness: truncating chunk count, missing TempTag, late Overwrite::Refuse, orphaned outboards' \
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

create_closed 'fanout: unbounded publisher cache and a 256 MiB unverified receive buffer' \
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

create_closed 'Pin fastcdc to 4.0.1 — 4.0.0 silently changes chunk boundaries' \
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

create_closed 'Incremental build_tree: reuse a parent snapshot instead of re-chunking every file' \
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

# ------------------------------------------ decisions taken while building ----

comment 52 <<'EOF'
**Option A is not implementable as written. Decision: neither A nor B.**

The issue offers "make it real" (servers answer their actual bitfield, plus a
striping scheduler) or "delete it", and recommends A on the grounds that the
push spool is a genuine partial holder. It is a genuine partial *holding*. It
cannot be served.

A tier-1 reply is a **bao slice**: the chunk's bytes plus the sibling hashes
proving them against the root. Those siblings are hashes of *other* subtrees,
so producing one requires the whole blob — which is exactly why the outboard is
computed at registration and at `finalize_push`, and never from a partial
spool. A holder with part of a blob can serve no verified slice of it at all.
Advertising a partial tier-1 holding would send clients after chunks they can
never obtain.

This was found by implementing A: the partial holders reported availability,
the striping client dutifully assigned them ranges, and every range came back
empty.

**What shipped instead:**

- **The striping scheduler**, which was the actual prize. `download_striped`
  addresses each range to one holder's concrete prefix, so a chunk crosses the
  wire once instead of once per replica — Zenoh cannot cancel remote replies in
  flight, so the old "ask a shared key and discard duplicates" cost N× the
  bandwidth for N replicas. Endgame duplication for the tail; per-holder
  rejection accounting drops a persistently bad peer from the rotation. The
  test asserts the byte count, since a version that asked everyone and threw
  the duplicates away would pass anything weaker.
- **Tier-1 availability stays all-or-nothing**, now documented as a property of
  bao rather than left looking unimplemented, with a test pinning it — because
  "have the push spool report its bitfield" is the obvious improvement and it
  is the lie the constraint forbids.
- **Partial possession lives on tier 2**, where it is real: a `ContentStore`
  holds whatever subset it holds and each chunk is verified against its own
  address rather than a whole-object root. That is what `StoreClient::probe`
  reports (#49).

So the half-state the issue rightly objects to is gone, without inventing a
capability the integrity model cannot back.
EOF

comment 54 <<'EOF'
**Decision: keep the feature, fix the framing, leave the normative question to
the RFC.** (Option A, minus the part this repo cannot decide.)

Adoption is still zero and every registry entry still excludes it, so nothing
here argues for promotion. But the framing defects were real and a wire bump is
the moment to fix them, whatever the RFC later says:

- samples are **version-first structs** carrying **`ENC_FANOUT`**, and the
  receiver filters on the tag *before* decoding instead of relying on decode
  failure to reject foreign samples — the "opaque error deep in a transfer"
  mode v2 removed everywhere else, surviving in this one place;
- `FanoutFrame`'s postcard-positional variant ordering is now documented as
  append-only, since adding a variant anywhere else is a silent wire break no
  version field would catch;
- the publisher cached **every** slice for the handle's lifetime — the whole
  blob plus outboard resident, on producers that are frequently embedded. Now
  `FanoutConfig::cache_samples` (default 4096); a joiner arriving after
  eviction re-requests what it missed;
- the receiver buffered **256 MiB** of unverified pre-manifest frames from an
  unauthenticated publisher, for a manifest that need never arrive. Now
  `max_early_bytes` (default 16 MiB);
- the manifest size cap was hard-coded at 1 TiB, so a receiver could not
  decline an implausible one. Now `max_blob_size`.

Whether §2.2 keeps naming it is a zenkey decision (`marcpardo/zenkey#146`), and
the code is honest either way now. The README states the feature's status
rather than leaving it inferable only from a TOML comment downstream.
EOF

comment 55 <<'EOF'
**Decided by measurement: 256 KiB, as proposed.** The issue asked for a bench
rather than a guess; here it is.

`tests/chunk_size.rs` encodes every bao slice for 8 MiB of incompressible data
at each size — so the header overhead is measured, not modelled — and applies
the fragment-loss model on top:

```
  chunk   slices   header      wire bytes / useful byte
                             p=0      p=1%     p=5%
    64K      128    0.977%   1.0098   1.0200   1.0629
   128K       64    0.635%   1.0063   1.0268   1.1151
   256K       32    0.488%   1.0049   1.0461   1.2337   <- new default
   512K       16    0.427%   1.0043   1.0884   1.5138   <- old default
  1024K        8    0.403%   1.0040   1.1792   2.2812
  4096K        2    0.391%   1.0039   1.9100  26.7536
```

Two effects pull against each other: a bao slice carries parent hashes, so
smaller chunks put proportionally more overhead on the wire; but Zenoh
fragments over 64 KiB and a dropped fragment discards the whole message, so a
chunk survives with probability `(1-p)^ceil(S/64KiB)`.

256 KiB is the knee — the first effect has flattened (0.488% against a
best-possible 0.391%) and the second has not taken off. Moving there from
512 KiB costs **0.06% on a clean link** and saves **4 points at 1% loss and 28
points at 5%**. Going on to 128 KiB saves roughly half as much again while
doubling the slice count.

The reasoning is in `DEFAULT_CHUNK_SIZE`'s doc comment, so the number is not
folklore. The test asserts the *shape* of the result — overhead falls with
size, loss cost rises with size, the default is few enough fragments and near
enough the clean-link optimum — so it stays meaningful if the measurement moves
rather than pinning numbers that would just need updating.
EOF

# --------------------------------------------------- pre-release review ----

comment 41 <<'EOF'
## Pre-release review (2026-08-13)

0.3.0 was built, then reviewed before tagging — while every breaking change
was still free. The review was not a polish pass: it found **seven
behavioural defects** and **five tests that could not fail**, none of which
the existing suite could have caught.

### Defects found and fixed

| | What | How it was established |
|---|---|---|
| B1/B2 | Store I/O ran on the reactor in three public async paths — `publish_hashes` and `publish_store` read the store inline (a file read per chunk, and a full recursive `read_dir`), and `TreeServer::register` sharded an index on the async thread: ~64 fsynced atomic renames for a 4 MB index | read |
| B3 | `ContentStore::has -> bool` / `get -> Option<Vec<u8>>` spelled `EIO`, `EACCES` and "absent" identically. The client's response to absence is to re-fetch and `put` back into the same broken store, so a read-only disk became an undiagnosable loop | read |
| B4 | `cancel()` was *polled* after a blocking receive, so its observed latency was the query timeout, not "after the current chunk" as documented | **measured: 5.00 s of a 5 s budget, versus 0.17 s after** |
| B5 | Three `BlobServerBuilder` push knobs silently did nothing unless called after `accept_push` — their doc comments said "call after `accept_push`", which is documentation compensating for a type error | read |
| — | A hostile `fanout` publisher could hold a receiver open **forever** by streaming junk: `stall_timeout` bounded the wait for a *sample*, not for progress, and this is the one tier with no second responder to fall back on | **found by a test that hung instead of failing** |
| — | `TransferStats::queries` reported 0 for every ordinary single-origin download — it was incremented on the striped and tier-2 paths only | found by asserting a capped transfer took several rounds |
| — | `SettleCoverage::Sample(k)` could probe `k + 1` keys, one over its own documented bound | found by extracting the arithmetic so it could be tested at all |

### Tests that could not fail

`tests/striping.rs` asserted `stats.bytes_fetched <= data.len()` against a
counter that only increments on a newly marked chunk — true under *every*
implementation, including the one striping replaced. The counting subscriber
beside it was never read, and could not have worked: a Zenoh subscriber does
not observe query replies. **The headline claim of the feature was untested.**
It now uses counting fake servers per origin and fails against an unstriped
fetch with `16 requests for 8 chunks`.

Also: an `A || B` whose second arm held for every generated input; two index
entries sharing a `path`, so `validate` returned "duplicate entry path" before
ever reaching the symlink arm the test existed for; a discarded upload result
that let an *empty* spool satisfy "a partial spool does not advertise"; an
assertion about Zenoh's enum rather than about this crate; and a test that
reported green on any machine without `mkfifo`.

### API changes (all source-breaking, all free before tagging)

Transfers became `IntoFuture` call builders (`download_to(&req, &dest)
.progress(&sink).cancel(&tok).await`); `BlobId`/`HashAlgo`/`Ext` validate at
*decode* rather than relying on someone calling a validator;
`BlobError::Protocol(String)` — 56 of the crate's error sites — split into
five variants plus `kind()`/`is_retriable()`/`is_cancelled()`; `Publisher`
replaced the five `publish_*` functions (the widest took eight arguments);
the seventeen key builders moved to `zblob::keys`; sessions are
`&zenoh::Session` rather than `Arc<Arc<..>>`.

New capabilities, each added because a consumer was working around its
absence: `TreeClient::fetch_file` (one path out of a snapshot without
materializing the tree), server introspection, `TreeIndex` navigation, and
`progress_channel`.

### Numbers

- **241 tests**, up from 190.
- **89% line coverage**, up from 77%. `cargo-llvm-cov` had only ever run on
  the CI runner, so nobody had seen the number.
- All five fuzz targets re-run at 100 s each: **120M executions, no crashes,
  no slow units** (the DoS found in the previous cycle showed up as 3,177
  runs against millions).
- Every gate green on both feature sets: fmt, clippy, tests, docs,
  `publish --dry-run`, benches, MSRV 1.97, `cargo audit`.

Full detail in `CHANGELOG.md`; `docs/MIGRATION-v3.md` is now compiled by
`tests/migration_guide.rs`, so it cannot drift again.
EOF

# ------------------------------------------------------------- close-out ----

close 39 <<'EOF'
Shipped in 0.3.0 as **`StoreClient::fetch_chunk`** (plus `fetch_chunk_sized`,
`fetch_many` and `probe`). A caller holding a bare `<store>/<algo>/<hash>`
address can fetch and verify it with no tree and no `TreeClient`.

`tests/read_surface.rs::a_bare_content_address_can_be_fetched_and_verified`.
EOF

close 42 <<'EOF'
Shipped in 0.3.0. `TreeClient::fetch_index_by_root` inspects a snapshot with
no store, and `StoreClient` is the store-side reader.

Note the correction above: `fetch_index` was already public when this was
filed. What was actually missing was the *by-root* form and the store client.

The pre-release review added **`TreeClient::fetch_file`** on top — one path
out of a snapshot without materializing the tree, which is the capability a
snapshot most obviously implies and did not have.
EOF

close 43 <<'EOF'
Shipped in 0.3.0: `BlobClient::probe` returns one entry per holder, each
naming the origin that answered, and `Manifest::chunk_count` replaces the
`div_ceil` two consumers were rewriting.

The pre-release review went further in the same direction: server
introspection (`registered`/`manifest`/`index`/`serves`) and `TreeIndex`
navigation (`entry`/`entries`/`files`/`file_chunks`), so a consumer never
needs to match `Entry`'s five variants or keep a shadow copy of a registry.
EOF

close 44 <<'EOF'
Shipped in 0.3.0. `ServePrefix` (concrete) and `QueryPrefix` (single-segment
wildcards allowed, `**` refused); serving implies querying, so the conversion
is free one way and fallible the other. A server cannot be built on a
wildcard because there is no value to build one from.
EOF

close 45 <<'EOF'
Shipped in 0.3.0. `DirStore` is atomic, fsynced, fanned out
(`blake3/<xx>/<hex>`), with optional verify-on-read, `scrub()`, zstd at rest
and (feature) XChaCha20-Poly1305 sealing; `gc::sweep` does tag-based
mark-and-sweep with persistent snapshot tags and in-flight temp tags.
`examples/durable_store.rs` is the shape for a sensor to copy.
EOF

close 46 <<'EOF'
All ten shipped in 0.3.0 — see the `### Fixed` section of `CHANGELOG.md`.

The pre-release review then found **seven more** of the same kind (see the
review comment on #41), which is the honest lesson here: a hardening list
assembled by reading code finds what reading code finds. The three that
mattered most were only found by *measuring* (`cancel()`'s real latency),
by *writing an adversarial test* (the fanout hang), and by *extracting
untestable arithmetic* (`SettleCoverage::Sample`).
EOF

close 47 <<'EOF'
Shipped in 0.3.0 as `BlobClient::download_staged`, returning `Staged { path,
suggested, stats }` — staged under the **id**, with the server's advisory
filename kept aside rather than joined to any path.

Since the pre-release review it is a call builder like every other transfer:
`download_staged(&req, &dir).progress(&sink).await`.
EOF

close 48 <<'EOF'
Shipped in 0.3.0, with the reply-key correction above: replies land on each
chunk's own key, so the query sets `accept_replies(ReplyKeyExpr::Any)`.

The pre-release review added the adversarial coverage this endpoint had none
of — `tests/hostile_store.rs` drives every rejection branch in
`accept_batch_reply`, and pins two things that were documented and untested:
that the same query **without** `ReplyKeyExpr::Any` gets zero replies
(refused on the server), and that a holder with nothing at `…/batch` still
resolves through the per-chunk fallback. That fallback is not a corner case —
it is how every snapshot fetched from a router storage resolves.
EOF

close 49 <<'EOF'
Shipped in 0.3.0: `…/<algo>/have` answers one bit per address asked, and
`<tree>/<id>/have` answers four numbers whatever the snapshot's size. Both
reply with a size that is a function of the *question*, never of the objects
— which is the whole reason tier 2 may have a probe at all under RFC 07 §3.

Property tests for both validators were added in the pre-release review;
neither had any test of a rejection branch.
EOF

close 50 <<'EOF'
Shipped in 0.3.0 — but **conditionally**, not as filed. See the `[rev]` note
on `docs/analysis-2026-08.md` §4.3.

The dedup and size-ceiling arguments did not survive measurement: an index
costs 0.05–0.10% of its payload and the ceiling is ~40 GiB. Only the
resumability argument held. So a *large* index shards into an
`IndexDescriptor` and a small one is still served whole — a descriptor on
every fetch would add a round trip to fix a problem the common case does not
have.
EOF

close 51 <<'EOF'
Shipped in 0.3.0: one `WIRE_VERSION`, a trailing `ext` list on the metadata
messages, and servers advertising `max_chunks_per_query` so a client clamps
instead of being rejected with no way to discover why.

The pre-release review typed the pieces that were still strings: `ext` became
`Ext` with `MAX_FIELDS`/`MAX_VALUE_LEN` enforced at decode (nothing bounded
it before, on a field that arrives off the network), and the `ENC_*` `&str`
constants became `WireTag`, which removed a `String` allocation per reply.
EOF

close 52 <<'EOF'
Resolved: **availability stays, and stays all-or-nothing.** See the
correction above and the `[rev]` note on `docs/analysis-2026-08.md` §4.6.

The recommended option — have a holder answer its real bitfield — is not
implementable on tier 1, and this was established by implementing it and
watching every striped range come back empty. A bao slice carries sibling
hashes derived from the whole blob, so a partial holder can serve no verified
slice at all; advertising one would send clients after chunks they can never
obtain.

What shipped instead: `download_to(..).striped(&holders)`, so a chunk crosses
the wire once rather than once per replica, and partial possession is
reported on **tier 2**, where it is real. `tests/striping.rs` pins the
constraint so the "obvious improvement" is not re-proposed.
EOF

close 53 <<'EOF'
**Rejected: the premise is false.** See the correction above.

"NotFound costs a 30 s timeout" came from a code comment, not a measurement.
Measured before building: about a millisecond — with a server present, with
none present, and across a wildcard fan-out. A Zenoh query finalizes once its
matching queryables complete, and completing without replying is immediate.

`ENC_NACK`, an RFC amendment, and a subtle "authoritative only when no
positive reply arrives" rule were all avoided by one test
(`an_unknown_id_fails_fast_not_on_the_timeout`). Silence is how a server says
"not mine", and it is what lets several servers share one prefix.
EOF

close 54 <<'EOF'
Resolved: **fanout stays, and was brought up to the crate's own wire rules.**

Frames are version-first structs carrying an `ENC_FANOUT` tag, so a foreign
sample is rejected by its tag rather than by a decode failure deep in a
transfer; the publisher cache, receive buffer and manifest cap are bounded
and configurable.

The pre-release review then found that the tier had a **hang**: a hostile
publisher streaming frames a receiver rejects could hold it open
indefinitely, because `stall_timeout` bounded the wait for a *sample* rather
than for progress — and this is the one tier with no second responder to fall
back on. Fixed, and `tests/fanout.rs` now tests the "every receiver verifies"
claim that makes the tier safe to point at a fleet, which nothing did before.

Adoption is still zero. That is a reason to keep it correct and feature-gated,
not a reason to ship it broken.
EOF

close 55 <<'EOF'
Shipped in 0.3.0: `DEFAULT_CHUNK_SIZE` is 256 KiB, with the measurement in
its doc comment and `tests/chunk_size.rs` asserting the *shape* of the result
rather than pinning numbers that would just need updating.
EOF

close 40 <<'EOF'
Merged into #41 and shipped as one breaking 0.3.0. Keeping two releases in
flight bought nothing once wire v3 was going to break the wire anyway.

Every child is closed. See the pre-release review comment on #41 for what
changed after this epic's work was already complete.
EOF

close 41 <<'EOF'
0.3.0 is complete on `main`: 241 tests, 89% line coverage, every gate green
on both feature sets, all five fuzz targets clean at 100 s each.

**Not tagged and not published** — that is deliberate and is now the only
thing between here and the release. See the two issues opened alongside this
close-out for what remains.
EOF

# ------------------------------------------------------ remaining work ----

create 'Release gate: 0.3.0 cannot be published until the three consumers migrate' "${L_ENHANCEMENT}" <<'EOF'
0.3.0 is built, reviewed and green on `main`, and is **deliberately untagged**.
This issue tracks what has to happen before it can be.

## Why it is not just "publish it"

v2 and v3 peers **do not interoperate** — every `ENC_*` tag is re-spelled and
`WIRE_VERSION` is 3, so a mixed deployment fails closed. The rollout is a cut,
not a rolling upgrade. All three consumers pin `zblob = "0.2.0"`:

- `zensight` (`zensight-common`, artifact channel, netring)
- `tcgui` (`tcgui-shared`)
- `zenkey` (RFC 07 examples)

## What each needs

- [ ] **zensight** — port to the 0.3 API and cut over. `docs/MIGRATION-v3.md`
      is the guide, and every snippet in it is compiled by
      `tests/migration_guide.rs`, so it is accurate as of this release.
- [ ] **tcgui** — same, smaller surface.
- [ ] **zenkey** — RFC 07 §§2.2–2.5 amendments are **v1.17** (not the v1.9 the
      original epic said; the set was already at v1.16).
- [ ] Decide whether any consumer needs a feature that is currently compiled
      **nowhere in the fleet**: `zstd`, `tracing`, `fanout`, `encryption` are
      all off by default and all three consumers take default features.
- [ ] Tag `v0.3.0`, publish to crates.io, then bump the consumers' pins.

## Already done (2026-08-15 follow-up pass, not blocking the tag)

- [x] `upload_source` shipped (`525c04e`) — the last additive API gap.
- [x] Server + fanout-receiver adversarial suites shipped
      (`9c6d6b4`/`dcf1d65`/`0ecd46a`); coverage of the weak files lifted
      (fanout 77→92%, server 81→86%, publish 77→85%).
- [x] A sixth defect found + fixed on that pass (fanout phase-B tag filter,
      `2f67a49`).
- [x] Dependencies updated on `main` (`10d579c`): chacha20poly1305 0.11,
      criterion 0.8, blake3/thiserror patches, CI action pins. MSRV 1.97
      holds. Renovate PR #38 auto-closes once main carries these.

## Not blocking, but worth deciding with it

**Chunk addresses did not change.** Existing `DirStore`s and router-hosted
storages stay warm across the upgrade — the opposite of the sha256→blake3 cut,
which orphaned every cached chunk. So a storage does not need draining, and
the cut can be done per-peer as long as no peer talks v2 to a v3 peer.
EOF

create_closed 'upload_source: the push path can only send a file' "${L_ENHANCEMENT}" <<'EOF'
**Shipped 2026-08-15** (`main`, commit `525c04e`) — filed closed for the
record. `BlobClient::upload_source(spec, Arc<dyn BlobSource>)` now exists,
sharing the `Upload` builder with `upload_file` over a private `UploadSrc`
enum; source uploads emit no `Progress::Completed` (no final path) and fail
loudly if the source fingerprint changes between the hash pass and the send
pass. `DynReadAt` moved to `pub(crate)` so the client can reuse it. No wire
change; existing stores stay warm.

Original report follows.

---

The crate is symmetric everywhere except here.

| direction | from/to a file | from/to anything else |
|---|---|---|
| serve | `register_file` | `register_source(&dyn BlobSource)` |
| download | `download_to` | `download_to_writer` |
| **upload** | `upload_file` | **missing** |

`BlobClient::upload_file(spec, path)` takes a `PathBuf` and opens it with
`std::fs::File::open` on the blocking pool. A caller pushing something it
already holds — a generated report, a buffer, an artifact assembled in
memory — has to write it to a temporary file first, which for a large
artifact means paying the whole thing to disk for no reason, and on a
read-only or memory-backed root may be impossible.

## Shape

`upload_source(spec, Arc<dyn BlobSource>) -> Upload<'_>`, the same builder
`upload_file` returns. `BlobSource` already exists and is already what the
*server* registers from, so this is joining two things the crate has rather
than adding a concept:

```rust
client
    .upload_source(BlobSpec::new("report-01"), Arc::new(MemoryBlobSource::new(bytes)))
    .token(token)
    .await?;
```

`upload_file` becomes a thin wrapper over it with a `FileBlobSource`.

## Why it is not done yet

It was on the pre-release review's list and was cut for scope — it is
additive, so unlike everything else in that review it is *not* cheaper before
0.3.0 is tagged. Doing it after costs nothing.

## Watch out for

The push path re-opens the source per slice via `spawn_blocking`, and
`BlobSource::open` is documented as cheap and re-openable — which a
`MemoryBlobSource` satisfies (it clones an `Arc`) but an arbitrary
implementation might not. Either keep one reader for the whole upload or
document the requirement at `upload_source` too.
EOF

create_closed 'Coverage floor: the error-reply paths in server.rs and fanout.rs' "${L_ENHANCEMENT}" <<'EOF'
**Done 2026-08-15** (`main`, commits `9c6d6b4` + `dcf1d65` + `0ecd46a`) —
filed closed for the record. Two layer-3 adversarial suites now point the
fixed oracle at the *server* and the fanout *receiver* (raw `session.get()`s,
since the honest client discards error replies): `tests/hostile_server.rs`
(malformed selectors, over-cap ranges, lying push offers, tampered slices,
idle eviction, finalize root mismatch, a flipping push policy) and
`tests/hostile_fanout.rs` (malformed manifests skipped, deferred-variant on
stall, early-buffer caps, cancellation, TOCTOU). Plus misbehaving-storage
tests in `tests/storage.rs`. Line coverage of the three weakest files:
`fanout.rs` 77→92%, `server.rs` 81→86%, `publish.rs` 77→85%. Writing them
found a sixth defect (see the fanout phase-B filter issue). What remains
uncovered is genuine-fault I/O (spool renames, storage read failures) that
needs fault injection, not a hostile peer.

Original report follows.

---

`cargo llvm-cov --all-features` is **89.16% of lines** as of the 0.3.0
pre-release review (it was 77.3% before it). That number is a floor to hold,
not a target to game — the tests that moved it found seven real defects.

The two weakest files are both dominated by paths that only run when
something has already gone wrong:

| file | lines | what is uncovered |
|---|---|---|
| `server.rs` | 80.7% | `reply_err` paths, push eviction/cleanup, the in-flight semaphore's refusal branch |
| `fanout.rs` | 77.4% | publisher-side error handling, heartbeat miss detection, late-joiner replay edge cases |

`publish.rs` (77.2%) is third, and for the same reason: most of it needs a
live session plus a storage that behaves badly.

## Why this is worth doing rather than accepting

These are exactly the paths a *hostile or degraded* peer drives. The
pre-release review's experience is the argument: the fanout tier was at 75%
before it, and the uncovered part contained a hang a hostile publisher could
trigger — found only by writing the adversarial test.

## Approach

Layer 3, not layer 1 (see `CLAUDE.md`'s "Tests"): a hostile peer against the
*server* — malformed selectors, over-cap range sets, push offers that lie
about their size, slices for ids the server never accepted — with the same
oracle the two existing hostile suites use. `tests/hostile_peer.rs` and
`tests/hostile_store.rs` are the templates; both point at clients, and nothing
points at a server.

Not a blocker for 0.3.0.
EOF

create_closed 'Fanout phase B skipped the encoding-tag filter a co-publisher could bypass' \
  "$L_BUG" "$L_SECURITY" <<'EOF'
**Found and fixed 2026-08-15** (`main`, commit `2f67a49`) while writing the
fanout receiver adversarial suite — filed closed for the record. The sixth
0.3 defect, invisible from the scenario tests.

`receive_fanout` phase A filters every sample on the `ENC_FANOUT` encoding
tag *before* decoding — the comment above it even called this "the one place"
the rule had been missing. It was half true: **phase B** (the slice loop)
still decoded any payload that happened to parse (positionally, as
`(u16, FanoutFrame)`), with no tag filter. So a co-publisher on the fanout
key whose frames the front door would reject could inject them once the
manifest was through.

The bao proof still protected the *bytes* — a mistagged frame carrying wrong
data fails verification — so this is not silent corruption. But a foreign
sample must be rejected for what it *is*, not for failing deep inside a
transfer (the "opaque error mid-transfer" failure mode v2 removed
everywhere else). Fixed by applying the same `ENC_FANOUT.matches(...)` filter
and `FanoutMessage` decode shape in phase B as phase A.

Test-first: `tests/hostile_fanout.rs::phase_b_ignores_frames_phase_a_would_reject`
publishes half the slices correctly tagged and half under
`application/octet-stream`; on the old code the transfer completes (the bug),
on the fix it stalls `Incomplete` at exactly the honest half. The same split,
fully tagged, completes — so the failure is the filter, not the harness.

Two lesser things fixed on the same pass: an unreachable `DestinationExists`
early-return in the phase-B error path (removed; the real TOCTOU backstop is
after the block and already preserves the `.part`), and a `Publisher::chunks`
doc/behaviour mismatch (a missing hash is `NotFound`, not "skipped").
EOF

echo
echo "done."
(( APPLY )) || echo "(dry run — re-run with --apply and FORGEJO_TOKEN set)"
