//! A hostile **tier-2** peer, against a fixed oracle.
//!
//! `hostile_peer.rs` covers tier 1 only, so every rejection branch on the
//! tier-2 fetch path — `accept_batch_reply`'s six, the want-list refusals, the
//! per-chunk fallback — was reachable by an attacker and exercised by nothing.
//! That path is *newer* than the tier-1 one and has more ways to go wrong: a
//! batch reply arrives on a key disjoint from the query, so the client has to
//! attribute it itself.
//!
//! The oracle is the same as tier 1's: **succeed with exactly the right bytes,
//! or fail cleanly.** A hostile holder must never make the client write wrong
//! bytes, and must never make it hang.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{open_session, pseudo_random, unique_prefix};
use zblob::keys::{Tier2Tail, parse_tier2_tail, store_key};
use zblob::wire::{ENC_CHUNK, ENC_HAVEBITS, HaveBits, WantList, encode};
use zblob::{ContentStore, Hash, HashAlgo, MemoryStore, StoreClient};

/// How a hostile holder corrupts the chunk reply it is about to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutation {
    /// Behave honestly (the control case).
    None,
    /// Reply with an empty payload.
    Empty,
    /// Drop the container's tag byte, so the framing is undecodable.
    Unframed,
    /// Flip a byte inside the framed content.
    FlipContent,
    /// Serve a *different* chunk's bytes under this chunk's key.
    SwapContent,
    /// Reply on another chunk's key.
    ShiftKey,
    /// Reply on a key outside the store prefix entirely.
    ForeignKey,
    /// Claim a hash algorithm this crate does not speak.
    ForeignAlgo,
    /// Reply with the wrong Zenoh encoding tag.
    WrongEncoding,
    /// Pad the payload far past the declared chunk length — the branch that
    /// refuses to even *unframe* an over-long reply, so a zip bomb is never
    /// expanded.
    Oversized,
    /// Answer every request with the same chunk.
    AlwaysFirst,
}

const MUTATIONS: &[Mutation] = &[
    Mutation::None,
    Mutation::Empty,
    Mutation::Unframed,
    Mutation::FlipContent,
    Mutation::SwapContent,
    Mutation::ShiftKey,
    Mutation::ForeignKey,
    Mutation::ForeignAlgo,
    Mutation::WrongEncoding,
    Mutation::Oversized,
    Mutation::AlwaysFirst,
];

/// A chunk in a self-describing container (`0x00` + raw bytes), as a real
/// holder frames it. A fake server that skips this is rejected before the code
/// under test runs, and the test passes for the wrong reason.
fn framed(bytes: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(bytes.len() + 1);
    v.push(0x00);
    v.extend_from_slice(bytes);
    v
}

/// Every mutation must leave the client with the right bytes or a clean
/// failure — never wrong bytes, never a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn batch_reply_mutations_never_yield_wrong_bytes() {
    let session = open_session().await;
    let base = unique_prefix();

    // Four distinct chunks, so "served someone else's bytes" is detectable.
    let chunks: Vec<Vec<u8>> = (0..4u64).map(|i| pseudo_random(6000, 900 + i)).collect();
    let hashes: Vec<Hash> = chunks.iter().map(|c| Hash::of(c)).collect();

    for (i, mutation) in MUTATIONS.iter().copied().enumerate() {
        let prefix = format!("{base}/m{i}");
        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let (srv_prefix, srv_chunks, srv_hashes) = (prefix.clone(), chunks.clone(), hashes.clone());
        let served = Arc::new(AtomicUsize::new(0));
        let counter = served.clone();

        let server = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let key = query.key_expr().as_str().to_string();
                let Some(tail) = parse_tier2_tail(&srv_prefix, &key) else {
                    continue;
                };
                let Tier2Tail::Two(_algo, last) = tail else {
                    continue;
                };

                // The probe answers honestly: this test is about the *fetch*
                // path, and a lying probe only changes who gets asked.
                if last == "have" {
                    let want: WantList =
                        zblob::wire::decode(&query.payload().unwrap().to_bytes()).unwrap();
                    let mut bits = vec![0u8; want.hashes.len().div_ceil(8)];
                    for (n, h) in want.hashes.iter().enumerate() {
                        if srv_hashes.contains(h) {
                            bits[n / 8] |= 1 << (n % 8);
                        }
                    }
                    let reply = HaveBits {
                        version: zblob::wire::WIRE_VERSION,
                        count: want.hashes.len() as u32,
                        bits,
                    };
                    let _ = query
                        .reply(key.clone(), encode(&reply).unwrap())
                        .encoding(&ENC_HAVEBITS)
                        .await;
                    continue;
                }

                // Which chunks are being asked for? Either a batch want-list
                // or a single chunk key.
                let wanted: Vec<Hash> = if last == "batch" {
                    match query.payload().map(|p| p.to_bytes()) {
                        Some(bytes) => zblob::wire::decode::<WantList>(&bytes)
                            .map(|w| w.hashes)
                            .unwrap_or_default(),
                        None => Vec::new(),
                    }
                } else {
                    match last.parse::<Hash>() {
                        Ok(h) => vec![h],
                        Err(_) => continue,
                    }
                };

                for hash in wanted {
                    let Some(idx) = srv_hashes.iter().position(|h| *h == hash) else {
                        continue;
                    };
                    counter.fetch_add(1, Ordering::Relaxed);

                    let (payload, reply_key, encoding) = match mutation {
                        Mutation::None => (
                            framed(&srv_chunks[idx]),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::Empty => (
                            Vec::new(),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::Unframed => (
                            srv_chunks[idx].clone(),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::FlipContent => {
                            let mut p = framed(&srv_chunks[idx]);
                            let mid = p.len() / 2;
                            p[mid] ^= 0xFF;
                            (
                                p,
                                store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                                ENC_CHUNK.encoding().clone(),
                            )
                        }
                        Mutation::SwapContent => (
                            framed(&srv_chunks[(idx + 1) % srv_chunks.len()]),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::ShiftKey => (
                            framed(&srv_chunks[idx]),
                            store_key(
                                &srv_prefix,
                                HashAlgo::Blake3,
                                &srv_hashes[(idx + 1) % srv_hashes.len()],
                            ),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::ForeignKey => (
                            framed(&srv_chunks[idx]),
                            format!("{srv_prefix}/elsewhere/{hash}"),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::ForeignAlgo => (
                            framed(&srv_chunks[idx]),
                            format!("{srv_prefix}/sha256/{hash}"),
                            ENC_CHUNK.encoding().clone(),
                        ),
                        Mutation::WrongEncoding => (
                            framed(&srv_chunks[idx]),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            zenoh::bytes::Encoding::from("application/octet-stream"),
                        ),
                        Mutation::Oversized => {
                            let mut p = framed(&srv_chunks[idx]);
                            p.extend(std::iter::repeat_n(0u8, 1_000_000));
                            (
                                p,
                                store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                                ENC_CHUNK.encoding().clone(),
                            )
                        }
                        Mutation::AlwaysFirst => (
                            framed(&srv_chunks[0]),
                            store_key(&srv_prefix, HashAlgo::Blake3, &hash),
                            ENC_CHUNK.encoding().clone(),
                        ),
                    };
                    let _ = query.reply(reply_key, payload).encoding(encoding).await;
                }
            }
        });

        let client = StoreClient::builder(&session, common::query(&prefix))
            .query_timeout(Duration::from_millis(600))
            .build();

        // 1. A single-chunk fetch.
        let one = client.fetch_chunk(&hashes[1]).await;
        match &one {
            Ok(bytes) => {
                assert_eq!(
                    bytes, &chunks[1],
                    "{mutation:?}: fetch_chunk succeeded with the wrong bytes"
                );
                assert_eq!(mutation, Mutation::None, "{mutation:?} must not succeed");
            }
            Err(e) => assert_ne!(
                mutation,
                Mutation::None,
                "the honest control must succeed, got {e}"
            ),
        }

        // 2. A batch fetch of everything.
        let refs: Vec<zblob::ChunkRef> = hashes
            .iter()
            .zip(&chunks)
            .map(|(h, c)| zblob::ChunkRef {
                hash: *h,
                len: c.len() as u32,
            })
            .collect();
        let many = client.fetch_many(&refs).await;
        match &many {
            Ok(got) => {
                for (h, bytes) in got {
                    let idx = hashes.iter().position(|x| x == h).expect("unknown hash");
                    assert_eq!(
                        bytes, &chunks[idx],
                        "{mutation:?}: fetch_many returned the wrong bytes for {h}"
                    );
                }
                // The oracle allows a *partial* correct answer — that is a
                // clean failure the caller completes elsewhere, not a lie.
                // What it forbids is a hostile peer producing a complete one.
                // (`AlwaysFirst` legitimately satisfies one address: the one
                // whose bytes it is actually serving.)
                if mutation != Mutation::None {
                    assert!(
                        got.len() < refs.len(),
                        "{mutation:?}: a hostile holder must not complete a fetch"
                    );
                }
            }
            Err(_) => assert_ne!(mutation, Mutation::None, "the honest control must succeed"),
        }
        if mutation == Mutation::None {
            assert_eq!(many.unwrap().len(), refs.len(), "the control must be whole");
        }

        // The peer was actually asked — without this the oracle would be
        // satisfied by a server that never ran.
        assert!(
            served.load(Ordering::Relaxed) > 0,
            "{mutation:?}: the hostile peer was never queried"
        );

        server.abort();
    }

    session.close().await.unwrap();
}

/// A holder that answers the batch key with nothing must not stop the
/// per-chunk fallback from finishing the job.
///
/// That fallback is what keeps `docs/router-storage.md`'s publish-then-exit
/// tier working — a Zenoh storage serves by key and has nothing at `…/batch`,
/// so *every* snapshot fetched from a storage goes through it — and nothing
/// tested it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_batch_endpoint_still_resolves_through_the_per_chunk_fallback() {
    use zblob::{CdcParams, DownloadRequest, MemoryStore, TreeClient, build_tree};

    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    // A real snapshot, so the fallback is exercised by the code that actually
    // uses it rather than by a hand-rolled chunk list.
    let src = tempfile::tempdir().unwrap();
    let payload = pseudo_random(120_000, 701);
    std::fs::write(src.path().join("data.bin"), &payload).unwrap();
    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let producer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &cdc, &producer).unwrap();
    assert!(
        index.needed_chunks().len() > 4,
        "the fixture must need several chunks"
    );

    let batch_asks = Arc::new(AtomicUsize::new(0));
    let single_asks = Arc::new(AtomicUsize::new(0));

    // A storage-shaped peer: answers a chunk GET by key, has nothing at
    // `…/batch` and nothing at `…/have`, and serves the index by key.
    let q = session
        .declare_queryable(format!("{store_prefix}/**"))
        .await
        .unwrap();
    let tq = session
        .declare_queryable(format!("{tree_prefix}/**"))
        .await
        .unwrap();
    let (sp, store, idx) = (store_prefix.clone(), producer.clone(), index.clone());
    let (b, s) = (batch_asks.clone(), single_asks.clone());
    let server = tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let key = query.key_expr().as_str().to_string();
            let Some(Tier2Tail::Two(_, last)) = parse_tier2_tail(&sp, &key) else {
                continue;
            };
            if last == "batch" {
                b.fetch_add(1, Ordering::Relaxed);
                continue; // a storage has nothing here: no reply, no error.
            }
            if last == "have" {
                continue;
            }
            let Ok(hash) = last.parse::<Hash>() else {
                continue;
            };
            let Ok(Some(bytes)) = store.get(&hash) else {
                continue;
            };
            s.fetch_add(1, Ordering::Relaxed);
            let _ = query
                .reply(key.clone(), framed(&bytes))
                .encoding(&ENC_CHUNK)
                .await;
        }
    });
    let tp = tree_prefix.clone();
    let tree_server = tokio::spawn(async move {
        while let Ok(query) = tq.recv_async().await {
            let key = query.key_expr().as_str().to_string();
            if !matches!(parse_tier2_tail(&tp, &key), Some(Tier2Tail::One(_))) {
                continue;
            }
            let _ = query
                .reply(key.clone(), encode(&idx).unwrap())
                .encoding(&zblob::wire::ENC_INDEX)
                .await;
        }
    });

    let consumer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let client = TreeClient::builder(
        &session,
        common::query(&store_prefix),
        common::query(&tree_prefix),
    )
    .query_timeout(Duration::from_secs(3))
    .build();

    let dest = tempfile::tempdir().unwrap();
    let stats = client
        .download_tree(
            &DownloadRequest::pinned("snap", index.root_hash),
            dest.path(),
            &consumer,
        )
        .await
        .expect("the fallback must finish the transfer");

    assert_eq!(
        std::fs::read(dest.path().join("data.bin")).unwrap(),
        payload,
        "wrong bytes materialized"
    );
    assert_eq!(stats.rejected, 0);

    // Discriminating power: the batch endpoint really was tried and really
    // answered nothing, so every chunk came through the fallback.
    assert!(
        batch_asks.load(Ordering::Relaxed) > 0,
        "the batch endpoint must be attempted first"
    );
    assert!(
        single_asks.load(Ordering::Relaxed) >= index.needed_chunks().len(),
        "every chunk must have been fetched individually: {} asks for {} chunks",
        single_asks.load(Ordering::Relaxed),
        index.needed_chunks().len()
    );

    server.abort();
    tree_server.abort();
    session.close().await.unwrap();
}

/// A want-list over the server's cap is refused rather than served, and the
/// refusal does not take the connection with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_oversized_want_list_is_refused_not_served() {
    use zblob::TreeServer;

    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let chunks: Vec<Vec<u8>> = (0..8u64).map(|i| pseudo_random(1000, 600 + i)).collect();
    let hashes: Vec<Hash> = chunks
        .iter()
        .map(|c| {
            let h = Hash::of(c);
            store.put(&h, c).unwrap();
            h
        })
        .collect();

    let server = TreeServer::builder(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        store.clone(),
    )
    .max_want_list(4)
    .build();
    let handle = server.spawn().await.unwrap();

    // A want-list at the cap is served…
    let at_cap = WantList::new(hashes[..4].to_vec());
    let replies = session
        .get(zblob::keys::store_batch_key(
            &store_prefix,
            HashAlgo::Blake3,
        ))
        .payload(encode(&at_cap).unwrap())
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let mut served = 0;
    while let Ok(r) = replies.recv_async().await {
        if r.result().is_ok() {
            served += 1;
        }
    }
    assert_eq!(served, 4, "a want-list at the cap must be served");

    // …one over it is not, and the server keeps serving afterwards.
    let over_cap = WantList::new(hashes.clone());
    let replies = session
        .get(zblob::keys::store_batch_key(
            &store_prefix,
            HashAlgo::Blake3,
        ))
        .payload(encode(&over_cap).unwrap())
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let mut chunks_back = 0;
    while let Ok(r) = replies.recv_async().await {
        if r.result().is_ok() {
            chunks_back += 1;
        }
    }
    assert_eq!(
        chunks_back, 0,
        "an over-cap want-list must yield no chunks, got {chunks_back}"
    );

    // Still alive: the refusal was of the request, not of the client.
    let again = StoreClient::new(&session, common::query(&store_prefix))
        .fetch_chunk(&hashes[0])
        .await
        .expect("the server must survive refusing a request");
    assert_eq!(again, chunks[0]);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A batch query without `accept_replies(ReplyKeyExpr::Any)` is refused **on
/// the server** — the trap `CLAUDE.md` documents, and one nothing checked.
///
/// Replies come back on each chunk's own key, which is disjoint from the batch
/// key. So the check is not a nicety: without it the batch tier silently
/// returns nothing, and the failure looks like an absent holder.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_query_without_any_reply_keys_gets_nothing() {
    use zblob::TreeServer;

    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let data = pseudo_random(2000, 555);
    let hash = Hash::of(&data);
    store.put(&hash, &data).unwrap();

    let handle = TreeServer::new(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        store.clone(),
    )
    .spawn()
    .await
    .unwrap();

    let want = WantList::new(vec![hash]);
    let batch_key = zblob::keys::store_batch_key(&store_prefix, HashAlgo::Blake3);

    // Default reply-key policy: `MatchingQuery`. The reply lands on the
    // chunk's key, which does not match, so it is dropped at the server.
    let replies = session
        .get(&batch_key)
        .payload(encode(&want).unwrap())
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let mut strict = 0;
    while let Ok(r) = replies.recv_async().await {
        if r.result().is_ok() {
            strict += 1;
        }
    }

    // …and with the policy the crate actually sets, the same query works.
    let replies = session
        .get(&batch_key)
        .payload(encode(&want).unwrap())
        .accept_replies(zenoh::query::ReplyKeyExpr::Any)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_secs(2))
        .await
        .unwrap();
    let mut permissive = 0;
    while let Ok(r) = replies.recv_async().await {
        if r.result().is_ok() {
            permissive += 1;
        }
    }

    assert_eq!(permissive, 1, "the documented form must work");
    assert_eq!(
        strict, 0,
        "without ReplyKeyExpr::Any the batch reply must not arrive — if this \
         starts passing, Zenoh's rule changed and the crate's comment is stale"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
