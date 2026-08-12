//! Tier-2 at scale: one query per round instead of one per chunk, and a probe
//! whose reply is a function of the question rather than of the objects.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{open_session, unique_prefix};
use zblob::{
    CancelToken, CdcParams, ContentStore, DownloadRequest, Hash, MemoryStore, StoreClient,
    TreeClient, TreeServer, build_tree,
};

fn small_cdc() -> CdcParams {
    CdcParams {
        min: 1024,
        avg: 2048,
        max: 8192,
        normalization: 2,
        gear_seed: 0,
    }
}

/// Build a tree with enough chunks that the difference is unmistakable.
fn many_chunk_tree(root: &std::path::Path) {
    std::fs::create_dir_all(root).unwrap();
    for i in 0..8 {
        std::fs::write(
            root.join(format!("f{i}.bin")),
            common::pseudo_random(60_000, 100 + i as u64),
        )
        .unwrap();
    }
}

/// A snapshot of N chunks costs ⌈N/batch⌉ queries, not N.
///
/// This is the crate's biggest scaling limit made measurable: a 100k-chunk
/// tree used to be 100k Zenoh queries. `TransferStats::queries` exists so the
/// claim is checkable rather than asserted, and so a fleet can tell whether
/// batching is being answered or silently falling back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_costs_one_query_per_round_not_one_per_chunk() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    many_chunk_tree(src.path());
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "big", &small_cdc(), &*server_store).unwrap();
    let chunks = index.needed_chunks().len();
    assert!(chunks >= 40, "fixture should be many chunks, got {chunks}");

    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index.clone()).await;
    let handle = server.spawn().await.unwrap();

    let batch = 16usize;
    let client = TreeClient::builder(
        session.clone(),
        common::query(store_prefix.clone()),
        common::query(tree_prefix.clone()),
    )
    .query_timeout(Duration::from_secs(5))
    .batch_size(batch)
    .build();

    let dest = tempfile::tempdir().unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let stats = client
        .download_tree(
            &DownloadRequest::pinned("big", index.root_hash),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("batched download");

    assert_eq!(stats.chunks_fetched as usize, chunks, "everything arrived");
    let expected_rounds = chunks.div_ceil(batch) as u64;
    assert_eq!(
        stats.queries, expected_rounds,
        "expected {expected_rounds} batched rounds for {chunks} chunks, got {}",
        stats.queries
    );
    assert!(
        (stats.queries as usize) < chunks / 2,
        "batching must be a large win, not a rounding error"
    );

    // Discriminating power: the same fetch with batching off costs one query
    // per chunk. Without this, the assertion above would also pass if the
    // counter were simply broken.
    let unbatched = TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(5))
    .batch_size(0)
    .build();
    let dest2 = tempfile::tempdir().unwrap();
    let store2: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let plain = unbatched
        .download_tree(
            &DownloadRequest::pinned("big", index.root_hash),
            dest2.path(),
            &store2,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("unbatched download");
    assert_eq!(plain.queries as usize, chunks, "one query per chunk");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A holder that has only some of the wanted chunks shortens the round rather
/// than failing it — deliberately unlike iroh's `GetMany`, which aborts as
/// soon as the provider lacks required data. On a bus of partial holders,
/// "answer what you have" composes and "abort" does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partial_holder_shortens_the_round_instead_of_failing_it() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    many_chunk_tree(src.path());
    let full: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "split", &small_cdc(), &*full).unwrap();
    let all = index.needed_chunks();

    // Two holders on the same store prefix, each with half the chunks and
    // neither able to serve the snapshot alone.
    let mut handles = Vec::new();
    for (n, half) in [(0usize, 0), (1, 1)] {
        let part: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
        for (i, h) in all.iter().enumerate() {
            if i % 2 == half {
                part.put(h, &full.get(h).unwrap()).unwrap();
            }
        }
        let srv = TreeServer::new(
            session.clone(),
            common::serve(store_prefix.clone()),
            // Only one of them serves the index, so the other is purely a
            // partial chunk holder.
            common::serve(format!("{tree_prefix}/h{n}")),
            part,
        );
        if n == 0 {
            srv.register(index.clone()).await;
        }
        handles.push(srv.spawn().await.unwrap());
    }

    let client = TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(format!("{tree_prefix}/h0")),
    )
    .query_timeout(Duration::from_secs(5))
    .batch_size(16)
    .build();
    let dest = tempfile::tempdir().unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    client
        .download_tree(
            &DownloadRequest::pinned("split", index.root_hash),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("two partial holders together hold everything");
    assert_eq!(store.hashes().unwrap().len(), all.len());

    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

/// The tier-2 probe's reply is a function of the *question*, not of the
/// objects asked about — which is the whole reason tier 2 can have a probe at
/// all, and why fanning one out across origins is legitimate where a tier-2
/// fetch is not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tier2_probe_answers_possession_without_shipping_anything() {
    let session = open_session().await;
    let base = unique_prefix();

    let src = tempfile::tempdir().unwrap();
    many_chunk_tree(src.path());
    let full: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "probed", &small_cdc(), &*full).unwrap();
    let all = index.needed_chunks();

    // host-a holds the first half; host-b holds nothing at all.
    let a_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    for h in all.iter().take(all.len() / 2) {
        a_store.put(h, &full.get(h).unwrap()).unwrap();
    }
    let mut handles = Vec::new();
    for (host, store) in [
        ("host-a", a_store),
        (
            "host-b",
            Arc::new(MemoryStore::new()) as Arc<dyn ContentStore>,
        ),
    ] {
        handles.push(
            TreeServer::new(
                session.clone(),
                common::serve(format!("{base}/{host}/store")),
                common::serve(format!("{base}/{host}/tree")),
                store,
            )
            .spawn()
            .await
            .unwrap(),
        );
    }

    // One wildcard-origin probe: legitimate here, because the reply is bits.
    let prober = StoreClient::builder(session.clone(), common::query(format!("{base}/*/store")))
        .query_timeout(Duration::from_secs(5))
        .build();
    let holders = prober.probe(&all).await.expect("probe");
    assert_eq!(holders.len(), 2, "both origins answered");

    let a = holders
        .iter()
        .find(|h| h.origin.as_str().contains("host-a"))
        .expect("host-a answered");
    let b = holders
        .iter()
        .find(|h| h.origin.as_str().contains("host-b"))
        .expect("host-b answered");
    assert_eq!(a.held.len(), all.len() / 2, "host-a reports its half");
    assert!(b.held.is_empty(), "host-b holds nothing and says so");
    // What it reports is true: every address it claimed is actually fetchable.
    let from_a = StoreClient::builder(session.clone(), a.origin.clone())
        .query_timeout(Duration::from_secs(5))
        .build();
    for h in &a.held {
        let bytes = from_a.fetch_chunk(h).await.expect("claimed chunks exist");
        assert_eq!(Hash::of(&bytes), *h);
    }

    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

/// "Who has this snapshot, and how much of it" — answered by four numbers,
/// whatever the snapshot's size.
///
/// This is what makes RFC 07 §2.5's probe-then-fetch total across all three
/// key families. Tier 2 previously had no probe, and the reasoning was sound
/// as far as it went — a `tree` or `store` key carries the object, so a
/// wildcard GET on one *is* the bulk fan-out §3 forbids. What follows is "give
/// tier 2 something small to ask for", not "tier 2 has no probe".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_probe_reports_partial_possession() {
    let session = open_session().await;
    let base = unique_prefix();

    let src = tempfile::tempdir().unwrap();
    many_chunk_tree(src.path());
    let full: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &small_cdc(), &*full).unwrap();
    let all = index.needed_chunks();

    // host-a is complete; host-b registered the same snapshot but holds only
    // a third of its chunks.
    let partial: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    for h in all.iter().take(all.len() / 3) {
        partial.put(h, &full.get(h).unwrap()).unwrap();
    }
    let mut handles = Vec::new();
    for (host, store) in [("host-a", full.clone()), ("host-b", partial)] {
        let srv = TreeServer::new(
            session.clone(),
            common::serve(format!("{base}/{host}/store")),
            common::serve(format!("{base}/{host}/tree")),
            store,
        );
        srv.register(index.clone()).await;
        handles.push(srv.spawn().await.unwrap());
    }

    let prober = TreeClient::builder(
        session.clone(),
        common::query(format!("{base}/*/store")),
        common::query(format!("{base}/*/tree")),
    )
    .query_timeout(Duration::from_secs(5))
    .build();

    let holders = prober.probe_snapshot("snap").await.expect("probe");
    assert_eq!(holders.len(), 2, "both origins answered");
    let complete = holders
        .iter()
        .find(|(o, _)| o.as_str().contains("host-a"))
        .expect("host-a");
    let short = holders
        .iter()
        .find(|(o, _)| o.as_str().contains("host-b"))
        .expect("host-b");
    assert!(complete.1.is_complete(), "host-a can serve it alone");
    assert_eq!(complete.1.chunks_total as usize, all.len());
    assert!(!short.1.is_complete(), "host-b cannot");
    assert_eq!(short.1.chunks_present as usize, all.len() / 3);
    assert!(short.1.have_index, "…but it does have the index");

    // The verdict is actionable: fetch from the holder that reported complete.
    let dest = tempfile::tempdir().unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    TreeClient::builder(
        session.clone(),
        common::query(complete.0.as_str().replace("/tree", "/store")),
        complete.0.clone(),
    )
    .query_timeout(Duration::from_secs(5))
    .build()
    .download_tree(
        &DownloadRequest::pinned("snap", index.root_hash),
        dest.path(),
        &store,
        &(),
        &CancelToken::new(),
    )
    .await
    .expect("the holder that said complete really is");

    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}
