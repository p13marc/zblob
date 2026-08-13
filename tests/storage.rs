//! Serverless Tier-2: a producer publishes a snapshot into a Zenoh *storage*
//! and exits; a client reconstructs it from the storage with no `TreeServer`
//! ever running. `publish_snapshot`'s read-back settle phase replaces the old
//! sleep-and-hope synchronization.
//!
//! Real deployments use `zenoh-plugin-storage-manager` on a router. Here a tiny
//! in-process `StandInStorage` stands in for it — it does exactly what a storage
//! does: subscribe to a key range to capture PUTs, and answer GETs on that range
//! from what it captured (preserving each PUT's encoding, as a storage does).

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::{isolated_config, unique_prefix};
use zblob::{
    CdcParams, ContentStore, DownloadRequest, MemoryStore, TreeClient, build_tree, publish_snapshot,
};

/// A minimal stand-in for `zenoh-plugin-storage-manager`: retain PUTs on a key
/// range and reply to GETs on that range. Content-addressed keys are immutable,
/// so last-writer-wins storage is exact.
async fn spawn_storage(session: &zenoh::Session, root: String) -> tokio::task::JoinHandle<()> {
    let sub = session
        .declare_subscriber(format!("{root}/**"))
        .await
        .unwrap();
    let q = session
        .declare_queryable(format!("{root}/**"))
        .await
        .unwrap();
    tokio::spawn(async move {
        type Stored = HashMap<String, (Vec<u8>, zenoh::bytes::Encoding)>;
        let map: Arc<Mutex<Stored>> = Arc::new(Mutex::new(HashMap::new()));
        loop {
            tokio::select! {
                Ok(sample) = sub.recv_async() => {
                    let key = sample.key_expr().as_str().to_string();
                    let bytes = sample.payload().to_bytes().to_vec();
                    let enc = sample.encoding().clone();
                    map.lock().unwrap().insert(key, (bytes, enc));
                }
                Ok(query) = q.recv_async() => {
                    let key = query.key_expr().as_str().to_string();
                    let value = map.lock().unwrap().get(&key).cloned();
                    if let Some((bytes, enc)) = value {
                        let _ = query.reply(query.key_expr().clone(), bytes).encoding(enc).await;
                    }
                }
                else => break,
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_to_storage_then_download_without_server() {
    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let root = unique_prefix();
    let store_prefix = format!("{root}/store");
    let tree_prefix = format!("{root}/tree");

    // Stand-in storage covers both the chunk and index key ranges; its
    // subscriber + queryable are declared before we publish.
    let storage = spawn_storage(&session, root.clone()).await;

    // Producer builds a snapshot and publishes it into the storage.
    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src.path().join("sub")).unwrap();
    let big = common::pseudo_random(90_000, 7);
    std::fs::write(src.path().join("big.bin"), &big).unwrap();
    std::fs::write(src.path().join("sub/a.txt"), b"alpha").unwrap();

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let producer_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap1", &cdc, &producer_store).unwrap();
    let expected_root = index.root_hash;

    // The settle phase inside publish_snapshot read-backs the index + sampled
    // chunks — when it returns Ok, a client can fetch immediately (no sleeps).
    publish_snapshot(
        &session,
        &common::serve(store_prefix.clone()),
        &common::serve(tree_prefix.clone()),
        &index,
        &producer_store,
        zblob::ChunkCompression::default(),
        // Every chunk, not a sample: the point of this test is that the
        // producer can exit and the snapshot is genuinely retrievable.
        zblob::SettleCoverage::All,
        Duration::from_secs(10),
    )
    .await
    .expect("publish snapshot");

    // The producer is "gone": only the storage answers from here on.
    let client_dir = tempfile::tempdir().unwrap();
    let client = TreeClient::builder(
        &session,
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(5))
    .build();
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    client
        .download_tree(
            &DownloadRequest::pinned("snap1", expected_root),
            client_dir.path(),
            &client_store,
        )
        .await
        .expect("download from storage");

    // Reconstructed byte-for-byte from the storage alone.
    assert_eq!(
        std::fs::read(client_dir.path().join("big.bin")).unwrap(),
        big
    );
    assert_eq!(
        std::fs::read(client_dir.path().join("sub/a.txt")).unwrap(),
        b"alpha"
    );

    storage.abort();
    session.close().await.unwrap();
}

/// `publish_snapshot` publishes the snapshot, not the store it was built in.
///
/// A producer's store routinely holds chunks from other snapshots — and on a
/// shared machine, other tenants' — while the storage being published into is
/// typically fleet-wide. Iterating `store.hashes()` therefore exported
/// everything the producer happened to have.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_snapshot_exports_only_the_snapshot() {
    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let root = unique_prefix();
    let store_prefix = format!("{root}/store");
    let tree_prefix = format!("{root}/tree");
    let storage = spawn_storage(&session, root.clone()).await;

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };

    // One store, two snapshots — the ordinary shape for a producer that keeps
    // a warm store to dedup against.
    let producer_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let published_src = tempfile::tempdir().unwrap();
    std::fs::write(published_src.path().join("shipped.bin"), b"this one ships").unwrap();
    let published = build_tree(published_src.path(), "shipped", &cdc, &producer_store).unwrap();

    let private_src = tempfile::tempdir().unwrap();
    let secret = b"this one must not leave the producer".to_vec();
    std::fs::write(private_src.path().join("private.bin"), &secret).unwrap();
    let private = build_tree(private_src.path(), "private", &cdc, &producer_store).unwrap();

    publish_snapshot(
        &session,
        &common::serve(store_prefix.clone()),
        &common::serve(tree_prefix.clone()),
        &published,
        &producer_store,
        zblob::ChunkCompression::default(),
        zblob::SettleCoverage::All,
        Duration::from_secs(10),
    )
    .await
    .expect("publish snapshot");

    // The published snapshot's chunks are in the storage (discriminating
    // power: without this, the assertion below would hold for an empty
    // storage too).
    for hash in published.needed_chunks() {
        let key = zblob::store_key(&store_prefix, zblob::HashAlgo::Blake3, &hash);
        assert!(
            probe(&session, &key).await,
            "the published snapshot must be retrievable: {key}"
        );
    }
    // The other snapshot's chunks are not.
    for hash in private.needed_chunks() {
        let key = zblob::store_key(&store_prefix, zblob::HashAlgo::Blake3, &hash);
        assert!(
            !probe(&session, &key).await,
            "an unpublished snapshot's chunk leaked into the storage: {key}"
        );
    }

    storage.abort();
    session.close().await.unwrap();
}

/// Does anything answer for `key`?
async fn probe(session: &zenoh::Session, key: &str) -> bool {
    let replies = session
        .get(key)
        .timeout(Duration::from_millis(700))
        .await
        .unwrap();
    while let Ok(reply) = replies.recv_async().await {
        if reply.result().is_ok() {
            return true;
        }
    }
    false
}
