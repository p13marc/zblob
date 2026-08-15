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
    BlobError, CdcParams, ContentStore, DownloadRequest, MemoryStore, Publisher, TreeClient,
    build_tree,
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
    Publisher::new(&session, common::serve(store_prefix.clone()))
        .snapshots(common::serve(tree_prefix.clone()))
        // Every chunk, not a sample: the point of this test is that the
        // producer can exit and the snapshot is genuinely retrievable.
        .coverage(zblob::SettleCoverage::All)
        .settle(Duration::from_secs(10))
        .publish(&index, &producer_store)
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

    Publisher::new(&session, common::serve(store_prefix.clone()))
        .snapshots(common::serve(tree_prefix.clone()))
        .coverage(zblob::SettleCoverage::All)
        .settle(Duration::from_secs(10))
        .publish(&published, &producer_store)
        .await
        .expect("publish snapshot");

    // The published snapshot's chunks are in the storage (discriminating
    // power: without this, the assertion below would hold for an empty
    // storage too).
    for hash in published.needed_chunks() {
        let key = zblob::keys::store_key(&store_prefix, zblob::HashAlgo::Blake3, &hash);
        assert!(
            probe(&session, &key).await,
            "the published snapshot must be retrievable: {key}"
        );
    }
    // The other snapshot's chunks are not.
    for hash in private.needed_chunks() {
        let key = zblob::keys::store_key(&store_prefix, zblob::HashAlgo::Blake3, &hash);
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

/// A stand-in "storage" that answers every query with an error reply — the
/// shape of a storage that is present but broken (permissions, a full disk).
async fn spawn_error_storage(
    session: &zenoh::Session,
    root: String,
) -> tokio::task::JoinHandle<()> {
    let q = session
        .declare_queryable(format!("{root}/**"))
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let _ = query.reply_err("storage is broken").await;
        }
    })
}

/// With no storage answering, the settle phase gives up with `NotSettled`
/// rather than returning a success a consumer cannot honor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publish_without_storage_fails_notsettled() {
    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let root = unique_prefix();
    let store_prefix = format!("{root}/store");
    let tree_prefix = format!("{root}/tree");

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("f.bin"), common::pseudo_random(40_000, 3)).unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &cdc, &store).unwrap();

    // No storage is running: nothing will ever answer the read-back.
    let err = Publisher::new(&session, common::serve(store_prefix))
        .snapshots(common::serve(tree_prefix))
        .coverage(zblob::SettleCoverage::All)
        .settle(Duration::from_millis(300))
        .publish(&index, &store)
        .await
        .expect_err("publish must not report success when nothing settled");
    assert!(matches!(err, BlobError::NotSettled(_)), "{err}");

    session.close().await.unwrap();
}

/// A storage that answers only with error replies never satisfies a probe, so
/// the publish does not settle. Discriminating power: an honest storage on the
/// same prefixes settles (covered by `publish_to_storage_then_download`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_answering_only_errors_never_settles() {
    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let root = unique_prefix();
    let store_prefix = format!("{root}/store");
    let tree_prefix = format!("{root}/tree");
    let storage = spawn_error_storage(&session, root.clone()).await;

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("f.bin"), common::pseudo_random(40_000, 4)).unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &cdc, &store).unwrap();

    let err = Publisher::new(&session, common::serve(store_prefix))
        .snapshots(common::serve(tree_prefix))
        .coverage(zblob::SettleCoverage::All)
        .settle(Duration::from_millis(300))
        .publish(&index, &store)
        .await
        .expect_err("an error-only storage must not settle");
    assert!(matches!(err, BlobError::NotSettled(_)), "{err}");

    storage.abort();
    session.close().await.unwrap();
}

/// `SettleCoverage::All` actually probes the chunks, not just the index: a
/// storage that captures the index but drops every chunk PUT does not settle.
/// The control is the full-storage test elsewhere in this file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_present_but_chunks_missing_trips_settle_all() {
    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let root = unique_prefix();
    let store_prefix = format!("{root}/store");
    let tree_prefix = format!("{root}/tree");

    // Storage covers only the tree (index) range — chunk PUTs to the store
    // range fall into the void, so the index settles but the chunks cannot.
    let storage = spawn_storage(&session, tree_prefix.clone()).await;

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("f.bin"), common::pseudo_random(90_000, 5)).unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &cdc, &store).unwrap();
    assert!(
        !index.needed_chunks().is_empty(),
        "the fixture must have chunks"
    );

    let err = Publisher::new(&session, common::serve(store_prefix))
        .snapshots(common::serve(tree_prefix))
        .coverage(zblob::SettleCoverage::All)
        .settle(Duration::from_millis(500))
        .publish(&index, &store)
        .await
        .expect_err("All coverage must catch missing chunks even when the index is present");
    assert!(matches!(err, BlobError::NotSettled(_)), "{err}");

    storage.abort();
    session.close().await.unwrap();
}

/// Publishing a hash the store cannot return fails: `Ok(None)` for a named
/// hash is `NotFound` (the caller asked for it), and an `Err` propagates as
/// I/O rather than being swallowed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lying_store_yields_notfound_or_propagates_io() {
    use zblob::Hash;

    /// Returns `Ok(None)` for `blind`, otherwise defers to an inner store.
    struct BlindStore {
        inner: MemoryStore,
        blind: Hash,
    }
    impl ContentStore for BlindStore {
        fn has(&self, h: &Hash) -> std::io::Result<bool> {
            self.inner.has(h)
        }
        fn get(&self, h: &Hash) -> std::io::Result<Option<Vec<u8>>> {
            if *h == self.blind {
                return Ok(None);
            }
            self.inner.get(h)
        }
        fn put(&self, h: &Hash, b: &[u8]) -> std::io::Result<()> {
            self.inner.put(h, b)
        }
        fn remove(&self, h: &Hash) -> std::io::Result<bool> {
            self.inner.remove(h)
        }
        fn for_each_hash(
            &self,
            f: &mut dyn FnMut(Hash) -> std::io::Result<()>,
        ) -> std::io::Result<()> {
            self.inner.for_each_hash(f)
        }
    }

    /// Fails every read with an I/O error.
    struct BrokenStore;
    impl ContentStore for BrokenStore {
        fn has(&self, _: &Hash) -> std::io::Result<bool> {
            Err(std::io::Error::other("disk on fire"))
        }
        fn get(&self, _: &Hash) -> std::io::Result<Option<Vec<u8>>> {
            Err(std::io::Error::other("disk on fire"))
        }
        fn put(&self, _: &Hash, _: &[u8]) -> std::io::Result<()> {
            Ok(())
        }
        fn remove(&self, _: &Hash) -> std::io::Result<bool> {
            Ok(false)
        }
        fn for_each_hash(
            &self,
            _: &mut dyn FnMut(Hash) -> std::io::Result<()>,
        ) -> std::io::Result<()> {
            Ok(())
        }
    }

    let session = Arc::new(zenoh::open(isolated_config()).await.unwrap());
    let store_prefix = format!("{}/store", unique_prefix());
    let bytes = common::pseudo_random(4096, 6);
    let hash = Hash::of(&bytes);

    // Ok(None) for a named hash → NotFound.
    let inner = MemoryStore::new();
    inner.put(&hash, &bytes).unwrap();
    let blind: Arc<dyn ContentStore> = Arc::new(BlindStore { inner, blind: hash });
    let err = Publisher::new(&session, common::serve(store_prefix.clone()))
        .chunks(std::slice::from_ref(&hash), &blind)
        .await
        .expect_err("a blind store must yield NotFound");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");

    // Err → I/O error propagates, not swallowed as absence.
    let broken: Arc<dyn ContentStore> = Arc::new(BrokenStore);
    let err = Publisher::new(&session, common::serve(store_prefix))
        .chunks(std::slice::from_ref(&hash), &broken)
        .await
        .expect_err("a broken store must propagate its error");
    assert!(matches!(err, BlobError::Io(_)), "{err}");

    session.close().await.unwrap();
}
