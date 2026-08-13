//! Every allocation bound, driven with an input just over the line.
//!
//! These knobs exist because a remote peer would otherwise choose how much
//! this process allocates. Each was configurable and documented and none was
//! exercised: a builder method that silently failed to take effect — which is
//! exactly the shape of bug B5 turned out to be — would have looked identical
//! to a working one.
//!
//! Each case pairs the refusal with a value just *under* the limit, so a
//! bound that refused everything would fail here too.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobServer, BlobSpec, CdcParams, ContentStore, DownloadRequest, Hash,
    MIN_CHUNK_SIZE, MemoryBlobSource, MemoryStore, Publisher, StoreClient, TreeClient, TreeServer,
    build_tree,
};

fn small_cdc() -> CdcParams {
    CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    }
}

/// `BlobClientBuilder::max_blob_size` bounds the `.part` preallocation and the
/// resume bitfield — the two allocations a remote manifest sizes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_over_max_blob_size_is_refused_before_anything_is_written() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 301);

    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    let manifest = server
        .register_source(
            BlobSpec::new("big").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dir = tempfile::tempdir().unwrap();

    // Just under: the transfer runs.
    let ok_dest = dir.path().join("ok.bin");
    BlobClient::builder(&session, common::query(&prefix))
        .query_timeout(Duration::from_secs(3))
        .max_blob_size(data.len() as u64)
        .build()
        .download_to(&DownloadRequest::pinned("big", manifest.root), &ok_dest)
        .await
        .expect("a blob exactly at the limit must be allowed");
    assert_eq!(std::fs::read(&ok_dest).unwrap(), data);

    // Just over: refused, and nothing was created for it.
    let no_dest = dir.path().join("refused.bin");
    let err = BlobClient::builder(&session, common::query(&prefix))
        .query_timeout(Duration::from_secs(3))
        .max_blob_size(data.len() as u64 - 1)
        .build()
        .download_to(&DownloadRequest::pinned("big", manifest.root), &no_dest)
        .await
        .expect_err("a blob over the limit must be refused");
    assert!(
        matches!(err, zblob::BlobError::InvalidManifest(_)),
        "unexpected: {err}"
    );
    assert!(
        !no_dest.exists() && !no_dest.with_extension("bin.part").exists(),
        "a refused manifest must not leave a preallocated partial behind"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// `BlobServerBuilder::max_chunks_per_query` bounds the work one query can
/// commit the *server* to, and is advertised so a client clamps to it rather
/// than having its queries rejected with no way to find out why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_chunk_cap_is_advertised_and_clamps_the_client() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 20, 302);

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .max_chunks_per_query(3)
        .build();
    let manifest = server
        .register_source(
            BlobSpec::new("capped").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // The cap rode out on the manifest…
    assert_eq!(
        manifest.max_chunks_per_query(),
        Some(3),
        "the server must advertise its cap"
    );

    // …and a client whose own default is far larger still completes, because
    // it clamps to what the server said.
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("capped.bin");
    let stats = BlobClient::builder(&session, common::query(&prefix))
        .query_timeout(Duration::from_secs(5))
        .max_chunks_per_query(512)
        .build()
        .download_to(&DownloadRequest::pinned("capped", manifest.root), &dest)
        .await
        .expect("a client with a larger default must clamp, not fail");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    // Discriminating power: it genuinely took several rounds, so the clamp
    // was applied rather than the cap being ignored.
    assert!(
        stats.queries >= 20 / 3,
        "20 chunks at 3 per query must take several rounds, took {}",
        stats.queries
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// `StoreClientBuilder::max_chunk_bytes` bounds a single tier-2 chunk reply.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_chunk_over_max_chunk_bytes_is_refused() {
    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let data = pseudo_random(20_000, 303);
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

    // Just under the line: fetched.
    let got = StoreClient::builder(&session, common::query(&store_prefix))
        .query_timeout(Duration::from_secs(3))
        .max_chunk_bytes(data.len())
        .build()
        .fetch_chunk(&hash)
        .await
        .expect("a chunk at the limit must be fetchable");
    assert_eq!(got, data);

    // Just over: refused rather than allocated.
    let err = StoreClient::builder(&session, common::query(&store_prefix))
        .query_timeout(Duration::from_secs(1))
        // The container adds a tag byte and, when compressed, a 4-byte
        // length, so the effective frame bound is `max_chunk_bytes + 5` — the
        // first value that actually refuses this chunk is six under it.
        .max_chunk_bytes(data.len() - 6)
        .build()
        .fetch_chunk(&hash)
        .await
        .expect_err("a chunk over the limit must be refused");
    assert!(!err.to_string().is_empty());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// `TreeClientBuilder::max_tree_bytes` bounds the work a remote *index* can
/// commit the client to, before the first chunk GET.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_over_max_tree_bytes_is_refused_before_fetching() {
    let session = open_session().await;
    let store_prefix = unique_prefix();
    let tree_prefix = unique_prefix();

    let src = tempfile::tempdir().unwrap();
    let payload = pseudo_random(60_000, 304);
    std::fs::write(src.path().join("f.bin"), &payload).unwrap();

    let producer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap", &small_cdc(), &producer).unwrap();
    let total = index.total_size();

    let server = TreeServer::new(
        &session,
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        producer.clone(),
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let make = |limit: u64| {
        TreeClient::builder(
            &session,
            common::query(&store_prefix),
            common::query(&tree_prefix),
        )
        .query_timeout(Duration::from_secs(3))
        .max_tree_bytes(limit)
        .build()
    };

    // Just under the line.
    let dest = tempfile::tempdir().unwrap();
    let consumer: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    make(total)
        .download_tree(
            &DownloadRequest::pinned("snap", index.root_hash),
            dest.path(),
            &consumer,
        )
        .await
        .expect("a snapshot exactly at the limit must be allowed");

    // Just over: refused, and nothing was fetched for it.
    let dest2 = tempfile::tempdir().unwrap();
    let consumer2: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = make(total - 1)
        .download_tree(
            &DownloadRequest::pinned("snap", index.root_hash),
            dest2.path(),
            &consumer2,
        )
        .await
        .expect_err("a snapshot over the limit must be refused");
    assert!(
        matches!(err, zblob::BlobError::InvalidManifest(_)),
        "unexpected: {err}"
    );
    assert!(
        consumer2.hashes().unwrap().is_empty(),
        "the bound must be applied before the first chunk GET, not after"
    );
    assert!(
        std::fs::read_dir(dest2.path()).unwrap().next().is_none(),
        "nothing must be materialized"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// `Publisher::store` mirrors a whole content store, as opposed to the chunks
/// one snapshot references.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn publishing_a_whole_store_publishes_everything_in_it() {
    let session = open_session().await;
    let store_prefix = unique_prefix();

    // A storage stand-in: a queryable recording every key it is asked for,
    // answering from what was PUT.
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let sub = session
        .declare_subscriber(format!("{store_prefix}/**"))
        .await
        .unwrap();
    let recorder = {
        let seen = seen.clone();
        tokio::spawn(async move {
            while let Ok(sample) = sub.recv_async().await {
                seen.lock()
                    .unwrap()
                    .insert(sample.key_expr().as_str().to_string());
            }
        })
    };

    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let chunks: Vec<Vec<u8>> = (0..6u64).map(|i| pseudo_random(3000, 400 + i)).collect();
    let hashes: Vec<Hash> = chunks
        .iter()
        .map(|c| {
            let h = Hash::of(c);
            store.put(&h, c).unwrap();
            h
        })
        .collect();
    assert_eq!(
        hashes
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        chunks.len(),
        "the fixture chunks must be distinct"
    );

    let published = Publisher::new(&session, common::serve(store_prefix.clone()))
        .store(&store)
        .await
        .expect("publish_store");
    assert_eq!(
        published,
        chunks.len() as u32,
        "every chunk must be counted"
    );

    // The PUTs are asynchronous relative to this subscriber, so wait for them
    // rather than sleeping a fixed amount.
    for _ in 0..200 {
        if seen.lock().unwrap().len() >= chunks.len() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let seen = seen.lock().unwrap().clone();
    for h in &hashes {
        let key = zblob::keys::store_key(&store_prefix, zblob::HashAlgo::Blake3, h);
        assert!(seen.contains(&key), "chunk {h} was not published");
    }

    // A publisher over an empty store publishes nothing and says so, rather
    // than erroring — the case a re-publish after a GC hits.
    let empty: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    assert_eq!(
        Publisher::new(&session, common::serve(store_prefix.clone()))
            .store(&empty)
            .await
            .unwrap(),
        0
    );

    recorder.abort();
    session.close().await.unwrap();
}
