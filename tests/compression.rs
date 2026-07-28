//! End-to-end zstd: a compressing TreeServer serves framed chunks that a
//! plain client transparently unpacks and verifies; a compressing DirStore
//! round-trips through a full tree download.
#![cfg(feature = "zstd")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{open_session, unique_prefix};
use zblob::{
    CancelToken, CdcParams, ChunkCompression, ContentStore, DirStore, DownloadRequest, MemoryStore,
    TreeClient, TreeServer, build_tree,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compressed_wire_and_store_roundtrip() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    // Highly compressible content so zstd actually engages.
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("logs.txt"), "log line\n".repeat(20_000)).unwrap();
    std::fs::write(
        src.path().join("data.bin"),
        common::pseudo_random(60_000, 61),
    )
    .unwrap();

    let cdc = CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    };
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "zsnap", &cdc, &*server_store).unwrap();
    let expected_root = index.root_hash;

    let server = TreeServer::builder(
        session.clone(),
        store_prefix.clone(),
        tree_prefix.clone(),
        server_store,
    )
    .compression(ChunkCompression::Zstd { level: 3 })
    .build();
    server.register(index).await;
    let handle = server.spawn().await.unwrap();

    // The client store also compresses at rest.
    let store_dir = tempfile::tempdir().unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(
        DirStore::open(store_dir.path())
            .unwrap()
            .with_compression(ChunkCompression::Zstd { level: 3 })
            .with_verify_on_read(true),
    );
    let dest = tempfile::tempdir().unwrap();
    let client = TreeClient::builder(session.clone(), &store_prefix, &tree_prefix)
        .query_timeout(Duration::from_secs(5))
        .build();
    client
        .download_tree(
            &DownloadRequest::pinned("zsnap", expected_root),
            dest.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("compressed download");

    assert_eq!(
        std::fs::read(dest.path().join("logs.txt")).unwrap(),
        "log line\n".repeat(20_000).into_bytes()
    );
    // At-rest form of the compressible chunks is really smaller than raw.
    let mut framed_smaller = false;
    for h in client_store.hashes().unwrap() {
        let hex = h.to_string();
        let on_disk = store_dir
            .path()
            .join("blake3")
            .join(&hex[..2])
            .join(&hex)
            .metadata()
            .unwrap()
            .len();
        let raw = client_store.get(&h).unwrap().len() as u64;
        if on_disk < raw {
            framed_smaller = true;
        }
    }
    assert!(framed_smaller, "some chunk must be stored compressed");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
