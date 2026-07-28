//! Adversarial Tier-2: a hostile index cannot escape the destination root
//! (zip-slip), a forged root or substituted snapshot is rejected before
//! materialization, and a wrong-content chunk reply is ignored.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{open_session, unique_prefix};
use zblob::{
    BlobError, CancelToken, CdcParams, ContentStore, DownloadRequest, Entry, Hash, MemoryStore,
    TreeClient, TreeIndex, TreeServer, build_tree, wire,
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

fn test_client(session: Arc<zenoh::Session>, store_prefix: &str, tree_prefix: &str) -> TreeClient {
    TreeClient::builder(session, store_prefix, tree_prefix)
        .query_timeout(Duration::from_secs(3))
        .build()
}

/// Serve a hand-crafted (possibly malicious) index + chunk set.
async fn fake_tree_server(
    session: Arc<zenoh::Session>,
    tree_prefix: String,
    id: &str,
    index_payload: Vec<u8>,
    chunks: Vec<(String, Vec<u8>)>, // (full key, bytes)
) -> tokio::task::JoinHandle<()> {
    let index_key = format!("{tree_prefix}/{id}");
    let root: String = tree_prefix.rsplit_once('/').unwrap().0.to_string();
    let q = session
        .declare_queryable(format!("{root}/**"))
        .await
        .unwrap();
    tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let key = query.key_expr().as_str().to_string();
            if key == index_key {
                let _ = query
                    .reply(key.clone(), index_payload.clone())
                    .encoding(wire::ENC_INDEX)
                    .await;
                continue;
            }
            if let Some((_, bytes)) = chunks.iter().find(|(k, _)| *k == key) {
                let _ = query
                    .reply(key.clone(), bytes.clone())
                    .encoding(wire::ENC_CHUNK)
                    .await;
            }
        }
    })
}

/// Build a syntactically valid index around the given entries.
fn index_for(id: &str, entries: Vec<Entry>) -> TreeIndex {
    let mut idx = TreeIndex {
        version: 2,
        id: id.into(),
        algo: Hash::ALGO.into(),
        cdc: small_cdc(),
        entries,
        root_hash: Hash::of(b"placeholder"),
    };
    idx.root_hash = idx.compute_root().unwrap();
    idx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zip_slip_index_rejected_nothing_written() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    // A chunk of "evil" content, referenced by entries that try to escape.
    let evil = b"owned".to_vec();
    let evil_hash = Hash::of(&evil);
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &evil_hash);

    let escape_paths = ["../evil.txt", "/tmp/evil.txt", "a/../../evil.txt"];
    for (i, path) in escape_paths.iter().enumerate() {
        let id = format!("slip{i}");
        let index = index_for(
            &id,
            vec![Entry::File {
                path: (*path).into(),
                mode: 0,
                mtime: 0,
                size: evil.len() as u64,
                chunks: vec![zblob::ChunkRef {
                    hash: evil_hash,
                    len: evil.len() as u32,
                }],
            }],
        );
        let srv = fake_tree_server(
            session.clone(),
            tree_prefix.clone(),
            &id,
            wire::encode(&index).unwrap(),
            vec![(chunk_key.clone(), evil.clone())],
        )
        .await;

        // The parent of dest_root must stay untouched.
        let outer = tempfile::tempdir().unwrap();
        let dest = outer.path().join("dest");
        let client = test_client(session.clone(), &store_prefix, &tree_prefix);
        let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
        let err = client
            .download_tree(
                &DownloadRequest::new(&id),
                &dest,
                &store,
                &(),
                &CancelToken::new(),
            )
            .await
            .expect_err("zip-slip must be rejected");
        assert!(
            matches!(err, BlobError::Protocol(_)),
            "path {path:?}: {err}"
        );
        assert!(
            !outer.path().join("evil.txt").exists()
                && !std::path::Path::new("/tmp/evil.txt").exists(),
            "escape target must not exist for {path:?}"
        );
        srv.abort();
    }
    session.close().await.unwrap();
}

/// A symlink whose target escapes the root is rejected even though the link
/// itself lives inside.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escaping_symlink_target_rejected() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let index = index_for(
        "sym",
        vec![Entry::Symlink {
            path: "innocent".into(),
            target: "../../secrets".into(),
        }],
    );
    let srv = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "sym",
        wire::encode(&index).unwrap(),
        vec![],
    )
    .await;

    let dest = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = client
        .download_tree(
            &DownloadRequest::new("sym"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("escaping symlink must be rejected");
    assert!(matches!(err, BlobError::Protocol(_)), "{err}");

    srv.abort();
    session.close().await.unwrap();
}

/// An index whose root_hash doesn't match its entries is rejected at fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forged_root_hash_rejected() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let mut index = index_for(
        "forged",
        vec![Entry::Dir {
            path: "d".into(),
            mode: 0,
            mtime: 0,
        }],
    );
    index.root_hash = Hash::of(b"forged root"); // break entries↔root binding
    let srv = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "forged",
        wire::encode(&index).unwrap(),
        vec![],
    )
    .await;

    let dest = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = client
        .download_tree(
            &DownloadRequest::new("forged"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("forged root must be rejected");
    assert!(matches!(err, BlobError::RootMismatch { .. }), "{err}");
    assert!(!dest.path().join("d").exists(), "nothing materialized");

    srv.abort();
    session.close().await.unwrap();
}

/// A pinned expected_root rejects an honest-but-different snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_tree_root_rejects_substitution() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("f.txt"), b"substituted contents").unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "pinned", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        session.clone(),
        store_prefix.clone(),
        tree_prefix.clone(),
        server_store,
    );
    server.register(index).await;
    let handle = server.spawn().await.unwrap();

    let dest = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = client
        .download_tree(
            &DownloadRequest::pinned("pinned", Hash::of(b"the tree I actually wanted")),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("wrong pin must fail");
    assert!(matches!(err, BlobError::RootMismatch { .. }), "{err}");
    assert!(!dest.path().join("f.txt").exists());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A server replying wrong bytes for a chunk key: the reply is ignored
/// (re-hash mismatch) and the download times out incomplete instead of
/// materializing corruption.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_content_chunk_ignored() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let good = b"the real chunk contents".to_vec();
    let good_hash = Hash::of(&good);
    let index = index_for(
        "wrongchunk",
        vec![Entry::File {
            path: "f.bin".into(),
            mode: 0,
            mtime: 0,
            size: good.len() as u64,
            chunks: vec![zblob::ChunkRef {
                hash: good_hash,
                len: good.len() as u32,
            }],
        }],
    );
    // Serve *corrupted* bytes under the good hash's key.
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &good_hash);
    let srv = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "wrongchunk",
        wire::encode(&index).unwrap(),
        vec![(chunk_key, b"corrupted!".to_vec())],
    )
    .await;

    let dest = tempfile::tempdir().unwrap();
    let client = TreeClient::builder(session.clone(), &store_prefix, &tree_prefix)
        .query_timeout(Duration::from_secs(2))
        .build();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = client
        .download_tree(
            &DownloadRequest::new("wrongchunk"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("corrupt chunk must not complete");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");
    assert!(!dest.path().join("f.bin").exists());
    assert!(store.hashes().unwrap().is_empty(), "nothing stored");

    srv.abort();
    session.close().await.unwrap();
}
