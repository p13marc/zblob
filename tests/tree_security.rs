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
    TreeClient::builder(
        session,
        common::query(store_prefix),
        common::query(tree_prefix),
    )
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
                // Real servers frame chunks in a container (0x00 = raw); an
                // unframed reply would be rejected before materialization and
                // the test would pass for the wrong reason.
                let mut framed = vec![0u8];
                framed.extend_from_slice(bytes);
                let _ = query
                    .reply(key.clone(), framed)
                    .encoding(wire::ENC_CHUNK)
                    .await;
            }
        }
    })
}

/// Build a syntactically valid index around the given entries.
fn index_for(id: &str, entries: Vec<Entry>) -> TreeIndex {
    let mut idx = TreeIndex {
        version: wire::WIRE_VERSION,
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

    let outer_abs = tempfile::tempdir().unwrap();
    let abs_escape = outer_abs.path().join("evil.txt");
    let escape_paths = [
        "../evil.txt".to_string(),
        abs_escape.to_str().unwrap().to_string(),
        "a/../../evil.txt".to_string(),
    ];
    for (i, path) in escape_paths.iter().enumerate() {
        let id = format!("slip{i}");
        let index = index_for(
            &id,
            vec![Entry::File {
                path: path.clone(),
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
            matches!(err, BlobError::UnsafePath(_)),
            "path {path:?}: {err}"
        );
        assert!(
            !outer.path().join("evil.txt").exists() && !abs_escape.exists(),
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
    assert!(matches!(err, BlobError::UnsafePath(_)), "{err}");

    srv.abort();
    session.close().await.unwrap();
}

/// A *chain* of symlinks declared by one index escapes the root even though
/// every link passes the per-link lexical depth check. This is what
/// `assert_symlinks_confined` exists for.
///
/// Asserted against `validate()` directly: that is the attacker-input boundary
/// the whole tier depends on, so testing it there is testing the defence
/// rather than one path that happens to reach it.
#[test]
fn symlink_chain_escape_rejected() {
    // `sub/link` -> ".." resolves to the tree root, so `sub/link/..` is the
    // root's parent. The lexical walk for `e` never goes negative:
    //   sub(1) link(2) ..(1) ..(0) etc(1) passwd(2)
    let hostile = index_for(
        "chain",
        vec![
            Entry::Dir {
                path: "sub".into(),
                mode: 0o755,
                mtime: 0,
            },
            Entry::Symlink {
                path: "sub/link".into(),
                target: "..".into(),
            },
            Entry::Symlink {
                path: "e".into(),
                target: "sub/link/../../etc/passwd".into(),
            },
        ],
    );
    let err = hostile
        .validate()
        .expect_err("a symlink chain out of the root must be rejected");
    assert!(
        format!("{err}").contains("outside the tree root"),
        "wrong diagnosis: {err}"
    );

    // Discriminating power: the same *shape* — a link through a link, with
    // parent traversal — is legitimate as long as it lands inside. If this
    // fails, the check above is just banning symlinks and proves nothing.
    let honest = index_for(
        "chain-ok",
        vec![
            Entry::Dir {
                path: "sub".into(),
                mode: 0o755,
                mtime: 0,
            },
            Entry::Dir {
                path: "data".into(),
                mode: 0o755,
                mtime: 0,
            },
            Entry::Symlink {
                path: "sub/link".into(),
                target: "..".into(),
            },
            Entry::Symlink {
                path: "e".into(),
                target: "sub/link/data".into(),
            },
        ],
    );
    honest
        .validate()
        .expect("a chain that stays inside the root must still be accepted");
}

/// Mutually-referential symlinks terminate with an error instead of looping
/// forever — resolution is bounded, like the kernel's own `ELOOP`.
#[test]
fn symlink_cycle_terminates() {
    let cyclic = index_for(
        "cycle",
        vec![
            Entry::Symlink {
                path: "a".into(),
                target: "b".into(),
            },
            Entry::Symlink {
                path: "b".into(),
                target: "a".into(),
            },
        ],
    );
    let err = cyclic.validate().expect_err("a symlink cycle must error");
    assert!(
        format!("{err}").contains("hops"),
        "should report the hop budget: {err}"
    );
}

/// Two entries claiming one path let a snapshot destroy its own output — and,
/// with a `Dir` first, turn a directory into a file.
#[test]
fn duplicate_entry_paths_rejected() {
    let dup = index_for(
        "dup",
        vec![
            Entry::Dir {
                path: "x".into(),
                mode: 0o755,
                mtime: 0,
            },
            Entry::Symlink {
                path: "x".into(), // the same path, claimed twice
                target: "y".into(),
            },
        ],
    );
    let err = dup
        .validate()
        .expect_err("duplicate entry paths must be rejected");
    assert!(
        format!("{err}").contains("duplicate entry path"),
        "wrong diagnosis: {err}"
    );

    // Discriminating power: distinct paths that merely share a prefix are fine.
    index_for(
        "nodup",
        vec![
            Entry::Dir {
                path: "x".into(),
                mode: 0o755,
                mtime: 0,
            },
            Entry::Symlink {
                path: "x/y".into(),
                target: "z".into(),
            },
        ],
    )
    .validate()
    .expect("distinct paths sharing a prefix must be accepted");
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
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
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
    let client = TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(tree_prefix),
    )
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

/// An index entry that lands on an existing *directory* must not silently
/// `rm -rf` it. Materialization is in place, so the destination legitimately
/// holds data the caller cares about; deleting a subtree is a decision the
/// caller makes, not one an index makes for them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_directory_is_not_silently_destroyed() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let payload = b"replacement".to_vec();
    let payload_hash = Hash::of(&payload);
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &payload_hash);
    // A file entry named exactly like a directory the caller already has.
    let index = index_for(
        "clobber",
        vec![Entry::File {
            path: "Documents".into(),
            mode: 0o644,
            mtime: 0,
            size: payload.len() as u64,
            chunks: vec![zblob::ChunkRef {
                hash: payload_hash,
                len: payload.len() as u32,
            }],
        }],
    );
    let srv = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "clobber",
        wire::encode(&index).unwrap(),
        vec![(chunk_key, payload.clone())],
    )
    .await;

    // A destination that already holds something valuable.
    let dest = tempfile::tempdir().unwrap();
    std::fs::create_dir(dest.path().join("Documents")).unwrap();
    std::fs::write(dest.path().join("Documents/thesis.txt"), b"years of work").unwrap();

    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let err = test_client(session.clone(), &store_prefix, &tree_prefix)
        .download_tree(
            &DownloadRequest::new("clobber"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("replacing a directory must be refused by default");
    assert!(matches!(err, BlobError::UnsafePath(_)), "{err}");
    assert_eq!(
        std::fs::read(dest.path().join("Documents/thesis.txt")).unwrap(),
        b"years of work",
        "the pre-existing subtree must survive"
    );

    // Discriminating power: the same index succeeds once the caller opts in.
    // Without this, the assertion above would also pass if download_tree were
    // simply broken.
    let dest2 = tempfile::tempdir().unwrap();
    std::fs::create_dir(dest2.path().join("Documents")).unwrap();
    std::fs::write(dest2.path().join("Documents/thesis.txt"), b"years of work").unwrap();
    let permissive = TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(3))
    .materialize_policy(zblob::MaterializePolicy::default().replace_directories(true))
    .build();
    let store2: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    permissive
        .download_tree(
            &DownloadRequest::new("clobber"),
            dest2.path(),
            &store2,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("an opted-in caller may replace the directory");
    assert_eq!(
        std::fs::read(dest2.path().join("Documents")).unwrap(),
        payload
    );

    srv.abort();
    session.close().await.unwrap();
}

/// Mode bits come off the wire, so setuid/setgid/sticky are masked unless the
/// caller explicitly asks to restore them (tar and rsync gate this the same
/// way). Without the mask, a privileged extraction of an index with
/// attacker-chosen content yields a setuid-root binary.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn setid_bits_are_masked_unless_requested() {
    use std::os::unix::fs::PermissionsExt;

    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let payload = b"#!/bin/sh\nid\n".to_vec();
    let payload_hash = Hash::of(&payload);
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &payload_hash);
    let index = index_for(
        "setuid",
        vec![Entry::File {
            path: "rooted".into(),
            mode: 0o104755, // regular file, setuid, rwxr-xr-x
            mtime: 0,
            size: payload.len() as u64,
            chunks: vec![zblob::ChunkRef {
                hash: payload_hash,
                len: payload.len() as u32,
            }],
        }],
    );
    let srv = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "setuid",
        wire::encode(&index).unwrap(),
        vec![(chunk_key, payload.clone())],
    )
    .await;

    let dest = tempfile::tempdir().unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    test_client(session.clone(), &store_prefix, &tree_prefix)
        .download_tree(
            &DownloadRequest::new("setuid"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("the snapshot itself is legitimate; only the bit is refused");
    let mode = std::fs::metadata(dest.path().join("rooted"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o7000, 0, "setuid/setgid/sticky must be masked");
    assert_eq!(mode & 0o777, 0o755, "ordinary permissions must survive");

    // Discriminating power: with the opt-in, the bit is restored — so the
    // assertion above is about the mask, not about set_mode being a no-op.
    let dest2 = tempfile::tempdir().unwrap();
    let store2: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(3))
    .materialize_policy(zblob::MaterializePolicy::default().restore_setid(true))
    .build()
    .download_tree(
        &DownloadRequest::new("setuid"),
        dest2.path(),
        &store2,
        &(),
        &CancelToken::new(),
    )
    .await
    .expect("opted-in restore");
    let mode2 = std::fs::metadata(dest2.path().join("rooted"))
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(
        mode2 & 0o4000,
        0o4000,
        "opt-in must actually restore setuid"
    );

    srv.abort();
    session.close().await.unwrap();
}

/// A hostile index cannot launder a pre-existing outward symlink into a hard
/// link to data outside the root, nor create directories through it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn preexisting_symlink_cannot_be_traversed() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    // The secret outside the destination root.
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(outside.path().join("secret.pem"), b"KEY MATERIAL").unwrap();

    let payload = b"attacker data".to_vec();
    let payload_hash = Hash::of(&payload);
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &payload_hash);

    // Case A: hardlink whose target path traverses the pre-existing symlink.
    let hardlink_index = index_for(
        "hl-escape",
        vec![Entry::Hardlink {
            path: "loot".into(),
            target: "cache/secret.pem".into(),
        }],
    );
    // Case B: a directory entry that would be created *through* the symlink.
    let dir_index = index_for(
        "dir-escape",
        vec![
            Entry::Dir {
                path: "cache/spill".into(),
                mode: 0,
                mtime: 0,
            },
            Entry::File {
                path: "cache/spill/owned.txt".into(),
                mode: 0,
                mtime: 0,
                size: payload.len() as u64,
                chunks: vec![zblob::ChunkRef {
                    hash: payload_hash,
                    len: payload.len() as u32,
                }],
            },
        ],
    );
    let srv_a = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "hl-escape",
        wire::encode(&hardlink_index).unwrap(),
        vec![],
    )
    .await;

    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());

    for (id, srv) in [("hl-escape", Some(srv_a)), ("dir-escape", None)] {
        let srv = match srv {
            Some(s) => s,
            None => {
                fake_tree_server(
                    session.clone(),
                    tree_prefix.clone(),
                    "dir-escape",
                    wire::encode(&dir_index).unwrap(),
                    vec![(chunk_key.clone(), payload.clone())],
                )
                .await
            }
        };
        // Destination with a pre-existing symlink pointing outside.
        let dest = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), dest.path().join("cache")).unwrap();

        let err = client
            .download_tree(
                &DownloadRequest::new(id),
                dest.path(),
                &store,
                &(),
                &CancelToken::new(),
            )
            .await
            .expect_err("symlink traversal must be refused");
        assert!(matches!(err, BlobError::UnsafePath(_)), "{id}: {err}");
        // Nothing landed outside; nothing was created through the link.
        assert!(!outside.path().join("spill").exists());
        assert!(!outside.path().join("owned.txt").exists());
        assert!(!dest.path().join("loot").exists());
        srv.abort();
    }

    session.close().await.unwrap();
}

/// A holder that answers a small chunk's key with a very large payload must
/// not be able to choose our allocation, nor to deny a fetch an honest holder
/// is answering. The index declares each chunk's length and `validate()` has
/// already capped it, so an over-long reply is refusable before it is unframed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_over_long_chunk_reply_is_skipped_not_fatal() {
    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let payload = b"small".to_vec();
    let payload_hash = Hash::of(&payload);
    let chunk_key = zblob::store_key(&store_prefix, Hash::ALGO, &payload_hash);
    let index = index_for(
        "bloat",
        vec![Entry::File {
            path: "f.bin".into(),
            mode: 0o644,
            mtime: 0,
            size: payload.len() as u64,
            chunks: vec![zblob::ChunkRef {
                hash: payload_hash,
                len: payload.len() as u32,
            }],
        }],
    );

    // The hostile holder serves the index and a megabytes-long body under the
    // five-byte chunk's key…
    let hostile = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "bloat",
        wire::encode(&index).unwrap(),
        vec![(chunk_key.clone(), vec![0xAAu8; 4 * 1024 * 1024])],
    )
    .await;
    // …while an honest one serves the real bytes.
    let honest = fake_tree_server(
        session.clone(),
        tree_prefix.clone(),
        "bloat",
        wire::encode(&index).unwrap(),
        vec![(chunk_key, payload.clone())],
    )
    .await;

    let dest = tempfile::tempdir().unwrap();
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    test_client(session.clone(), &store_prefix, &tree_prefix)
        .download_tree(
            &DownloadRequest::new("bloat"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("an honest holder is answering; the fetch must complete");
    assert_eq!(std::fs::read(dest.path().join("f.bin")).unwrap(), payload);

    hostile.abort();
    honest.abort();
    session.close().await.unwrap();
}

/// A source tree containing an absolute symlink must fail at *build* time.
///
/// Every consumer validates on receipt and rejects such an index, so without
/// this the snapshot builds, registers and publishes without complaint and is
/// then unusable everywhere — with the error surfacing on machines that did
/// not build it. Same shape as the leading-`@` id rule for tier 1.
#[cfg(unix)]
#[test]
fn build_tree_refuses_a_snapshot_no_client_would_accept() {
    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("ok.txt"), b"fine").unwrap();
    std::os::unix::fs::symlink("/etc/localtime", src.path().join("tz")).unwrap();

    let store = MemoryStore::new();
    let err = build_tree(src.path(), "abs", &small_cdc(), &store)
        .expect_err("an absolute symlink must be refused at build time");
    assert!(
        format!("{err}").contains("absolute symlink target"),
        "wrong diagnosis: {err}"
    );

    // Discriminating power: the same tree without the offending link builds,
    // and what it builds validates.
    std::fs::remove_file(src.path().join("tz")).unwrap();
    std::os::unix::fs::symlink("ok.txt", src.path().join("rel")).unwrap();
    let index =
        build_tree(src.path(), "rel", &small_cdc(), &store).expect("a relative link is fine");
    index
        .validate()
        .expect("build_tree output must always validate");
}

/// Chunks are verified when fetched, but they are then read back out of a
/// `ContentStore` to be written — so a store that corrupts them in between, or
/// an implementation that breaks its contract, must not materialize wrong
/// bytes under a snapshot the caller believes is verified. `root_hash` cannot
/// catch this: it covers the entry list, not chunk contents.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_store_cannot_materialize_wrong_bytes() {
    /// A store that hands back something other than what was put in.
    struct LyingStore {
        inner: MemoryStore,
        corrupt: Hash,
    }
    impl ContentStore for LyingStore {
        fn has(&self, hash: &Hash) -> std::io::Result<bool> {
            self.inner.has(hash)
        }
        fn get(&self, hash: &Hash) -> std::io::Result<Option<Vec<u8>>> {
            let Some(bytes) = self.inner.get(hash)? else {
                return Ok(None);
            };
            if *hash == self.corrupt {
                // Same length, different content — so only a hash check finds it.
                return Ok(Some(vec![0xFFu8; bytes.len()]));
            }
            Ok(Some(bytes))
        }
        fn put(&self, hash: &Hash, bytes: &[u8]) -> std::io::Result<()> {
            self.inner.put(hash, bytes)
        }
        fn for_each_hash(
            &self,
            f: &mut dyn FnMut(Hash) -> std::io::Result<()>,
        ) -> std::io::Result<()> {
            self.inner.for_each_hash(f)
        }
        fn remove(&self, hash: &Hash) -> std::io::Result<bool> {
            self.inner.remove(hash)
        }
    }

    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    let body = common::pseudo_random(40_000, 11);
    std::fs::write(src.path().join("payload.bin"), &body).unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "rot", &small_cdc(), &*server_store).unwrap();
    let victim = index.needed_chunks()[0];
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let dest = tempfile::tempdir().unwrap();
    let lying: Arc<dyn ContentStore> = Arc::new(LyingStore {
        inner: MemoryStore::new(),
        corrupt: victim,
    });
    let err = client
        .download_tree(
            &DownloadRequest::pinned("rot", index.root_hash),
            dest.path(),
            &lying,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("a corrupt store must not produce a 'verified' tree");
    assert!(matches!(err, BlobError::CorruptStore { .. }), "{err}");

    // Discriminating power: an honest store materializes the same snapshot.
    let dest2 = tempfile::tempdir().unwrap();
    let honest: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    client
        .download_tree(
            &DownloadRequest::pinned("rot", index.root_hash),
            dest2.path(),
            &honest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("an honest store must still work");
    assert_eq!(
        std::fs::read(dest2.path().join("payload.bin")).unwrap(),
        body
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
