//! Tier-2 directory transfer over a loopback Zenoh session: build a tree, serve
//! its chunks + index, download into an empty store, and verify byte-for-byte.
//! Then prove the casync properties — re-pull after an edit transfers only the
//! changed chunks, and an interrupted pull resumes from the on-disk store.

mod common;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use common::{open_session, unique_prefix};
use zblob::{
    BlobError, CancelToken, CdcParams, ContentStore, DirStore, DownloadRequest, Entry, MemoryStore,
    Progress, ProgressSink, TreeClient, TreeServer, build_tree,
};

/// Small CDC parameters so the test fixtures span multiple chunks.
fn small_cdc() -> CdcParams {
    CdcParams {
        min: 2048,
        avg: 8192,
        max: 32768,
        normalization: 2,
        gear_seed: 0,
    }
}

/// Populate a temp directory tree: a nested dir, two files (one large enough to
/// span several chunks), and — on unix — a symlink.
fn make_tree(root: &std::path::Path) {
    std::fs::create_dir_all(root.join("sub/deep")).unwrap();
    let big = common::pseudo_random(100_000, 42);
    std::fs::write(root.join("big.bin"), &big).unwrap();
    std::fs::write(root.join("sub/hello.txt"), b"hello world").unwrap();
    std::fs::write(root.join("sub/deep/note.md"), b"# note\n").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink("hello.txt", root.join("sub/link")).unwrap();
}

/// Recursively compare two directory trees for byte-identical content +
/// structure, and (unix) equal modes.
fn assert_dirs_equal(a: &std::path::Path, b: &std::path::Path) {
    let mut ea: Vec<_> = std::fs::read_dir(a)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    let mut eb: Vec<_> = std::fs::read_dir(b)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    ea.sort();
    eb.sort();
    assert_eq!(ea, eb, "entry names differ in {a:?} vs {b:?}");
    for name in ea {
        let pa = a.join(&name);
        let pb = b.join(&name);
        let ma = std::fs::symlink_metadata(&pa).unwrap();
        if ma.file_type().is_symlink() {
            assert_eq!(
                std::fs::read_link(&pa).unwrap(),
                std::fs::read_link(&pb).unwrap()
            );
        } else if ma.is_dir() {
            assert_dirs_equal(&pa, &pb);
        } else {
            assert_eq!(
                std::fs::read(&pa).unwrap(),
                std::fs::read(&pb).unwrap(),
                "{name:?}"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mb = std::fs::symlink_metadata(&pb).unwrap();
                assert_eq!(
                    ma.permissions().mode(),
                    mb.permissions().mode(),
                    "mode differs for {name:?}"
                );
            }
        }
    }
}

fn test_client(session: Arc<zenoh::Session>, store_prefix: &str, tree_prefix: &str) -> TreeClient {
    TreeClient::builder(
        session,
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(5))
    .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tree_roundtrip_with_modes_and_mtime() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            src.path().join("big.bin"),
            std::fs::Permissions::from_mode(0o750),
        )
        .unwrap();
    }

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap1", &small_cdc(), &*server_store).unwrap();
    let n_chunks = index.needed_chunks().len();
    assert!(n_chunks > 3, "fixture should span several chunks");
    let expected_root = index.root_hash;

    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // Download (pinned) into an empty client store + fresh dest dir.
    let client_dir = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    tokio::time::timeout(
        Duration::from_secs(20),
        client.download_tree(
            &DownloadRequest::pinned("snap1", expected_root),
            client_dir.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("download tree");

    assert_dirs_equal(src.path(), client_dir.path());
    assert_eq!(client_store.hashes().unwrap().len(), n_chunks);

    // mtime restored (within fs precision).
    let src_mtime = std::fs::metadata(src.path().join("big.bin"))
        .unwrap()
        .modified()
        .unwrap();
    let dst_mtime = std::fs::metadata(client_dir.path().join("big.bin"))
        .unwrap()
        .modified()
        .unwrap();
    let drift = src_mtime
        .duration_since(dst_mtime)
        .unwrap_or_else(|e| e.duration());
    assert!(
        drift <= Duration::from_secs(1),
        "mtime restored ({drift:?})"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reedit_transfers_only_changed_chunks() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());

    // Snapshot 1 → client store now holds every chunk.
    let server_store_mem = Arc::new(MemoryStore::new());
    let server_store: Arc<dyn ContentStore> = server_store_mem.clone();
    let index1 = build_tree(src.path(), "snap1", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store.clone(),
    );
    server.register(index1).await.unwrap();
    let handle = server.clone().spawn().await.unwrap();

    let client_dir = tempfile::tempdir().unwrap();
    // Persistent client store survives "across syncs" (DirStore on disk).
    let store_dir = tempfile::tempdir().unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(DirStore::open(store_dir.path()).unwrap());
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    client
        .download_tree(
            &DownloadRequest::new("snap1"),
            client_dir.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .unwrap();
    let after_first = client_store.hashes().unwrap().len();

    // Edit one small file → only its chunk(s) change.
    std::fs::write(src.path().join("sub/hello.txt"), b"hello CHANGED world").unwrap();
    let index2 = build_tree(src.path(), "snap2", &small_cdc(), &*server_store).unwrap();
    server.register(index2.clone()).await.unwrap();

    client
        .download_tree(
            &DownloadRequest::new("snap2"),
            client_dir.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .unwrap();
    let after_second = client_store.hashes().unwrap().len();

    // The re-pull added exactly the one new (changed) chunk to the store.
    assert_eq!(
        after_second - after_first,
        1,
        "re-pull should transfer only the single changed chunk"
    );
    assert_eq!(
        std::fs::read(client_dir.path().join("sub/hello.txt")).unwrap(),
        b"hello CHANGED world"
    );
    // The unchanged big file still spans several reused chunks.
    let big_entry_chunks = index2
        .entries
        .iter()
        .find_map(|e| match e {
            Entry::File { path, chunks, .. } if path == "big.bin" => Some(chunks.len()),
            _ => None,
        })
        .unwrap();
    assert!(big_entry_chunks >= 3);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resume_from_prepopulated_store() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap1", &small_cdc(), &*server_store).unwrap();
    let total = index.needed_chunks().len();

    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store.clone(),
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // Simulate an interrupted earlier pull: half the chunks already on disk.
    let store_dir = tempfile::tempdir().unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(DirStore::open(store_dir.path()).unwrap());
    let needed = index.needed_chunks();
    for h in needed.iter().take(needed.len() / 2) {
        client_store
            .put(h, &server_store.get(h).unwrap().unwrap())
            .unwrap();
    }
    assert!(client_store.hashes().unwrap().len() < total);

    // Resume: download_tree fetches only the missing remainder.
    let client_dir = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    client
        .download_tree(
            &DownloadRequest::new("snap1"),
            client_dir.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .unwrap();

    assert_dirs_equal(src.path(), client_dir.path());
    assert_eq!(client_store.hashes().unwrap().len(), total);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A progress sink that records every event for assertions.
#[derive(Default)]
struct RecordingSink(Mutex<Vec<Progress>>);
impl ProgressSink for RecordingSink {
    fn emit(&self, p: Progress) {
        self.0.lock().unwrap().push(p);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellable_reports_progress_and_resumes() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "snap1", &small_cdc(), &*server_store).unwrap();
    let total = index.needed_chunks().len();
    assert!(
        total >= 4,
        "need several chunks to test a mid-stream cancel"
    );

    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // Serial, unbatched fetch so the cancel lands mid-stream deterministically.
    //
    // Batching changes cancellation granularity, and honestly so: chunks that
    // arrive together in one query round are stored together, so a cancel
    // observed during a round cannot un-fetch them. `batch_size(0)` selects
    // the per-chunk path, where a partial cancel is well-defined; the batched
    // path's cancellation is covered separately below.
    let client = TreeClient::builder(
        session.clone(),
        common::query(store_prefix.clone()),
        common::query(tree_prefix.clone()),
    )
    .query_timeout(Duration::from_secs(5))
    .fetch_concurrency(1)
    .batch_size(0)
    .build();

    // 1) Cancel after the first chunk: the call returns Cancelled and the store
    //    is left with whatever it fetched, so a resume can finish.
    let store_dir = tempfile::tempdir().unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(DirStore::open(store_dir.path()).unwrap());
    let cancel = CancelToken::new();
    {
        struct CancelAfterOne {
            cancel: CancelToken,
        }
        impl ProgressSink for CancelAfterOne {
            fn emit(&self, p: Progress) {
                if let Progress::Chunk { received, .. } = p
                    && received >= 1
                {
                    self.cancel.cancel();
                }
            }
        }
        let sink = CancelAfterOne {
            cancel: cancel.clone(),
        };
        let dest = tempfile::tempdir().unwrap();
        let err = client
            .download_tree(
                &DownloadRequest::new("snap1"),
                dest.path(),
                &client_store,
                &sink,
                &cancel,
            )
            .await
            .expect_err("cancelled mid-stream");
        match err {
            BlobError::Cancelled { received, total: t } => {
                assert!(received >= 1 && (received as usize) < total);
                assert_eq!(t as usize, total);
            }
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }
    let after_cancel = client_store.hashes().unwrap().len();
    assert!(after_cancel >= 1 && after_cancel < total, "partial on disk");

    // 2) Resume with a fresh token + recording sink: it completes, progress is
    //    monotonic up to `total`, and ends with Completed.
    let sink = RecordingSink::default();
    let dest = tempfile::tempdir().unwrap();
    client
        .download_tree(
            &DownloadRequest::new("snap1"),
            dest.path(),
            &client_store,
            &sink,
            &CancelToken::new(),
        )
        .await
        .expect("resume completes");
    assert_dirs_equal(src.path(), dest.path());

    let events: Vec<Progress> = sink.0.lock().unwrap().clone();
    let mut last = 0u32;
    let mut saw_complete = false;
    for ev in &events {
        match ev {
            Progress::Chunk {
                received, total: t, ..
            } => {
                assert!(*received >= last, "progress must be monotonic");
                last = *received;
                assert_eq!(*t as usize, total);
            }
            Progress::Completed { .. } => saw_complete = true,
            _ => {}
        }
    }
    assert_eq!(last as usize, total, "progress reaches total");
    assert!(saw_complete, "emits Completed");

    // 3) The batched path: cancellation is round-granular rather than
    //    chunk-granular, but the property that matters is unchanged — the call
    //    reports Cancelled, whatever it fetched is in the store, and calling
    //    again finishes the job.
    {
        let batched = TreeClient::builder(
            session.clone(),
            common::query(store_prefix),
            common::query(tree_prefix),
        )
        .query_timeout(Duration::from_secs(5))
        .build();
        let store_dir = tempfile::tempdir().unwrap();
        let fresh: Arc<dyn ContentStore> = Arc::new(DirStore::open(store_dir.path()).unwrap());
        let cancel = CancelToken::new();
        struct CancelAfterOne {
            cancel: CancelToken,
        }
        impl ProgressSink for CancelAfterOne {
            fn emit(&self, p: Progress) {
                if let Progress::Chunk { received, .. } = p
                    && received >= 1
                {
                    self.cancel.cancel();
                }
            }
        }
        let dest = tempfile::tempdir().unwrap();
        let err = batched
            .download_tree(
                &DownloadRequest::new("snap1"),
                dest.path(),
                &fresh,
                &CancelAfterOne {
                    cancel: cancel.clone(),
                },
                &cancel,
            )
            .await
            .expect_err("a cancel must surface even when a round resolved it all");
        assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");
        assert!(
            !fresh.hashes().unwrap().is_empty(),
            "a cancelled batch must leave what it fetched behind to resume from"
        );

        let dest2 = tempfile::tempdir().unwrap();
        batched
            .download_tree(
                &DownloadRequest::new("snap1"),
                dest2.path(),
                &fresh,
                &(),
                &CancelToken::new(),
            )
            .await
            .expect("resume after a batched cancel must complete");
        assert_dirs_equal(src.path(), dest2.path());
    }

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Hard links survive the round trip as hard links (same inode), not copies.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hardlinks_roundtrip() {
    use std::os::unix::fs::MetadataExt;

    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("orig.bin"), b"shared bytes").unwrap();
    std::fs::hard_link(src.path().join("orig.bin"), src.path().join("copy.bin")).unwrap();

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "hl", &small_cdc(), &*server_store).unwrap();
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
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    client
        .download_tree(
            &DownloadRequest::new("hl"),
            dest.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .unwrap();

    let a = std::fs::metadata(dest.path().join("orig.bin")).unwrap();
    let b = std::fs::metadata(dest.path().join("copy.bin")).unwrap();
    assert_eq!(a.ino(), b.ino(), "hard link must be reconstructed as one");
    assert_eq!(
        std::fs::read(dest.path().join("copy.bin")).unwrap(),
        b"shared bytes"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Empty files, empty directories, and an empty tree all round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_file_dir_and_tree_roundtrip() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(src.path().join("empty-dir")).unwrap();
    std::fs::write(src.path().join("empty-file"), b"").unwrap();
    std::fs::write(src.path().join("real"), b"content").unwrap();

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "edges", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store.clone(),
    );
    server.register(index).await.unwrap();

    // A fully-empty tree too.
    let empty_src = tempfile::tempdir().unwrap();
    let empty_index = build_tree(empty_src.path(), "void", &small_cdc(), &*server_store).unwrap();
    assert!(empty_index.entries.is_empty());
    server.register(empty_index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());

    let dest = tempfile::tempdir().unwrap();
    client
        .download_tree(
            &DownloadRequest::new("edges"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("edges tree");
    assert_dirs_equal(src.path(), dest.path());
    assert_eq!(std::fs::read(dest.path().join("empty-file")).unwrap(), b"");
    assert!(dest.path().join("empty-dir").is_dir());

    let void_dest = tempfile::tempdir().unwrap();
    client
        .download_tree(
            &DownloadRequest::new("void"),
            void_dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("empty tree");
    assert_eq!(std::fs::read_dir(void_dest.path()).unwrap().count(), 0);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A read-only directory must not block materializing its own children —
/// directory modes are restored last (the tar/casync ordering).
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readonly_dir_roundtrips_with_mode_restored() {
    use std::os::unix::fs::PermissionsExt;

    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("locked")).unwrap();
    std::fs::write(src.path().join("locked/inside.txt"), b"still writable").unwrap();
    std::fs::set_permissions(
        src.path().join("locked"),
        std::fs::Permissions::from_mode(0o555),
    )
    .unwrap();

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "ro", &small_cdc(), &*server_store).unwrap();
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
    client
        .download_tree(
            &DownloadRequest::new("ro"),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("read-only dir tree");
    assert_eq!(
        std::fs::read(dest.path().join("locked/inside.txt")).unwrap(),
        b"still writable"
    );
    let mode = std::fs::metadata(dest.path().join("locked"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o555, "read-only mode restored");

    // Restore writability so the tempdir can be cleaned up.
    std::fs::set_permissions(
        src.path().join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::set_permissions(
        dest.path().join("locked"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Two concurrent tree downloads sharing one DirStore (the v1 fixed-name
/// temp-file race) both succeed with identical results.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_tree_downloads_share_one_dirstore() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "shared", &small_cdc(), &*server_store).unwrap();
    let total = index.needed_chunks().len();
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let store_dir = tempfile::tempdir().unwrap();
    let shared_store: Arc<dyn ContentStore> = Arc::new(DirStore::open(store_dir.path()).unwrap());
    let client = Arc::new(test_client(session.clone(), &store_prefix, &tree_prefix));

    let mut joins = Vec::new();
    let dests: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
    for dest in &dests {
        let client = client.clone();
        let store = shared_store.clone();
        let path = dest.path().to_path_buf();
        joins.push(tokio::spawn(async move {
            client
                .download_tree(
                    &DownloadRequest::new("shared"),
                    &path,
                    &store,
                    &(),
                    &CancelToken::new(),
                )
                .await
        }));
    }
    for join in joins {
        join.await.unwrap().expect("concurrent tree download");
    }
    for dest in &dests {
        assert_dirs_equal(src.path(), dest.path());
    }
    assert_eq!(shared_store.hashes().unwrap().len(), total);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Non-UTF-8 file names are a loud build error, not silent mangling.
///
/// Only some unix filesystems permit such a name at all — APFS (macOS) and
/// Windows reject it at the OS layer — so the fixture is best-effort: where
/// the name cannot be created there is nothing for `build_tree` to mishandle
/// and the test has no subject.
#[cfg(unix)]
#[test]
fn non_utf8_names_error_on_build() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let src = tempfile::tempdir().unwrap();
    let bad = src.path().join(OsStr::from_bytes(b"bad-\xff-name"));
    if std::fs::write(&bad, b"data").is_err() {
        eprintln!("skipping: this filesystem refuses non-UTF-8 names");
        return;
    }
    let store = MemoryStore::new();
    let err = build_tree(src.path(), "bad", &small_cdc(), &store).expect_err("must error");
    assert!(err.to_string().contains("non-UTF-8"), "{err}");
}

/// The content-addressed snapshot shape: a tree re-keyed by its own root is
/// fetched with the root as both the key and the pin, so a substituted index
/// cannot even be requested — trust-on-first-use is not expressible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn content_addressed_trees_pin_by_construction() {
    let session = common::open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    make_tree(src.path());

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    // Build under a human name, then re-key by root — the id is not part of
    // the root digest, so the identity is unchanged by the rename.
    let named = build_tree(src.path(), "nightly", &small_cdc(), &*server_store).unwrap();
    let index = named.clone().keyed_by_root();
    assert_eq!(
        index.root_hash, named.root_hash,
        "re-keying must not alter identity"
    );
    assert!(index.is_content_addressed());
    assert!(!named.is_content_addressed());
    let root = index.root_hash;

    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // One value carries both the key and the pin.
    let dest = tempfile::tempdir().unwrap();
    let client = test_client(session.clone(), &store_prefix, &tree_prefix);
    let store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    client
        .download_tree(
            &DownloadRequest::by_root(root),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("content-addressed fetch");
    assert_dirs_equal(src.path(), dest.path());

    // Asking for a root nobody serves fails; it cannot silently resolve to
    // some other snapshot the way a name could.
    let other = zblob::Hash::of(b"a tree that does not exist here");
    let err = client
        .download_tree(
            &DownloadRequest::by_root(other),
            dest.path(),
            &store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("unknown root must not resolve");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A garbage collection running mid-download must not collect the chunks that
/// download has already fetched.
///
/// Progress in tier 2 *is* "which hashes are in the store", so a sweep sees
/// freshly-fetched chunks that no tagged snapshot references yet and takes
/// them for garbage — and the download then fails with `NotFound` for a chunk
/// it stored itself. `gc::TempTags` has always existed for this and had no
/// caller; giving the client the same registry the sweep uses connects them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sweep_cannot_collect_an_in_flight_download() {
    use std::sync::Arc as StdArc;
    use zblob::gc;

    let session = open_session().await;
    let p = unique_prefix();
    let store_prefix = format!("{p}/store");
    let tree_prefix = format!("{p}/tree");

    let src = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("a.bin"), common::pseudo_random(60_000, 61)).unwrap();
    std::fs::write(src.path().join("b.bin"), common::pseudo_random(60_000, 62)).unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "swept", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    // The client's store starts empty and holds nothing any tag references, so
    // an unprotected sweep would collect every chunk it fetches.
    let temps = StdArc::new(gc::TempTags::new());
    let tag_dir = tempfile::tempdir().unwrap();
    let tags = gc::SnapshotTags::open(tag_dir.path()).unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let client = TreeClient::builder(
        session.clone(),
        common::query(store_prefix),
        common::query(tree_prefix),
    )
    .query_timeout(Duration::from_secs(5))
    .temp_tags(temps.clone())
    .build();

    let dest = tempfile::tempdir().unwrap();
    // Sweep repeatedly while the download runs.
    let sweeper = {
        let store = client_store.clone();
        let temps = temps.clone();
        let tag_dir = tag_dir.path().to_path_buf();
        tokio::spawn(async move {
            let tags = gc::SnapshotTags::open(&tag_dir).unwrap();
            for _ in 0..40 {
                let _ = gc::sweep(&*store, &tags, &temps, []);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    };

    client
        .download_tree(
            &DownloadRequest::pinned("swept", index.root_hash),
            dest.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("a concurrent sweep must not break the download");
    sweeper.await.unwrap();

    assert_eq!(
        std::fs::read(dest.path().join("a.bin")).unwrap(),
        common::pseudo_random(60_000, 61)
    );

    // Discriminating power: the sweeps were real and effective. Once the
    // download released its temp tag and nothing is tagged, its chunks *are*
    // garbage and get collected — the protection is scoped to the transfer,
    // not permanent. If sweeping had been a no-op the store would still be
    // full here, and the assertion above would have proved nothing.
    let _ = gc::sweep(&*client_store, &tags, &temps, []).unwrap();
    assert!(
        client_store.hashes().unwrap().is_empty(),
        "an untagged store must be collectable once the download released its tag"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A wildcard-origin tier-2 prefix must be *answerable*, not merely
/// acceptable.
///
/// `TreeClient` validated its prefixes with the rule that permits a
/// single-segment origin wildcard, while the server resolved both index and
/// chunk keys by literal `strip_prefix`. So such a client passed validation
/// and then every query went unanswered by every server — a silent total
/// failure, and exactly the bug class `parse_id` was rewritten to fix for
/// tier 1. Either the shape works or it is refused; it must not validate and
/// then be unserviceable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wildcard_origin_tier2_prefix_is_answerable() {
    let session = open_session().await;
    let base = unique_prefix();
    // The server owns a concrete origin segment…
    let store_prefix = format!("{base}/host-a/store");
    let tree_prefix = format!("{base}/host-a/tree");
    // …and the client does not know which origin holds the snapshot.
    let store_query = format!("{base}/*/store");
    let tree_query = format!("{base}/*/tree");

    let src = tempfile::tempdir().unwrap();
    let body = common::pseudo_random(50_000, 71);
    std::fs::write(src.path().join("wide.bin"), &body).unwrap();
    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let index = build_tree(src.path(), "anyorigin", &small_cdc(), &*server_store).unwrap();
    let server = TreeServer::new(
        session.clone(),
        common::serve(store_prefix.clone()),
        common::serve(tree_prefix.clone()),
        server_store,
    );
    server.register(index.clone()).await.unwrap();
    let handle = server.spawn().await.unwrap();

    let dest = tempfile::tempdir().unwrap();
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    TreeClient::builder(
        session.clone(),
        common::query(store_query),
        common::query(tree_query),
    )
    .query_timeout(Duration::from_secs(5))
    .build()
    .download_tree(
        &DownloadRequest::pinned("anyorigin", index.root_hash),
        dest.path(),
        &client_store,
        &(),
        &CancelToken::new(),
    )
    .await
    .expect("a wildcard-origin prefix must reach the server that owns the id");
    assert_eq!(std::fs::read(dest.path().join("wide.bin")).unwrap(), body);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A snapshot taken against a parent reuses unchanged files without reading
/// them, and produces **the same root** a full build would.
///
/// The root equality is the property that matters: an incremental build is an
/// optimisation, and an optimisation that changes identity is a bug.
///
/// Proving the work was skipped needs care. Counting store writes proves
/// nothing — `walk` only writes chunks the store lacks, so a *full* rebuild
/// against a warm store writes almost nothing either. So the test instead
/// rewrites a file's contents while restoring its size and mtime: an
/// incremental build must then report the *old* chunks (it never opened the
/// file) and a full build the new ones. That is exactly the heuristic's
/// documented blind spot, used here as an instrument.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incremental_snapshot_reuses_unchanged_files_and_keeps_the_root() {
    let src = tempfile::tempdir().unwrap();
    for i in 0..6 {
        std::fs::write(
            src.path().join(format!("f{i}.bin")),
            common::pseudo_random(40_000, 300 + i as u64),
        )
        .unwrap();
    }
    let store = MemoryStore::new();
    let parent = zblob::build_tree(src.path(), "s1", &small_cdc(), &store).unwrap();

    // Rewrite f0's *contents* but put its size and mtime back, so the
    // heuristic cannot tell it changed.
    let f0 = src.path().join("f0.bin");
    let before = std::fs::metadata(&f0).unwrap().modified().unwrap();
    let disguised = common::pseudo_random(40_000, 777);
    std::fs::write(&f0, &disguised).unwrap();
    std::fs::File::options()
        .write(true)
        .open(&f0)
        .unwrap()
        .set_modified(before)
        .unwrap();

    let entry_of = |idx: &zblob::TreeIndex, path: &str| {
        idx.entries
            .iter()
            .find(|e| e.path() == path)
            .cloned()
            .expect("entry present")
    };

    let incremental =
        zblob::build_tree_from(src.path(), "s2", &small_cdc(), &store, Some(&parent)).unwrap();
    assert_eq!(
        entry_of(&incremental, "f0.bin"),
        entry_of(&parent, "f0.bin"),
        "an unchanged (size, mtime) file must be reused without being read"
    );

    // Discriminating power: a full build *does* read it and sees the change.
    let full = zblob::build_tree(src.path(), "s2", &small_cdc(), &MemoryStore::new()).unwrap();
    assert_ne!(
        entry_of(&full, "f0.bin"),
        entry_of(&parent, "f0.bin"),
        "the file really did change on disk"
    );

    // Now a genuine change, mtime and all: the incremental build must agree
    // with a full one, byte for byte and root for root.
    std::fs::write(
        src.path().join("f3.bin"),
        common::pseudo_random(41_000, 999),
    )
    .unwrap();
    let fresh = MemoryStore::new();
    let base = zblob::build_tree(src.path(), "s3", &small_cdc(), &fresh).unwrap();
    let inc = zblob::build_tree_from(src.path(), "s3", &small_cdc(), &fresh, Some(&base)).unwrap();
    assert_eq!(
        inc.root_hash, base.root_hash,
        "an incremental build must produce the same identity as a full one"
    );
    assert_eq!(inc.entries, base.entries);

    // A parent cut with different CDC parameters is refused rather than
    // silently producing an index whose chunks do not tile.
    let other_cdc = CdcParams {
        min: 4096,
        avg: 16384,
        max: 65536,
        normalization: 2,
        gear_seed: 7,
    };
    let err = zblob::build_tree_from(src.path(), "s4", &other_cdc, &fresh, Some(&base))
        .expect_err("a mismatched parent must be refused");
    assert!(
        format!("{err}").contains("CDC parameters"),
        "wrong diagnosis: {err}"
    );

    // A parent whose chunks are gone falls back to re-chunking rather than
    // producing an index nobody can fetch.
    let empty = MemoryStore::new();
    let rebuilt =
        zblob::build_tree_from(src.path(), "s5", &small_cdc(), &empty, Some(&base)).unwrap();
    assert_eq!(rebuilt.root_hash, base.root_hash);
    assert_eq!(
        empty.hashes().unwrap().len(),
        base.needed_chunks().len(),
        "every chunk had to be re-made because none were present"
    );
}
