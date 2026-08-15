//! Push (upload) protocol: authorized verified uploads land, get registered,
//! and are immediately downloadable; unauthorized ones are refused; an
//! interrupted upload resumes from the server's spool.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    Manifest, MemoryBlobSource, Overwrite, Progress, ProgressSink, PushConfig, PushPolicy,
    RetryPolicy,
};

/// Allows pushes carrying the byte token `"secret"`.
struct TokenPolicy;
impl PushPolicy for TokenPolicy {
    fn allow(&self, _manifest: &Manifest, token: Option<&[u8]>) -> bool {
        token == Some(b"secret")
    }
}

fn test_client(session: &zenoh::Session, prefix: &str) -> BlobClient {
    BlobClient::builder(session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        })
        .overwrite(Overwrite::Replace)
        .build()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_push_lands_and_serves() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    // Uploader-side source file: multi-chunk with a short tail.
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3 + 777, 11);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(&session, &prefix);
    let manifest = tokio::time::timeout(
        Duration::from_secs(20),
        client
            .upload_file(
                BlobSpec::new("pushed").chunk_size(MIN_CHUNK_SIZE),
                &src_path,
            )
            .token(b"secret".to_vec()),
    )
    .await
    .expect("timed out")
    .expect("upload");
    assert_eq!(manifest.root, content_hash(&data));

    // The receiver now serves the blob: download it back, pinned.
    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("down.bin");
    client
        .download_to(&DownloadRequest::pinned("pushed", manifest.root), &dest)
        .await
        .expect("download pushed blob");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unauthorized_or_unconfigured_push_denied() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize, 12);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(&session, &prefix);
    // Wrong token → denied by policy.
    let err = client
        .upload_file(BlobSpec::new("nope").chunk_size(MIN_CHUNK_SIZE), &src_path)
        .token(b"wrong".to_vec())
        .await
        .expect_err("must be denied");
    assert!(matches!(err, BlobError::PushDenied(_)), "{err}");
    // Nothing was registered.
    let err = client.fetch_manifest("nope").await.expect_err("no blob");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");
    handle.shutdown().await.unwrap();

    // A server without accept_push refuses outright.
    let plain_prefix = unique_prefix();
    let plain = BlobServer::new(&session, common::serve(plain_prefix.clone()));
    let plain_handle = plain.spawn().await.unwrap();
    let client2 = test_client(&session, &plain_prefix);
    let err = client2
        .upload_file(BlobSpec::new("x").chunk_size(MIN_CHUNK_SIZE), &src_path)
        .token(b"secret".to_vec())
        .await
        .expect_err("push must be off by default");
    assert!(matches!(err, BlobError::PushDenied(_)), "{err}");

    plain_handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Cancel an upload mid-stream, then re-upload: the server's offer names only
/// the missing chunks and the second pass completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_upload_resumes_from_spool() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 6, 13);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    struct CancelAt {
        token: CancelToken,
        at: u32,
    }
    impl ProgressSink for CancelAt {
        fn emit(&self, p: Progress) {
            if let Progress::Chunk { received, .. } = p
                && received >= self.at
            {
                self.token.cancel();
            }
        }
    }

    let client = test_client(&session, &prefix);
    let token = CancelToken::new();
    let sink = CancelAt {
        token: token.clone(),
        at: 2,
    };
    let err = client
        .upload_file(
            BlobSpec::new("resumable").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
        )
        .token(b"secret".to_vec())
        .progress(&sink)
        .cancel(&token)
        .await
        .expect_err("must cancel");
    assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");

    // Second attempt: the offer's Resumed event proves a spool head start.
    let saw_resume = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = saw_resume.clone();
    let sink = move |p: Progress| {
        if let Progress::Resumed { received, .. } = p
            && received >= 2
        {
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    };
    let manifest = tokio::time::timeout(
        Duration::from_secs(20),
        client
            .upload_file(
                BlobSpec::new("resumable").chunk_size(MIN_CHUNK_SIZE),
                &src_path,
            )
            .token(b"secret".to_vec())
            .progress(&sink),
    )
    .await
    .expect("timed out")
    .expect("resume upload");
    assert!(
        saw_resume.load(std::sync::atomic::Ordering::SeqCst),
        "server must offer a resume, not a restart"
    );

    // Round-trip the pushed blob.
    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("down.bin");
    client
        .download_to(&DownloadRequest::pinned("resumable", manifest.root), &dest)
        .await
        .expect("download");
    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Pushing an empty blob finalizes at the offer itself (no slices exist) and
/// the empty blob is immediately downloadable.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_blob_push_finalizes_at_offer() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("empty.bin");
    std::fs::write(&src_path, b"").unwrap();

    let client = test_client(&session, &prefix);
    let manifest = client
        .upload_file(BlobSpec::new("void"), &src_path)
        .token(b"secret".to_vec())
        .await
        .expect("empty upload");
    assert_eq!(manifest.total_len, 0);

    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("void.bin");
    client
        .download_to(&DownloadRequest::pinned("void", manifest.root), &dest)
        .await
        .expect("download empty pushed blob");
    assert_eq!(std::fs::read(&dest).unwrap(), b"");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A push must never hijack a blob the server already serves: a different-
/// content offer for a registered id is refused; a same-content offer is
/// acknowledged as already complete without touching the registration.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_cannot_hijack_registered_blob() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let original = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 71);
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let registered = server
        .register_source(
            BlobSpec::new("victim").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(original.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // Attacker holds a valid token but different content.
    let evil = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 72);
    let src = tempfile::tempdir().unwrap();
    let evil_path = src.path().join("evil.bin");
    std::fs::write(&evil_path, &evil).unwrap();

    let client = test_client(&session, &prefix);
    let err = client
        .upload_file(
            BlobSpec::new("victim").chunk_size(MIN_CHUNK_SIZE),
            &evil_path,
        )
        .token(b"secret".to_vec())
        .await
        .expect_err("hijack must be refused");
    assert!(matches!(err, BlobError::PushDenied(_)), "{err}");

    // The original is untouched and still served.
    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("check.bin");
    client
        .download_to(&DownloadRequest::pinned("victim", registered.root), &dest)
        .await
        .expect("original still served");
    assert_eq!(std::fs::read(&dest).unwrap(), original);

    // Same-content re-push short-circuits successfully (idempotent).
    let same_path = src.path().join("same.bin");
    std::fs::write(&same_path, &original).unwrap();
    let m = client
        .upload_file(
            BlobSpec::new("victim").chunk_size(MIN_CHUNK_SIZE),
            &same_path,
        )
        .token(b"secret".to_vec())
        .await
        .expect("idempotent re-push of identical content");
    assert_eq!(m.root, registered.root);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A hostile push endpoint replying malformed "wanted" ranges must produce a
/// clean protocol error on the uploader — never a panic or wild indices.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostile_offer_reply_is_rejected_cleanly() {
    let session = open_session().await;
    let prefix = unique_prefix();

    // Fake endpoint: acks every offer with garbage ranges.
    let q = session
        .declare_queryable(format!("{prefix}/**"))
        .await
        .unwrap();
    let evil = tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let garbage: Vec<(u32, u32)> = vec![(5, 2), (0, 1_000_000)];
            let _ = query
                .reply(
                    query.key_expr().clone(),
                    zblob::wire::encode(&garbage).unwrap(),
                )
                .encoding(&zblob::wire::ENC_PUSH)
                .await;
        }
    });

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 73);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(&session, &prefix);
    let err = client
        .upload_file(
            BlobSpec::new("garbage").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
        )
        .await
        .expect_err("garbage ranges must be rejected");
    assert!(matches!(err, BlobError::MalformedMessage(_)), "{err}");

    evil.abort();
    session.close().await.unwrap();
}

/// The concurrent-push cap bounds spool growth: with a cap of 1, a second
/// distinct id is turned away while the first is in progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_push_cap_enforced() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()).max_concurrent(1))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 74);
    let src = tempfile::tempdir().unwrap();
    let a = src.path().join("a.bin");
    std::fs::write(&a, &data).unwrap();

    let client = test_client(&session, &prefix);

    // Occupy the single slot: cancel after the first slice so the push stays
    // in progress server-side.
    struct CancelFirst(CancelToken);
    impl ProgressSink for CancelFirst {
        fn emit(&self, p: Progress) {
            if matches!(p, Progress::Chunk { .. }) {
                self.0.cancel();
            }
        }
    }
    let token = CancelToken::new();
    let _ = client
        .upload_file(BlobSpec::new("slot").chunk_size(MIN_CHUNK_SIZE), &a)
        .token(b"secret".to_vec())
        .progress(&CancelFirst(token.clone()))
        .cancel(&token)
        .await;

    // A second, different id is now over the cap.
    let err = client
        .upload_file(BlobSpec::new("overflow").chunk_size(MIN_CHUNK_SIZE), &a)
        .token(b"secret".to_vec())
        .await
        .expect_err("second push must exceed the cap");
    assert!(
        matches!(&err, BlobError::PushDenied(msg) if msg.contains("too many")),
        "{err}"
    );
    // Resuming the *first* id still works (it holds the slot, not a new one).
    client
        .upload_file(BlobSpec::new("slot").chunk_size(MIN_CHUNK_SIZE), &a)
        .token(b"secret".to_vec())
        .await
        .expect("resuming the slot-holder completes");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Allows everything (the willing receiver).
struct OpenPolicy;
impl PushPolicy for OpenPolicy {
    fn allow(&self, _manifest: &Manifest, _token: Option<&[u8]>) -> bool {
        true
    }
}

/// Two servers on one prefix, one of them with push disabled: the upload must
/// still complete.
///
/// This is not hypothetical. zensight's netring sensor deliberately runs a
/// second `BlobServer` on its artifact prefix, relying on servers ignoring ids
/// they do not own — so a server answering "push not enabled on this server"
/// shares the prefix with one that accepts. Treating any responder's error as
/// the answer violates the crate's own rule that one bad reply must not be
/// fatal, and it is the arm that would bite a real consumer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusing_co_server_cannot_deny_an_accepting_one() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    // One server accepts pushes…
    let accepting = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(OpenPolicy), spool.path()))
        .build()
        .spawn()
        .await
        .unwrap();
    // …and one on the same prefix has push switched off entirely.
    let refusing = BlobServer::new(&session, common::serve(prefix.clone()))
        .spawn()
        .await
        .unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2 + 13, 23);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("shared.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(&session, &prefix);
    let manifest = tokio::time::timeout(
        Duration::from_secs(20),
        client.upload_file(
            BlobSpec::new("coexist").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
        ),
    )
    .await
    .expect("upload must not hang")
    .expect("a refusing co-server must not deny an accepting one");
    assert_eq!(manifest.root, content_hash(&data));

    // …and the accepting server really did land it: it serves the blob now.
    let dest = tempfile::tempdir().unwrap().keep();
    let out = dest.join("back.bin");
    client
        .download_to(&DownloadRequest::pinned("coexist", manifest.root), &out)
        .await
        .expect("the pushed blob must be downloadable");
    assert_eq!(std::fs::read(&out).unwrap(), data);

    accepting.shutdown().await.unwrap();
    refusing.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// An upload has exactly one destination, so a wildcard prefix is refused
/// rather than fanning the upload out across every matching origin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_refuses_a_wildcard_prefix() {
    let session = open_session().await;
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("x.bin");
    std::fs::write(&src_path, b"payload").unwrap();

    let base = unique_prefix();
    let wildcard = format!("{base}/*/blob");
    let err = test_client(&session, &wildcard)
        .upload_file(BlobSpec::new("nope").chunk_size(MIN_CHUNK_SIZE), &src_path)
        .await
        .expect_err("a wildcard upload prefix must be refused");
    assert!(matches!(err, BlobError::Usage(_)), "{err}");

    // Discriminating power: the same call against a concrete prefix gets past
    // prefix validation (it then fails because nothing is serving, which is a
    // different error).
    let concrete = format!("{base}/one/blob");
    let err2 = test_client(&session, &concrete)
        .upload_file(BlobSpec::new("nope").chunk_size(MIN_CHUNK_SIZE), &src_path)
        .await
        .expect_err("nothing is serving that prefix");
    assert!(
        matches!(err2, BlobError::PushDenied(_)),
        "a concrete prefix must get past validation: {err2}"
    );

    session.close().await.unwrap();
}

/// `upload_source` is `upload_file` without the file: an in-memory source
/// pushes, registers, and round-trips — and, per its contract, emits no
/// `Completed` event (the returned manifest is the completion signal) while
/// still emitting per-chunk progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_source_lands_and_serves() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3 + 777, 41);
    let chunks_seen = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let completed_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (chunks_flag, completed_flag) = (chunks_seen.clone(), completed_seen.clone());
    let sink = move |p: Progress| match p {
        Progress::Chunk { .. } => {
            chunks_flag.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Progress::Completed { .. } => {
            completed_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        _ => {}
    };

    let client = test_client(&session, &prefix);
    let manifest = tokio::time::timeout(
        Duration::from_secs(20),
        client
            .upload_source(
                BlobSpec::new("from-memory").chunk_size(MIN_CHUNK_SIZE),
                Arc::new(MemoryBlobSource::new(data.clone())),
            )
            .token(b"secret".to_vec())
            .progress(&sink),
    )
    .await
    .expect("timed out")
    .expect("upload from source");
    assert_eq!(manifest.root, content_hash(&data));
    assert!(
        chunks_seen.load(std::sync::atomic::Ordering::SeqCst) >= 4,
        "per-chunk progress must still be emitted"
    );
    assert!(
        !completed_seen.load(std::sync::atomic::Ordering::SeqCst),
        "a source upload has no final path, so it must not emit Completed"
    );

    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("down.bin");
    client
        .download_to(&DownloadRequest::pinned("from-memory", manifest.root), &dest)
        .await
        .expect("download pushed blob");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// The zero-length source: finalized at the offer like the file path, and
/// `MemoryBlobSource::new(vec![])` reports `Some(0)`, not "unknown".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_source_upload_finalizes_at_offer() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let client = test_client(&session, &prefix);
    let manifest = client
        .upload_source(
            BlobSpec::new("void-src"),
            Arc::new(MemoryBlobSource::new(Vec::new())),
        )
        .token(b"secret".to_vec())
        .await
        .expect("empty source upload");
    assert_eq!(manifest.total_len, 0);

    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("void.bin");
    client
        .download_to(&DownloadRequest::pinned("void-src", manifest.root), &dest)
        .await
        .expect("download empty pushed blob");
    assert_eq!(std::fs::read(&dest).unwrap(), b"");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// The wildcard-prefix refusal is shared between both upload entry points.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upload_source_refuses_a_wildcard_prefix() {
    let session = open_session().await;
    let wildcard = format!("{}/*/blob", unique_prefix());
    let err = test_client(&session, &wildcard)
        .upload_source(
            BlobSpec::new("nope").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(b"payload".to_vec())),
        )
        .await
        .expect_err("a wildcard upload prefix must be refused");
    assert!(matches!(err, BlobError::Usage(_)), "{err}");
    session.close().await.unwrap();
}

/// A source whose reader cannot state its size fails before any network
/// traffic with a clear error, mirroring `register_source`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sizeless_source_fails_with_a_clear_error() {
    use zblob::{BlobSource, ReadAt, ReadAtSize, Size};

    struct Endless;
    impl ReadAt for Endless {
        fn read_at(&self, _pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
            buf.fill(0);
            Ok(buf.len())
        }
    }
    impl Size for Endless {
        fn size(&self) -> std::io::Result<Option<u64>> {
            Ok(None)
        }
    }
    struct SizelessSource;
    impl BlobSource for SizelessSource {
        fn open(&self) -> std::io::Result<Box<dyn ReadAtSize>> {
            Ok(Box::new(Endless))
        }
    }

    let session = open_session().await;
    let prefix = unique_prefix();
    let err = test_client(&session, &prefix)
        .upload_source(BlobSpec::new("endless"), Arc::new(SizelessSource))
        .await
        .expect_err("a sizeless source cannot be hashed");
    assert!(
        err.to_string().contains("no known size"),
        "the error must name the problem: {err}"
    );
    session.close().await.unwrap();
}

/// The fingerprint guard: a source whose identity changes between the hash
/// pass and completion fails loudly on the uploader, instead of leaving the
/// receiver holding slices hashed from bytes that no longer exist. The bytes
/// themselves stay constant here — only the fingerprint lies — so every
/// slice verifies and the guard is the *only* thing that can catch it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_that_mutates_mid_upload_fails_loudly() {
    use std::sync::atomic::{AtomicI64, Ordering};
    use zblob::{BlobSource, ReadAtSize, SourceFingerprint};

    struct VersionedSource {
        data: Arc<Vec<u8>>,
        version: AtomicI64,
    }
    impl BlobSource for VersionedSource {
        fn open(&self) -> std::io::Result<Box<dyn ReadAtSize>> {
            MemoryBlobSource::from_arc(self.data.clone()).open()
        }
        fn fingerprint(&self) -> Option<SourceFingerprint> {
            Some(SourceFingerprint {
                len: self.data.len() as u64,
                mtime_ns: Some(self.version.load(Ordering::SeqCst) as i128),
            })
        }
    }

    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 42);
    let source = Arc::new(VersionedSource {
        data: Arc::new(data),
        version: AtomicI64::new(0),
    });

    // "Mutate" deterministically mid-transfer: the first chunk ack bumps the
    // version, so the post-transfer re-check must see a changed fingerprint.
    let bump = source.clone();
    let sink = move |p: Progress| {
        if matches!(p, Progress::Chunk { .. }) {
            bump.version.store(1, Ordering::SeqCst);
        }
    };

    let client = test_client(&session, &prefix);
    let err = client
        .upload_source(
            BlobSpec::new("mutant").chunk_size(MIN_CHUNK_SIZE),
            source.clone(),
        )
        .token(b"secret".to_vec())
        .progress(&sink)
        .await
        .expect_err("a mutated source must fail the upload");
    assert!(
        err.to_string().contains("changed during the upload"),
        "the error must diagnose the mutation: {err}"
    );

    // Discriminating power: the identical harness with a stable fingerprint
    // succeeds — the failure above is the guard, not the harness.
    let stable = Arc::new(VersionedSource {
        data: source.data.clone(),
        version: AtomicI64::new(7),
    });
    client
        .upload_source(
            BlobSpec::new("stable").chunk_size(MIN_CHUNK_SIZE),
            stable,
        )
        .token(b"secret".to_vec())
        .await
        .expect("a stable source through the same harness must succeed");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
