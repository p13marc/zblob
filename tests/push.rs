//! Push (upload) protocol: authorized verified uploads land, get registered,
//! and are immediately downloadable; unauthorized ones are refused; an
//! interrupted upload resumes from the server's spool.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    Manifest, Overwrite, Progress, ProgressSink, PushPolicy, RetryPolicy,
};

/// Allows pushes carrying the byte token `"secret"`.
struct TokenPolicy;
impl PushPolicy for TokenPolicy {
    fn allow(&self, _manifest: &Manifest, token: Option<&[u8]>) -> bool {
        token == Some(b"secret")
    }
}

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, prefix)
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

    let server = BlobServer::builder(session.clone(), prefix.clone())
        .accept_push(Arc::new(TokenPolicy), spool.path())
        .build();
    let handle = server.spawn().await.unwrap();

    // Uploader-side source file: multi-chunk with a short tail.
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3 + 777, 11);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(session.clone(), &prefix);
    let manifest = tokio::time::timeout(
        Duration::from_secs(20),
        client.upload_file(
            BlobSpec::new("pushed").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
            Some(b"secret".to_vec()),
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("upload");
    assert_eq!(manifest.root, content_hash(&data));

    // The receiver now serves the blob: download it back, pinned.
    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("down.bin");
    client
        .download_to(
            &DownloadRequest::pinned("pushed", manifest.root),
            &dest,
            &(),
            &CancelToken::new(),
        )
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

    let server = BlobServer::builder(session.clone(), prefix.clone())
        .accept_push(Arc::new(TokenPolicy), spool.path())
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize, 12);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let client = test_client(session.clone(), &prefix);
    // Wrong token → denied by policy.
    let err = client
        .upload_file(
            BlobSpec::new("nope").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
            Some(b"wrong".to_vec()),
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("must be denied");
    assert!(matches!(err, BlobError::PushDenied(_)), "{err}");
    // Nothing was registered.
    let err = client.fetch_manifest("nope").await.expect_err("no blob");
    assert!(matches!(err, BlobError::NotFound(_)), "{err}");
    handle.shutdown().await.unwrap();

    // A server without accept_push refuses outright.
    let plain_prefix = unique_prefix();
    let plain = BlobServer::new(session.clone(), plain_prefix.clone());
    let plain_handle = plain.spawn().await.unwrap();
    let client2 = test_client(session.clone(), &plain_prefix);
    let err = client2
        .upload_file(
            BlobSpec::new("x").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
            Some(b"secret".to_vec()),
            &(),
            &CancelToken::new(),
        )
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

    let server = BlobServer::builder(session.clone(), prefix.clone())
        .accept_push(Arc::new(TokenPolicy), spool.path())
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

    let client = test_client(session.clone(), &prefix);
    let token = CancelToken::new();
    let sink = CancelAt {
        token: token.clone(),
        at: 2,
    };
    let err = client
        .upload_file(
            BlobSpec::new("resumable").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
            Some(b"secret".to_vec()),
            &sink,
            &token,
        )
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
        client.upload_file(
            BlobSpec::new("resumable").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
            Some(b"secret".to_vec()),
            &sink,
            &CancelToken::new(),
        ),
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
        .download_to(
            &DownloadRequest::pinned("resumable", manifest.root),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect("download");
    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
