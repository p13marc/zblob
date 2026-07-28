//! Adversarial servers: substituted content is caught by root pinning, a
//! tampered slice is rejected *alone* (the partial survives and the transfer
//! completes once an honest server appears), and a manifest for the wrong id
//! is refused.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::{open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, Hash,
    MIN_CHUNK_SIZE, MemoryBlobSource, RetryPolicy, manifest_key, parse_ranges, slice_key,
    wire::{ENC_MANIFEST, ENC_SLICE, encode},
};

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, prefix)
        .query_timeout(Duration::from_secs(3))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(100),
        })
        .build()
}

/// An honest server serving *different* content under the id the client wants:
/// with a pinned root this dies at the manifest, before any byte moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_root_rejects_substituted_content() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let expected = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 1);
    let substituted = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 2);

    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("blob-s").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(substituted)),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let client = test_client(session.clone(), &prefix);
    let err = client
        .download_to(
            &DownloadRequest::pinned("blob-s", Hash::of(&expected)),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("must reject substituted content");
    assert!(matches!(err, BlobError::RootMismatch { .. }), "{err}");
    assert!(!dest.exists() && !dir.path().join("out.bin.part").exists());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A server that serves one *tampered* slice (valid bao framing, one payload
/// byte flipped): the client drops exactly that slice, keeps the rest, ends
/// `Incomplete` with the `.part` intact — and completes once the server turns
/// honest, re-fetching only the poisoned chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tampered_slice_dropped_alone_and_healed() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("t.bin");

    let chunk = MIN_CHUNK_SIZE;
    let data = Arc::new(pseudo_random(chunk as usize * 4, 0xABCD));
    let count = 4u32;
    let bad_index = 1u32;

    let ob = common::bao::outboard(&data);
    let manifest = zblob::Manifest {
        version: 2,
        id: "blob-t".into(),
        filename: None,
        total_len: data.len() as u64,
        chunk_size: chunk,
        root: ob.root.into(),
        created_ms: 0,
    };
    let root: Hash = ob.root.into();

    let honest = Arc::new(AtomicBool::new(false));
    let honest_flag = honest.clone();
    let (srv_session, srv_prefix, srv_data, srv_manifest) =
        (session.clone(), prefix.clone(), data.clone(), manifest);
    // Declare before spawning so the client can query immediately.
    let q = srv_session
        .declare_queryable(format!("{srv_prefix}/**"))
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let key = query.key_expr().as_str().to_string();
            if key.ends_with("/manifest") {
                let _ = query
                    .reply(
                        manifest_key(&srv_prefix, "blob-t"),
                        encode(&srv_manifest).unwrap(),
                    )
                    .encoding(ENC_MANIFEST)
                    .await;
                continue;
            }
            let ranges = parse_ranges(query.parameters().as_str(), count, 512).unwrap_or_default();
            for range in ranges {
                for index in range {
                    let mut payload = common::bao::slice(&srv_data, &ob, chunk, index);
                    if index == bad_index && !honest_flag.load(Ordering::SeqCst) {
                        let n = payload.len();
                        payload[n - 5] ^= 0xff; // tamper one payload byte
                    }
                    let _ = query
                        .reply(slice_key(&srv_prefix, "blob-t", index), payload)
                        .encoding(ENC_SLICE)
                        .await;
                }
            }
        }
    });

    let client = test_client(session.clone(), &prefix);
    let err = client
        .download_to(
            &DownloadRequest::pinned("blob-t", root),
            &dest,
            &(),
            &CancelToken::new(),
        )
        .await
        .expect_err("tampered chunk must keep the transfer incomplete");
    match err {
        BlobError::Incomplete { received, total } => {
            assert_eq!(total, count);
            assert_eq!(received, count - 1, "only the tampered chunk is missing");
        }
        other => panic!("expected Incomplete, got {other}"),
    }
    // The partial with 3 verified chunks survives — nothing was deleted.
    assert!(dir.path().join("t.bin.part").exists());

    // Server turns honest → the retry fills exactly the poisoned hole.
    honest.store(true, Ordering::SeqCst);
    tokio::time::timeout(
        Duration::from_secs(20),
        client.download_to(
            &DownloadRequest::pinned("blob-t", root),
            &dest,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("healed download");
    assert_eq!(std::fs::read(&dest).unwrap(), *data);

    server.abort();
    session.close().await.unwrap();
}

/// A confused/malicious responder answering with a manifest for a different id.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_id_mismatch_rejected() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(20_000, 5);
    let ob = common::bao::outboard(&data);
    let manifest = zblob::Manifest {
        version: 2,
        id: "other-blob".into(), // not what the client asked for
        filename: None,
        total_len: data.len() as u64,
        chunk_size: MIN_CHUNK_SIZE,
        root: ob.root.into(),
        created_ms: 0,
    };

    let (srv_session, srv_prefix) = (session.clone(), prefix.clone());
    // Declare before spawning so the client can query immediately.
    let q = srv_session
        .declare_queryable(format!("{srv_prefix}/**"))
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let _ = &srv_prefix;
        while let Ok(query) = q.recv_async().await {
            let _ = query
                .reply(query.key_expr().clone(), encode(&manifest).unwrap())
                .encoding(ENC_MANIFEST)
                .await;
        }
    });

    let client = test_client(session.clone(), &prefix);
    let err = client
        .fetch_manifest("wanted-blob")
        .await
        .expect_err("id mismatch must be rejected");
    assert!(matches!(err, BlobError::Protocol(_)), "{err}");

    server.abort();
    session.close().await.unwrap();
}
