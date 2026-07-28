//! Resume: an interrupted transfer continues from its bitfield's holes — a
//! *middle* hole is re-requested as a range, not by re-streaming a suffix —
//! and reassembles correctly across client instances.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, Progress, ProgressSink, RetryPolicy, manifest_key, parse_ranges, slice_key,
    wire::{ENC_MANIFEST, ENC_SLICE, encode},
};

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, prefix)
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 3,
            base_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_millis(200),
        })
        .build()
}

/// Cancels its token once `received` reaches a threshold.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_then_resume_across_clients() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("data.bin");

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 8, 0x1234);
    let server = BlobServer::new(session.clone(), prefix.clone());
    server
        .register_source(
            BlobSpec::new("blob-r").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // Round 1: cancel after 3 chunks → Cancelled, partial + sidecar persisted.
    let client = test_client(session.clone(), &prefix);
    let token = CancelToken::new();
    let sink = CancelAt {
        token: token.clone(),
        at: 3,
    };
    let err = client
        .download_to(&DownloadRequest::new("blob-r"), &dest, &sink, &token)
        .await
        .expect_err("must cancel");
    assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");
    assert!(dir.path().join("data.bin.part").exists());
    assert!(dir.path().join("data.bin.part.meta").exists());

    // Round 2: a *fresh client* (as after a process restart) resumes from the
    // sidecar instead of starting over.
    let events: Arc<Mutex<Vec<Progress>>> = Arc::new(Mutex::new(Vec::new()));
    let ev = events.clone();
    let sink = move |p: Progress| ev.lock().unwrap().push(p);
    let client2 = test_client(session.clone(), &prefix);
    let stats = tokio::time::timeout(
        Duration::from_secs(20),
        client2.download_to(
            &DownloadRequest::new("blob-r"),
            &dest,
            &sink,
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("resume failed");
    assert!(stats.chunks_resumed >= 3, "resume head start recorded");
    assert_eq!(stats.chunks_fetched + stats.chunks_resumed, 8);

    assert_eq!(
        content_hash(&std::fs::read(&dest).unwrap()),
        content_hash(&data)
    );
    {
        let events = events.lock().unwrap();
        let resumed = events
            .iter()
            .find_map(|e| match e {
                Progress::Resumed { received, .. } => Some(*received),
                _ => None,
            })
            .expect("must emit Resumed, not Started");
        assert!(resumed >= 3, "resume starts from persisted progress");
        let refetched = events
            .iter()
            .filter(|e| matches!(e, Progress::Chunk { .. }))
            .count() as u32;
        assert_eq!(refetched, 8 - resumed, "only the holes are re-fetched");
    }

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A fake server that *skips* one middle chunk on the first slice query, then
/// serves everything on later queries — and records each query's `ranges`
/// parameter so the test can prove the client re-requested exactly the hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn middle_hole_is_refetched_as_a_range() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("holey.bin");

    let chunk = MIN_CHUNK_SIZE;
    let data = Arc::new(pseudo_random(chunk as usize * 6, 0x9999));
    let count = 6u32;
    let skipped = 2u32;

    let ob = common::bao::outboard(&data);
    let manifest = zblob::Manifest {
        version: 2,
        id: "holey".into(),
        filename: None,
        total_len: data.len() as u64,
        chunk_size: chunk,
        root: ob.root.into(),
        created_ms: 0,
    };

    let pinned_root = manifest.root;
    let params_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let plog = params_log.clone();
    let (srv_session, srv_prefix, srv_data) = (session.clone(), prefix.clone(), data.clone());
    // Declare before spawning so the client can query immediately.
    let q = srv_session
        .declare_queryable(format!("{srv_prefix}/**"))
        .await
        .unwrap();
    let server = tokio::spawn(async move {
        let mut slice_queries = 0u32;
        while let Ok(query) = q.recv_async().await {
            let key = query.key_expr().as_str().to_string();
            if key.ends_with("/manifest") {
                let _ = query
                    .reply(
                        manifest_key(&srv_prefix, "holey"),
                        encode(&manifest).unwrap(),
                    )
                    .encoding(ENC_MANIFEST)
                    .await;
                continue;
            }
            let params = query.parameters().as_str().to_string();
            plog.lock().unwrap().push(params.clone());
            slice_queries += 1;
            let ranges = parse_ranges(&params, count, 512).unwrap();
            for range in ranges {
                for index in range {
                    if slice_queries == 1 && index == skipped {
                        continue; // manufacture a middle hole
                    }
                    let payload = common::bao::slice(&srv_data, &ob, chunk, index);
                    let _ = query
                        .reply(slice_key(&srv_prefix, "holey", index), payload)
                        .encoding(ENC_SLICE)
                        .await;
                }
            }
        }
    });

    let client = test_client(session.clone(), &prefix);
    tokio::time::timeout(
        Duration::from_secs(20),
        client.download_to(
            &DownloadRequest::pinned("holey", pinned_root),
            &dest,
            &(),
            &CancelToken::new(),
        ),
    )
    .await
    .expect("timed out")
    .expect("download");

    assert_eq!(std::fs::read(&dest).unwrap(), *data);
    {
        let log = params_log.lock().unwrap();
        assert!(
            log.len() >= 2,
            "needed a second query for the hole: {log:?}"
        );
        assert!(
            log[1].contains(&format!("ranges={skipped}")),
            "second query must request exactly the middle hole, got {:?}",
            log[1]
        );
    }

    server.abort();
    session.close().await.unwrap();
}
