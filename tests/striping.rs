//! Multi-source: availability that means something, and ranges that cross the
//! wire once.
//!
//! What the crate called multi-source was multi-*responder* tolerance. With
//! reply consolidation off, every matching holder sends every requested slice
//! and the client discards the duplicates — after they have crossed the wire,
//! because Zenoh cannot cancel remote replies in flight. N replicas cost N
//! times the bandwidth. These tests hold the line on the difference.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobServer, BlobSpec, CancelToken, DownloadRequest, MIN_CHUNK_SIZE,
    MemoryBlobSource, Overwrite, PushConfig, RetryPolicy,
};

fn client(session: &zenoh::Session, prefix: &str) -> BlobClient {
    BlobClient::builder(session, common::query(prefix))
        .query_timeout(Duration::from_secs(5))
        .retry(RetryPolicy {
            max_attempts: 4,
            base_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(100),
        })
        .overwrite(Overwrite::Replace)
        .build()
}

/// Striping addresses each range to one holder, so each chunk crosses the wire
/// once — instead of once per replica, which is what asking a shared key does.
///
/// The byte count is the assertion that matters. Without it this would pass
/// for an implementation that simply asked everyone and threw the duplicates
/// away, which is exactly the behaviour being replaced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn striping_sends_each_chunk_once_not_once_per_replica() {
    let session = open_session().await;
    let base = unique_prefix();
    // Eight chunks so the deal-out across two holders is unambiguous.
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 8, 51);

    let mut handles = Vec::new();
    for host in ["host-a", "host-b"] {
        let server = BlobServer::new(&session, common::serve(format!("{base}/{host}/blob")));
        server
            .register_source(
                BlobSpec::new("shared").chunk_size(MIN_CHUNK_SIZE),
                Arc::new(MemoryBlobSource::new(data.clone())),
            )
            .await
            .unwrap();
        handles.push(server.spawn().await.unwrap());
    }

    // Count slice replies actually delivered, by watching the wire.
    let served = Arc::new(AtomicUsize::new(0));
    let watcher = {
        let served = served.clone();
        let sub = session
            .declare_subscriber(format!("{base}/*/blob/**"))
            .await
            .unwrap();
        tokio::spawn(async move {
            while let Ok(_s) = sub.recv_async().await {
                served.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let prober = client(&session, &format!("{base}/*/blob"));
    let holders = prober.probe("shared").await.expect("probe");
    assert_eq!(holders.len(), 2, "two holders");
    for h in &holders {
        let avail = h.availability.as_ref().expect("availability answered");
        assert_eq!(avail.count(), 8, "each holder has the whole blob");
    }

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("striped.bin");
    let stats = prober
        .download_to(
            &DownloadRequest::pinned("shared", content_hash(&data)),
            &dest,
        )
        .striped(&holders)
        .await
        .expect("striped download");

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert_eq!(stats.chunks_fetched, 8);
    // The bandwidth claim: eight chunks, not sixteen. A little slack for the
    // endgame duplicating the tail.
    assert!(
        stats.bytes_fetched <= data.len() as u64,
        "striping must not fetch more than the blob: {} vs {}",
        stats.bytes_fetched,
        data.len()
    );
    assert_eq!(stats.rejected, 0, "no wasted verification work");

    watcher.abort();
    for h in handles {
        h.shutdown().await.unwrap();
    }
    session.close().await.unwrap();
}

/// Tier-1 availability is all-or-nothing, and that is a property of bao, not
/// an unimplemented feature.
///
/// A slice is the chunk's bytes *plus the sibling hashes proving them against
/// the root*. Those siblings are hashes of other subtrees, so producing one
/// requires the whole blob — which is why the outboard is computed at
/// registration and at push finalization, never from a partial spool. A holder
/// with part of a blob cannot serve any verified slice of it.
///
/// So an `Availability` reporting a partial tier-1 holding would advertise
/// chunks no client could obtain. This test pins the constraint, because the
/// obvious "improvement" — have an in-flight push report its resume bitfield —
/// is exactly the lie it forbids.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tier1_availability_is_all_or_nothing_by_construction() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 6, 52);

    struct Yes;
    impl zblob::PushPolicy for Yes {
        fn allow(&self, _m: &zblob::Manifest, _t: Option<&[u8]>) -> bool {
            true
        }
    }
    let handle = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(Yes), spool.path()))
        .build()
        .spawn()
        .await
        .unwrap();

    // Push part of a blob, then stop: the server now holds a genuinely partial
    // spool for this id.
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("half.bin");
    std::fs::write(&src_path, &data).unwrap();
    let up = client(&session, &prefix);
    let cancel = CancelToken::new();
    struct StopEarly(CancelToken);
    impl zblob::ProgressSink for StopEarly {
        fn emit(&self, p: zblob::Progress) {
            if let zblob::Progress::Chunk { received, .. } = p
                && received >= 3
            {
                self.0.cancel();
            }
        }
    }
    let _ = up
        .upload_file(
            BlobSpec::new("halfway").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
        )
        .progress(&StopEarly(cancel.clone()))
        .cancel(&cancel)
        .await;

    // The server does not advertise the half it holds — it *cannot* serve any
    // of it, so claiming it would send clients after chunks they can never get.
    let holders = client(&session, &prefix)
        .probe("halfway")
        .await
        .expect("probe");
    assert!(
        holders.is_empty(),
        "a partial tier-1 spool must not advertise itself as a holder"
    );

    // Discriminating power: once the same blob is registered whole, it does
    // advertise — and reports every chunk.
    let server2 = BlobServer::new(&session, common::serve(prefix.clone()));
    server2
        .register_source(
            BlobSpec::new("halfway").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let h2 = server2.spawn().await.unwrap();
    let holders = client(&session, &prefix)
        .probe("halfway")
        .await
        .expect("probe");
    assert_eq!(holders.len(), 1, "a complete holder does advertise");
    assert_eq!(
        holders[0].availability.as_ref().unwrap().count(),
        6,
        "and reports the whole blob"
    );

    h2.shutdown().await.unwrap();
    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// With fewer than two holders, striping degrades to the ordinary download
/// rather than erroring — the right behaviour for "I probed and found one".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_single_holder_degrades_to_a_plain_download() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 53);
    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    server
        .register_source(
            BlobSpec::new("solo").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let c = client(&session, &prefix);
    let holders = c.probe("solo").await.unwrap();
    assert_eq!(holders.len(), 1);

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("solo.bin");
    c.download_to(&DownloadRequest::pinned("solo", content_hash(&data)), &dest)
        .striped(&holders)
        .await
        .expect("one holder is not an error");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
