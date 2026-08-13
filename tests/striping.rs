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
/// **The instrument is the test.** This asserted `stats.bytes_fetched <=
/// data.len()` against a counter the client only increments inside
/// `if state.mark(index)` — so it could not exceed the blob under *any*
/// implementation, including the one being replaced. The counting subscriber
/// beside it was never read (`watcher.abort()`), and could not have worked
/// anyway: a Zenoh subscriber does not observe query replies. The claim the
/// doc comment called "the assertion that matters" was untested.
///
/// The holders are therefore fake servers that count what they are *asked
/// for*, which is the only place the answer lives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn striping_asks_each_holder_for_a_disjoint_share() {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use zblob::keys::{manifest_key, parse_ranges, slice_key};
    use zblob::wire::Availability;
    use zblob::wire::{ENC_AVAIL, ENC_MANIFEST, ENC_SLICE, encode};
    use zblob::{BlobId, Manifest, wire};

    let session = open_session().await;
    let base = unique_prefix();
    // Eight chunks so the deal-out across two holders is unambiguous.
    const CHUNKS: u32 = 8;
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * CHUNKS as usize, 51);
    let root = content_hash(&data);
    let ob = Arc::new(common::bao::outboard(&data));

    let manifest = Manifest {
        version: wire::WIRE_VERSION,
        id: BlobId::new("shared").unwrap(),
        filename: None,
        total_len: data.len() as u64,
        chunk_size: MIN_CHUNK_SIZE,
        root,
        created_ms: 0,
        ext: wire::Ext::new(),
    };

    // One honest fake server per origin, recording every chunk index it is
    // asked for. Real bao slices, so the client's verification is genuinely
    // exercised rather than bypassed.
    let asked: Vec<Arc<Mutex<Vec<u32>>>> =
        (0..2).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
    let mut servers = Vec::new();
    for (i, host) in ["host-a", "host-b"].iter().enumerate() {
        let prefix = format!("{base}/{host}/blob");
        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let (m, d, o, log) = (manifest.clone(), data.clone(), ob.clone(), asked[i].clone());
        servers.push(tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let key = query.key_expr().as_str().to_string();
                if key.ends_with("/manifest") {
                    let _ = query
                        .reply(manifest_key(&prefix, "shared"), encode(&m).unwrap())
                        .encoding(&ENC_MANIFEST)
                        .await;
                    continue;
                }
                if key.ends_with("/have") {
                    let avail = Availability::full(CHUNKS);
                    let _ = query
                        .reply(
                            zblob::keys::availability_key(&prefix, "shared"),
                            encode(&avail).unwrap(),
                        )
                        .encoding(&ENC_AVAIL)
                        .await;
                    continue;
                }
                let params = query.parameters().as_str().to_string();
                let Ok(ranges) = parse_ranges(&params, CHUNKS, 512) else {
                    continue;
                };
                for r in ranges {
                    for index in r {
                        log.lock().unwrap().push(index);
                        let _ = query
                            .reply(
                                slice_key(&prefix, "shared", index),
                                common::bao::slice(&d, &o, MIN_CHUNK_SIZE, index),
                            )
                            .encoding(&ENC_SLICE)
                            .await;
                    }
                }
            }
        }));
    }

    let prober = client(&session, &format!("{base}/*/blob"));
    let holders = prober.probe("shared").await.expect("probe");
    assert_eq!(holders.len(), 2, "two holders");
    for h in &holders {
        let avail = h.availability.as_ref().expect("availability answered");
        assert_eq!(avail.count(), CHUNKS, "each holder has the whole blob");
    }

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("striped.bin");
    let stats = prober
        .download_to(&DownloadRequest::pinned("shared", root), &dest)
        .striped(&holders)
        .await
        .expect("striped download");

    assert_eq!(std::fs::read(&dest).unwrap(), data, "wrong bytes");
    assert_eq!(stats.chunks_fetched, CHUNKS);
    assert_eq!(stats.rejected, 0, "no wasted verification work");

    let per_holder: Vec<Vec<u32>> = asked.iter().map(|a| a.lock().unwrap().clone()).collect();
    let total: usize = per_holder.iter().map(Vec::len).sum();

    // The claim. Every chunk was covered, and the whole fetch cost close to
    // one request per chunk rather than one per chunk *per replica* — the
    // endgame deliberately duplicates the tail, so the bound is generous but
    // far below the 2x an unstriped fetch would cost.
    let covered: HashSet<u32> = per_holder.iter().flatten().copied().collect();
    assert_eq!(
        covered.len(),
        CHUNKS as usize,
        "every chunk must be requested somewhere: {per_holder:?}"
    );
    assert!(
        total < CHUNKS as usize * 2,
        "striping must not ask both holders for everything: {total} requests for {CHUNKS} chunks ({per_holder:?})"
    );

    // …and it genuinely used both, rather than degrading to one holder and
    // passing the bound trivially.
    assert!(
        per_holder.iter().all(|h| !h.is_empty()),
        "both holders must be used: {per_holder:?}"
    );

    for s in servers {
        s.abort();
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
    // Discarding this result would let an *empty* spool satisfy the assertion
    // below — a server that accepted nothing also advertises nothing, so the
    // test would pass without ever creating the partial state it is about.
    let err = up
        .upload_file(
            BlobSpec::new("halfway").chunk_size(MIN_CHUNK_SIZE),
            &src_path,
        )
        .progress(&StopEarly(cancel.clone()))
        .cancel(&cancel)
        .await
        .expect_err("the upload must be interrupted, not complete");
    let zblob::BlobError::Cancelled { received, total } = err else {
        panic!("expected a cancellation, got {err:?}");
    };
    assert_eq!(total, 6);
    assert!(
        (1..total).contains(&received),
        "the spool must be genuinely partial, not empty or complete: {received}/{total}"
    );

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
