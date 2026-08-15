//! Adversarial clients against a `BlobServer`.
//!
//! Every other hostile suite points at a *client* (`hostile_peer`,
//! `hostile_store`); nothing drove the server's own refusal paths, which are
//! exactly what a malicious or broken peer exercises — and they are the least
//! covered lines in the crate. The honest [`BlobClient`] can't reach them: it
//! *discards* error replies, so a `reply_err` is invisible through it. These
//! tests issue raw `session.get()`s and read `reply.result().err()` directly.
//!
//! The oracle, the server-side mirror of the two existing hostile suites: for
//! any query a client can send, the server must (a) not panic, (b) answer
//! with a well-formed typed reply or a `reply_err` — never wedge, never
//! silence except for ids it does not own, and (c) keep serving honest
//! clients afterwards. Every test carries that (c) control, so a refusal is
//! shown to be of the *request*, not of the client.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::keys::{push_offer_key, push_slice_key, slice_selector};
use zblob::wire::{self, ENC_PUSH, ENC_SLICE};
use zblob::{
    BlobClient, BlobId, BlobServer, BlobSpec, DownloadRequest, Hash, MIN_CHUNK_SIZE, Manifest,
    MemoryBlobSource, Overwrite, PushConfig, PushPolicy, RetryPolicy,
};
use zenoh::query::ConsolidationMode;

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

fn manifest_for(id: &str, data: &[u8]) -> Manifest {
    Manifest {
        version: wire::WIRE_VERSION,
        id: BlobId::new(id).unwrap(),
        filename: None,
        total_len: data.len() as u64,
        chunk_size: MIN_CHUNK_SIZE,
        root: Hash::of(data),
        created_ms: 0,
        ext: wire::Ext::new(),
    }
}

/// Raw slice-request GET: returns the number of `ENC_SLICE` replies and the
/// first error-reply string, if any.
async fn raw_get(session: &zenoh::Session, selector: &str) -> (usize, Option<String>) {
    let replies = session
        .get(selector)
        .consolidation(ConsolidationMode::None)
        .await
        .unwrap();
    let (mut slices, mut err) = (0usize, None);
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(s) if ENC_SLICE.matches(s.encoding()) => slices += 1,
            Ok(_) => {}
            Err(e) => {
                err.get_or_insert_with(|| String::from_utf8_lossy(&e.payload().to_bytes()).into());
            }
        }
    }
    (slices, err)
}

/// Raw push-offer GET. `Ok(wanted ranges)` on an `ENC_PUSH` ack, `Err(reason)`
/// on a refusal, mirroring what the honest uploader does with the reply.
async fn raw_offer(
    session: &zenoh::Session,
    prefix: &str,
    id: &str,
    payload: Option<Vec<u8>>,
    token: Option<&[u8]>,
) -> Result<Vec<(u32, u32)>, String> {
    let mut b = session
        .get(push_offer_key(prefix, id))
        .consolidation(ConsolidationMode::None);
    if let Some(p) = payload {
        b = b.payload(p);
    }
    if let Some(t) = token {
        b = b.attachment(t.to_vec());
    }
    let replies = b.await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(s) if ENC_PUSH.matches(s.encoding()) => {
                return Ok(wire::decode(&s.payload().to_bytes()).unwrap());
            }
            Ok(_) => {}
            Err(e) => return Err(String::from_utf8_lossy(&e.payload().to_bytes()).into()),
        }
    }
    Err("no reply".into())
}

/// Raw push-slice GET. `Ok(remaining)` on an ack, `Err(reason)` on a refusal.
async fn raw_slice(
    session: &zenoh::Session,
    prefix: &str,
    id: &str,
    index: u32,
    payload: Option<Vec<u8>>,
    token: Option<&[u8]>,
) -> Result<u32, String> {
    let mut b = session
        .get(push_slice_key(prefix, id, index))
        .consolidation(ConsolidationMode::None);
    if let Some(p) = payload {
        b = b.payload(p);
    }
    if let Some(t) = token {
        b = b.attachment(t.to_vec());
    }
    let replies = b.await.unwrap();
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(s) if ENC_PUSH.matches(s.encoding()) => {
                return Ok(wire::decode(&s.payload().to_bytes()).unwrap());
            }
            Ok(_) => {}
            Err(e) => return Err(String::from_utf8_lossy(&e.payload().to_bytes()).into()),
        }
    }
    Err("no reply".into())
}

/// The honest bao slice for transfer chunk `index` of `data`.
fn honest_slice(data: &[u8], index: u32) -> Vec<u8> {
    let ob = common::bao::outboard(data);
    common::bao::slice(data, &ob, MIN_CHUNK_SIZE, index)
}

// ---------------------------------------------------------------- serve side

/// Every shape of malformed range selector is refused with an error reply, and
/// the server keeps serving the honest range right after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_range_selectors_reply_err_and_keep_serving() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 51);

    // A tight cap so the over-cap arm is reachable cheaply.
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .max_chunks_per_query(2)
        .build();
    let _m = server
        .register_source(
            BlobSpec::new("blob").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let base = format!("{prefix}/blob/**");
    let bad = [
        format!("{base}?ranges=garbage"), // unparseable
        format!("{base}?ranges="),        // empty span
        format!("{base}?ranges=3-1"),     // inverted
        format!("{base}?ranges=2-2"),     // empty
        format!("{base}?ranges=2-1,0-1"), // unsorted
        format!("{base}?ranges=0-2,1-3"), // overlapping
        format!("{base}?ranges=0-9999"),  // out of bounds
        format!("{base}?other=1"),        // missing ranges param
        format!("{base}?ranges=0-3"),     // over the 2-chunk cap
    ];
    for selector in &bad {
        let (slices, err) = raw_get(&session, selector).await;
        assert_eq!(slices, 0, "{selector}: a bad selector served slices");
        assert!(
            err.is_some(),
            "{selector}: a bad selector got no error reply"
        );
    }

    // (c) The honest range still serves — the refusals were of the requests.
    let (slices, err) = raw_get(&session, &slice_selector(&prefix, "blob", &[0..2])).await;
    assert_eq!(
        slices, 2,
        "the honest 2-chunk range must serve: err={err:?}"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A `?ranges=` GET against a registered *empty* blob is refused (chunk count
/// 0), while its manifest and availability still answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ranges_against_a_registered_empty_blob_reply_err() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    server
        .register_source(
            BlobSpec::new("void"),
            Arc::new(MemoryBlobSource::new(Vec::new())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    let (slices, err) = raw_get(&session, &format!("{prefix}/void/**?ranges=0-1")).await;
    assert_eq!(slices, 0);
    assert!(err.is_some(), "ranges on an empty blob must be refused");

    // (c) The manifest still answers — the empty blob is served, just sliceless.
    let m = test_client(&session, &prefix)
        .fetch_manifest("void")
        .await
        .expect("empty blob manifest still served");
    assert_eq!(m.total_len, 0);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A multi-range GET aimed at a *concrete* slice key (no `/**`) makes the
/// server's second reply fail the reply-key match mid-stream. It must stop
/// cleanly — no panic, no wedge — and keep serving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multi_range_on_a_concrete_slice_key_stops_cleanly() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 52);

    let server = BlobServer::new(&session, common::serve(prefix.clone()));
    let m = server
        .register_source(
            BlobSpec::new("blob").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    let handle = server.spawn().await.unwrap();

    // Concrete key for index 0, but asking for two chunks: reply for index 1
    // cannot match the query key and errors on the server.
    let (_slices, _err) = raw_get(&session, &format!("{prefix}/blob/slice/0?ranges=0-2")).await;

    // (c) The server survived the mid-stream reply failure and serves honestly.
    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("out.bin");
    test_client(&session, &prefix)
        .download_to(&DownloadRequest::pinned("blob", m.root), &dest)
        .await
        .expect("server still serves after a mid-stream reply failure");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

// ----------------------------------------------------------------- push side

/// Every push-offer refusal path answers with an error reply, leaves no
/// registration behind, and the server accepts an honest push afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_offer_error_arms_are_survivable() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    // A tiny blob-size cap makes the lying-size arm reachable; every crafted
    // manifest below is small enough to pass it and reach its own check.
    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()).max_blob_size(64 * 1024))
        .build();
    let handle = server.spawn().await.unwrap();

    let small = pseudo_random(1000, 60);

    // no payload
    assert!(
        raw_offer(&session, &prefix, "a", None, Some(b"secret"))
            .await
            .is_err()
    );

    // undecodable manifest
    assert!(
        raw_offer(
            &session,
            &prefix,
            "a",
            Some(vec![0xFF; 40]),
            Some(b"secret")
        )
        .await
        .is_err()
    );

    // bad wire version
    let mut m = manifest_for("a", &small);
    m.version = 99;
    assert!(
        raw_offer(
            &session,
            &prefix,
            "a",
            Some(wire::encode(&m).unwrap()),
            Some(b"secret")
        )
        .await
        .is_err()
    );

    // bad chunk size (not aligned / below MIN)
    let mut m = manifest_for("a", &small);
    m.chunk_size = 3;
    assert!(
        raw_offer(
            &session,
            &prefix,
            "a",
            Some(wire::encode(&m).unwrap()),
            Some(b"secret")
        )
        .await
        .is_err()
    );

    // lying size: declares far more than max_blob_size
    let mut m = manifest_for("a", &small);
    m.total_len = 1 << 30;
    assert!(
        raw_offer(
            &session,
            &prefix,
            "a",
            Some(wire::encode(&m).unwrap()),
            Some(b"secret")
        )
        .await
        .is_err()
    );

    // id ≠ offer-key id
    let m = manifest_for("elsewhere", &small);
    assert!(
        raw_offer(
            &session,
            &prefix,
            "here",
            Some(wire::encode(&m).unwrap()),
            Some(b"secret")
        )
        .await
        .is_err()
    );

    // wrong token → policy denial
    let m = manifest_for("a", &small);
    assert!(
        raw_offer(
            &session,
            &prefix,
            "a",
            Some(wire::encode(&m).unwrap()),
            Some(b"wrong")
        )
        .await
        .is_err()
    );

    // Nothing above registered anything.
    assert!(
        server_serves(&session, &prefix, "a").await.is_none(),
        "a refused offer must leave no registration"
    );

    // (c) An honest push still lands and serves.
    let client = test_client(&session, &prefix);
    let m = client
        .upload_source(
            BlobSpec::new("good").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(small.clone())),
        )
        .token(b"secret".to_vec())
        .await
        .expect("an honest push must still be accepted");
    assert_eq!(m.root, content_hash(&small));

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Fetch a manifest if the server serves the id, else `None` — a small probe
/// for "did a refused request register anything".
async fn server_serves(session: &zenoh::Session, prefix: &str, id: &str) -> Option<Manifest> {
    test_client(session, prefix).fetch_manifest(id).await.ok()
}

/// On a server without push enabled, both offer and slice keys reply with an
/// error rather than silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_not_enabled_offer_and_slice_reply_err() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let handle = BlobServer::new(&session, common::serve(prefix.clone()))
        .spawn()
        .await
        .unwrap();

    let m = manifest_for("x", b"hi");
    assert!(
        raw_offer(
            &session,
            &prefix,
            "x",
            Some(wire::encode(&m).unwrap()),
            None
        )
        .await
        .is_err()
    );
    assert!(
        raw_slice(&session, &prefix, "x", 0, Some(vec![0u8; 4]), None)
            .await
            .is_err()
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A second offer for an in-flight id with different geometry is refused, and
/// the original push can still complete; an identical re-offer is acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_conflicting_reoffer_for_an_inflight_push_is_refused() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 61);
    let m = manifest_for("job", &data);

    // Open the push and leave it in-flight (no slices yet).
    let wanted = raw_offer(
        &session,
        &prefix,
        "job",
        Some(wire::encode(&m).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("offer accepted");
    assert_eq!(wanted, vec![(0, 2)]);

    // A conflicting re-offer (different content) is refused…
    let other = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 62);
    let mut conflict = manifest_for("job", &other);
    conflict.total_len = m.total_len; // keep len equal, change only the root
    let err = raw_offer(
        &session,
        &prefix,
        "job",
        Some(wire::encode(&conflict).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect_err("a conflicting re-offer must be refused");
    assert!(err.contains("conflicting"), "{err}");

    // …while an identical re-offer is acked (idempotent resume).
    let again = raw_offer(
        &session,
        &prefix,
        "job",
        Some(wire::encode(&m).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("identical re-offer acked");
    assert_eq!(again, vec![(0, 2)], "resume names the still-missing chunks");

    // (c) The original push still completes with honest slices.
    raw_slice(
        &session,
        &prefix,
        "job",
        0,
        Some(honest_slice(&data, 0)),
        Some(b"secret"),
    )
    .await
    .expect("slice 0");
    let remaining = raw_slice(
        &session,
        &prefix,
        "job",
        1,
        Some(honest_slice(&data, 1)),
        Some(b"secret"),
    )
    .await
    .expect("slice 1");
    assert_eq!(
        remaining, 0,
        "the push completes despite the conflicting offer"
    );
    assert!(server_serves(&session, &prefix, "job").await.is_some());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// An abandoned push is evicted (spool files removed) when the next offer
/// arrives. `idle_timeout(ZERO)` makes eviction unconditional — no sleeps.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_push_is_evicted_on_the_next_offer() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(
            PushConfig::new(Arc::new(TokenPolicy), spool.path()).idle_timeout(Duration::ZERO),
        )
        .build();
    let handle = server.spawn().await.unwrap();

    let data_a = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 63);
    let a = manifest_for("aaaa", &data_a);
    raw_offer(
        &session,
        &prefix,
        "aaaa",
        Some(wire::encode(&a).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("offer A");
    assert!(
        spool.path().join("aaaa.push.part").exists(),
        "A's spool must exist after its offer"
    );

    // A second offer for a different id evaluates eviction: A is idle (ZERO
    // timeout) so its spool is swept.
    let data_b = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 64);
    let b = manifest_for("bbbb", &data_b);
    raw_offer(
        &session,
        &prefix,
        "bbbb",
        Some(wire::encode(&b).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("offer B");
    assert!(
        !spool.path().join("aaaa.push.part").exists(),
        "A's spool must be evicted on B's offer"
    );

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A spool path that is a regular file (not a directory) fails the offer with
/// an error reply, and the server survives it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spool_that_is_a_regular_file_fails_the_offer() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let tmp = tempfile::tempdir().unwrap();
    let spool_as_file = tmp.path().join("not-a-dir");
    std::fs::write(&spool_as_file, b"i am a file").unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), &spool_as_file))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 65);
    let m = manifest_for("blocked", &data);
    let err = raw_offer(
        &session,
        &prefix,
        "blocked",
        Some(wire::encode(&m).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect_err("a file-as-spool must fail the offer");
    assert!(!err.is_empty());

    // (c) The serve loop is still alive: an unknown-id manifest fetch returns a
    // clean NotFound rather than hanging.
    assert!(server_serves(&session, &prefix, "blocked").await.is_none());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Every push-slice refusal path answers with an error reply, and an honest
/// completion still lands afterwards on the same in-flight push.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_slice_error_arms_are_survivable() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 66);
    let m = manifest_for("push", &data);
    raw_offer(
        &session,
        &prefix,
        "push",
        Some(wire::encode(&m).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("offer");

    // slice for a never-offered id
    assert!(
        raw_slice(
            &session,
            &prefix,
            "ghost",
            0,
            Some(honest_slice(&data, 0)),
            Some(b"secret")
        )
        .await
        .is_err()
    );
    // no payload
    assert!(
        raw_slice(&session, &prefix, "push", 0, None, Some(b"secret"))
            .await
            .is_err()
    );
    // index out of range
    assert!(
        raw_slice(
            &session,
            &prefix,
            "push",
            99,
            Some(honest_slice(&data, 0)),
            Some(b"secret")
        )
        .await
        .is_err()
    );
    // tampered / truncated / garbage bytes at a valid index — none may mark it
    for tamper in ["flip", "truncate", "garbage"] {
        let mut bao = honest_slice(&data, 0);
        match tamper {
            "flip" => {
                let mid = bao.len() / 2;
                bao[mid] ^= 0xFF;
            }
            "truncate" => bao.truncate(bao.len() / 2),
            _ => bao = vec![0xABu8; bao.len()],
        }
        assert!(
            raw_slice(&session, &prefix, "push", 0, Some(bao), Some(b"secret"))
                .await
                .is_err(),
            "{tamper}: a bad slice must be refused"
        );
    }

    // (c) The honest slices still complete the push — none of the refusals
    // above corrupted its spool state.
    raw_slice(
        &session,
        &prefix,
        "push",
        0,
        Some(honest_slice(&data, 0)),
        Some(b"secret"),
    )
    .await
    .expect("honest slice 0");
    let remaining = raw_slice(
        &session,
        &prefix,
        "push",
        1,
        Some(honest_slice(&data, 1)),
        Some(b"secret"),
    )
    .await
    .expect("honest slice 1");
    assert_eq!(remaining, 0);

    let dl = tempfile::tempdir().unwrap();
    let dest = dl.path().join("out.bin");
    test_client(&session, &prefix)
        .download_to(&DownloadRequest::pinned("push", m.root), &dest)
        .await
        .expect("the completed push serves");
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A policy that authorizes the offer but denies the slices refuses each slice
/// with an error, and the server keeps serving. This is the only way to reach
/// the slice-stage policy check — a stateless policy can't.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flipping_policy_that_denies_mid_push_is_survivable() {
    /// Allows the offer (call 1), denies every subsequent request.
    struct FlippingPolicy {
        calls: AtomicUsize,
    }
    impl PushPolicy for FlippingPolicy {
        fn allow(&self, _m: &Manifest, _t: Option<&[u8]>) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst) == 0
        }
    }

    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();
    let policy = Arc::new(FlippingPolicy {
        calls: AtomicUsize::new(0),
    });

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(policy.clone(), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 67);
    let m = manifest_for("flip", &data);
    raw_offer(
        &session,
        &prefix,
        "flip",
        Some(wire::encode(&m).unwrap()),
        None,
    )
    .await
    .expect("offer allowed on the first policy call");

    let err = raw_slice(
        &session,
        &prefix,
        "flip",
        0,
        Some(honest_slice(&data, 0)),
        None,
    )
    .await
    .expect_err("the slice must be denied by the flipped policy");
    assert!(err.contains("denied"), "{err}");
    assert!(
        policy.calls.load(Ordering::SeqCst) >= 2,
        "the policy must have been consulted at the slice stage"
    );

    // (c) The server is still alive and serving.
    assert!(server_serves(&session, &prefix, "flip").await.is_none());

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// Tampering the spool `.part` between the last two slices makes finalize's
/// root recomputation fail: the blob file is removed and the id is left
/// unregistered.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finalize_root_mismatch_removes_the_spool() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let spool = tempfile::tempdir().unwrap();

    let server = BlobServer::builder(&session, common::serve(prefix.clone()))
        .accept_push(PushConfig::new(Arc::new(TokenPolicy), spool.path()))
        .build();
    let handle = server.spawn().await.unwrap();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 68);
    let m = manifest_for("corrupt", &data);
    raw_offer(
        &session,
        &prefix,
        "corrupt",
        Some(wire::encode(&m).unwrap()),
        Some(b"secret"),
    )
    .await
    .expect("offer");

    // Send the first slice honestly (verifies + marks + writes chunk 0).
    raw_slice(
        &session,
        &prefix,
        "corrupt",
        0,
        Some(honest_slice(&data, 0)),
        Some(b"secret"),
    )
    .await
    .expect("slice 0");

    // Corrupt the spool on disk between the verified write and finalize. The
    // per-slice check can't catch this — only finalize's whole-blob rehash.
    let part = spool.path().join("corrupt.push.part");
    let mut bytes = std::fs::read(&part).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&part, &bytes).unwrap();

    // The final honest slice triggers finalize → root mismatch.
    let err = raw_slice(
        &session,
        &prefix,
        "corrupt",
        1,
        Some(honest_slice(&data, 1)),
        Some(b"secret"),
    )
    .await
    .expect_err("finalize must reject a tampered spool");
    assert!(err.contains("root mismatch"), "{err}");

    // The blob file is gone and the id is not registered.
    assert!(
        !spool.path().join("corrupt.blob").exists(),
        "the blob must be removed"
    );
    assert!(server_serves(&session, &prefix, "corrupt").await.is_none());

    // (c) The server still accepts a fresh honest push.
    let clean = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 69);
    test_client(&session, &prefix)
        .upload_source(
            BlobSpec::new("clean").chunk_size(MIN_CHUNK_SIZE),
            Arc::new(MemoryBlobSource::new(clean)),
        )
        .token(b"secret".to_vec())
        .await
        .expect("a clean push after a rejected one");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
