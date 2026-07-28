//! Adversarial peers, systematically.
//!
//! Every earlier test asserted an outcome for an input *I chose*, which is why
//! they confirmed the implementation instead of interrogating it. These tests
//! instead sweep a matrix of reply mutations against a fixed oracle:
//!
//! > For any behaviour a peer can exhibit, the client must (a) not panic,
//! > (b) terminate, (c) never report success with bytes that differ from the
//! > pinned root's content, and (d) never write outside its destination.
//!
//! The mutations are applied to *protocol-correct* replies, so each case
//! exercises a real code path rather than being rejected at the front door.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{open_session, pseudo_random, unique_prefix};
use zblob::{
    BlobClient, BlobError, CancelToken, DownloadRequest, Hash, MIN_CHUNK_SIZE, Manifest,
    RetryPolicy, manifest_key, parse_ranges, slice_key,
    wire::{self, ENC_MANIFEST, ENC_SLICE},
};

/// How a hostile peer corrupts the reply it is about to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutation {
    /// Behave honestly (the control case).
    None,
    /// Send an empty payload.
    Empty,
    /// Cut the payload in half.
    Truncate,
    /// Append junk.
    Extend,
    /// Flip a byte in the middle.
    FlipMiddle,
    /// Flip the last byte.
    FlipLast,
    /// Replace the payload with unrelated bytes.
    Garbage,
    /// Reply under a *different* chunk index's key.
    ShiftKey,
    /// Reply with the wrong Zenoh encoding tag.
    WrongEncoding,
    /// Send the same reply twice.
    Duplicate,
    /// Serve chunk 0's slice for every index.
    AlwaysChunkZero,
}

const MUTATIONS: &[Mutation] = &[
    Mutation::None,
    Mutation::Empty,
    Mutation::Truncate,
    Mutation::Extend,
    Mutation::FlipMiddle,
    Mutation::FlipLast,
    Mutation::Garbage,
    Mutation::ShiftKey,
    Mutation::WrongEncoding,
    Mutation::Duplicate,
    Mutation::AlwaysChunkZero,
];

fn mutate(payload: Vec<u8>, m: Mutation) -> Vec<u8> {
    match m {
        Mutation::Empty => Vec::new(),
        Mutation::Truncate => payload[..payload.len() / 2].to_vec(),
        Mutation::Extend => {
            let mut p = payload;
            p.extend_from_slice(&[0xAB; 64]);
            p
        }
        Mutation::FlipMiddle => {
            let mut p = payload;
            if !p.is_empty() {
                let i = p.len() / 2;
                p[i] ^= 0xff;
            }
            p
        }
        Mutation::FlipLast => {
            let mut p = payload;
            if let Some(last) = p.last_mut() {
                *last ^= 0xff;
            }
            p
        }
        Mutation::Garbage => vec![0x5A; 128],
        _ => payload,
    }
}

fn test_client(session: Arc<zenoh::Session>, prefix: &str) -> BlobClient {
    BlobClient::builder(session, prefix)
        .query_timeout(Duration::from_millis(700))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(30),
        })
        .build()
}

/// Sweep every mutation applied to **slice** replies. The oracle: succeed with
/// exactly the right bytes, or fail cleanly — never anything in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slice_reply_mutations_never_yield_wrong_bytes() {
    let session = open_session().await;
    let data = Arc::new(pseudo_random(MIN_CHUNK_SIZE as usize * 3 + 1234, 91));
    let ob = Arc::new(common::bao::outboard(&data));
    let root: Hash = ob.root.into();

    let mut succeeded: Vec<Mutation> = Vec::new();
    let mut refused: Vec<Mutation> = Vec::new();
    for &mutation in MUTATIONS {
        let prefix = unique_prefix();
        let manifest = Manifest {
            version: 2,
            id: "hostile".into(),
            filename: None,
            total_len: data.len() as u64,
            chunk_size: MIN_CHUNK_SIZE,
            root,
            created_ms: 0,
        };
        let count = manifest.chunks().unwrap().count();

        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let (srv_prefix, srv_data, srv_ob) = (prefix.clone(), data.clone(), ob.clone());
        let server = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let key = query.key_expr().as_str().to_string();
                if key.ends_with("/manifest") {
                    let _ = query
                        .reply(
                            manifest_key(&srv_prefix, "hostile"),
                            wire::encode(&manifest).unwrap(),
                        )
                        .encoding(ENC_MANIFEST)
                        .await;
                    continue;
                }
                let ranges =
                    parse_ranges(query.parameters().as_str(), count, 512).unwrap_or_default();
                for range in ranges {
                    for index in range {
                        let source_index = if mutation == Mutation::AlwaysChunkZero {
                            0
                        } else {
                            index
                        };
                        let payload =
                            common::bao::slice(&srv_data, &srv_ob, MIN_CHUNK_SIZE, source_index);
                        let payload = mutate(payload, mutation);
                        let reply_index = if mutation == Mutation::ShiftKey {
                            (index + 1) % count.max(1)
                        } else {
                            index
                        };
                        let enc = if mutation == Mutation::WrongEncoding {
                            "application/octet-stream"
                        } else {
                            ENC_SLICE
                        };
                        let key = slice_key(&srv_prefix, "hostile", reply_index);
                        let _ = query
                            .reply(key.clone(), payload.clone())
                            .encoding(enc)
                            .await;
                        if mutation == Mutation::Duplicate {
                            let _ = query.reply(key, payload).encoding(enc).await;
                        }
                    }
                }
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let client = test_client(session.clone(), &prefix);
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            client.download_to(
                &DownloadRequest::pinned("hostile", root),
                &dest,
                &(),
                &CancelToken::new(),
            ),
        )
        .await;

        // (b) terminates.
        let outcome = outcome.unwrap_or_else(|_| panic!("{mutation:?}: download hung"));
        match outcome {
            // (c) success implies byte-exact content.
            Ok(_) => {
                assert_eq!(
                    std::fs::read(&dest).unwrap(),
                    *data,
                    "{mutation:?}: reported success with wrong bytes"
                );
                succeeded.push(mutation);
            }
            Err(e) => {
                assert!(
                    !dest.exists(),
                    "{mutation:?}: failed but left a destination file ({e})"
                );
                refused.push(mutation);
            }
        }
        // (d) nothing escaped the destination directory.
        let stray: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| !n.starts_with("out.bin"))
            .collect();
        assert!(stray.is_empty(), "{mutation:?}: unexpected files {stray:?}");

        server.abort();
    }

    // Discriminating power: a sweep that fails (or succeeds) uniformly proves
    // nothing about the code under test, so assert the shape of the outcome
    // set — the honest control must succeed, and content-destroying mutations
    // must be refused.
    assert!(
        succeeded.contains(&Mutation::None),
        "the honest control case failed — the harness never exercised the real path"
    );
    for destroying in [
        Mutation::Empty,
        Mutation::Truncate,
        Mutation::Garbage,
        Mutation::FlipMiddle,
        Mutation::WrongEncoding,
    ] {
        assert!(
            refused.contains(&destroying),
            "{destroying:?} was accepted; expected the transfer to fail"
        );
    }
    // Duplicates and slices served under a shifted key are *tolerable*: the
    // bitfield dedups and mis-keyed slices simply never satisfy their index.
    assert!(
        succeeded.contains(&Mutation::Duplicate),
        "duplicate replies must be harmless, not fatal"
    );
    session.close().await.unwrap();
}

/// The same sweep applied to **manifest** replies, where a bad value steers
/// sizing, allocation, and geometry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_reply_mutations_are_survivable() {
    let session = open_session().await;
    let data = Arc::new(pseudo_random(MIN_CHUNK_SIZE as usize * 2, 92));
    let ob = common::bao::outboard(&data);
    let root: Hash = ob.root.into();

    // Structurally-valid-but-hostile manifests, plus payload-level mutations.
    let hostile_manifests = vec![
        (
            "wrong_version",
            Manifest {
                version: 99,
                id: "m".into(),
                filename: None,
                total_len: data.len() as u64,
                chunk_size: MIN_CHUNK_SIZE,
                root,
                created_ms: 0,
            },
        ),
        (
            "huge_total_len",
            Manifest {
                version: 2,
                id: "m".into(),
                filename: None,
                total_len: u64::MAX,
                chunk_size: MIN_CHUNK_SIZE,
                root,
                created_ms: 0,
            },
        ),
        (
            "zero_chunk_size",
            Manifest {
                version: 2,
                id: "m".into(),
                filename: None,
                total_len: 1024,
                chunk_size: 0,
                root,
                created_ms: 0,
            },
        ),
        (
            "unaligned_chunk_size",
            Manifest {
                version: 2,
                id: "m".into(),
                filename: None,
                total_len: 1024,
                chunk_size: MIN_CHUNK_SIZE + 1,
                root,
                created_ms: 0,
            },
        ),
        (
            "traversal_filename",
            Manifest {
                version: 2,
                id: "m".into(),
                filename: Some("../../../etc/pwned".into()),
                total_len: data.len() as u64,
                chunk_size: MIN_CHUNK_SIZE,
                root,
                created_ms: 0,
            },
        ),
        (
            "wrong_id",
            Manifest {
                version: 2,
                id: "someone-else".into(),
                filename: None,
                total_len: data.len() as u64,
                chunk_size: MIN_CHUNK_SIZE,
                root,
                created_ms: 0,
            },
        ),
        (
            "empty_claim",
            Manifest {
                version: 2,
                id: "m".into(),
                filename: None,
                total_len: 0,
                chunk_size: MIN_CHUNK_SIZE,
                root,
                created_ms: 0,
            },
        ),
    ];

    for (label, manifest) in hostile_manifests {
        let prefix = unique_prefix();
        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let srv_prefix = prefix.clone();
        let server = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let _ = query
                    .reply(
                        manifest_key(&srv_prefix, "m"),
                        wire::encode(&manifest).unwrap(),
                    )
                    .encoding(ENC_MANIFEST)
                    .await;
            }
        });

        let outer = tempfile::tempdir().unwrap();
        let dest_dir = outer.path().join("dest");
        std::fs::create_dir(&dest_dir).unwrap();
        let dest = dest_dir.join("out.bin");
        let client = test_client(session.clone(), &prefix);
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            client.download_to(
                &DownloadRequest::pinned("m", root),
                &dest,
                &(),
                &CancelToken::new(),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("{label}: download hung"));

        if outcome.is_ok() {
            assert_eq!(
                std::fs::read(&dest).unwrap(),
                *data,
                "{label}: success with wrong bytes"
            );
        }
        // The advisory filename must never have been honored, anywhere.
        assert!(
            !outer.path().join("etc").exists() && !dest_dir.join("pwned").exists(),
            "{label}: advisory filename influenced a path"
        );
        server.abort();
    }
    session.close().await.unwrap();
}

/// A hostile responder sharing the key range with an honest one must not be
/// able to deny the fetch — whoever answers first, the honest content wins.
/// (This is the invariant the audit found violated: the first bad reply
/// aborted the whole query.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hostile_responder_cannot_deny_an_honest_one() {
    for hostile_reply in ["garbage", "wrong-id", "wrong-root"] {
        let session = open_session().await;
        let prefix = unique_prefix();
        let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 93);

        // Honest server.
        let server = zblob::BlobServer::new(session.clone(), prefix.clone());
        let manifest = server
            .register_source(
                zblob::BlobSpec::new("contested").chunk_size(MIN_CHUNK_SIZE),
                Arc::new(zblob::MemoryBlobSource::new(data.clone())),
            )
            .await
            .unwrap();
        let handle = server.spawn().await.unwrap();

        // Hostile co-responder on the same range, answering manifests only.
        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let (srv_prefix, kind) = (prefix.clone(), hostile_reply.to_string());
        let replies_sent = Arc::new(AtomicUsize::new(0));
        let counter = replies_sent.clone();
        let hostile = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                if !query.key_expr().as_str().ends_with("/manifest") {
                    continue;
                }
                let payload = match kind.as_str() {
                    "garbage" => vec![0xFF; 40],
                    "wrong-id" => wire::encode(&Manifest {
                        version: 2,
                        id: "not-contested".into(),
                        filename: None,
                        total_len: 4096,
                        chunk_size: MIN_CHUNK_SIZE,
                        root: Hash::of(b"nope"),
                        created_ms: 0,
                    })
                    .unwrap(),
                    _ => wire::encode(&Manifest {
                        version: 2,
                        id: "contested".into(),
                        filename: None,
                        total_len: 4096,
                        chunk_size: MIN_CHUNK_SIZE,
                        root: Hash::of(b"substituted"),
                        created_ms: 0,
                    })
                    .unwrap(),
                };
                counter.fetch_add(1, Ordering::SeqCst);
                let _ = query
                    .reply(manifest_key(&srv_prefix, "contested"), payload)
                    .encoding(ENC_MANIFEST)
                    .await;
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");
        let client = test_client(session.clone(), &prefix);
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            client.download_to(
                &DownloadRequest::pinned("contested", manifest.root),
                &dest,
                &(),
                &CancelToken::new(),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("{hostile_reply}: hung"));

        assert!(
            outcome.is_ok(),
            "{hostile_reply}: a hostile co-responder denied an honest fetch: {:?}",
            outcome.err()
        );
        assert_eq!(std::fs::read(&dest).unwrap(), data);
        assert!(
            replies_sent.load(Ordering::SeqCst) > 0,
            "{hostile_reply}: the hostile responder never actually replied — test is vacuous"
        );

        hostile.abort();
        handle.shutdown().await.unwrap();
        session.close().await.unwrap();
    }
}

/// A peer that answers a push offer with arbitrary "wanted" ranges must never
/// crash or mislead the uploader. (The audit found a `u32` underflow here;
/// this sweeps the shape space rather than the one example that caught it.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hostile_push_offer_replies_are_survivable() {
    let session = open_session().await;
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 94);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("up.bin");
    std::fs::write(&src_path, &data).unwrap();

    let hostile_ranges: Vec<Vec<(u32, u32)>> = vec![
        vec![],                                         // "nothing wanted" for a non-empty blob
        vec![(0, u32::MAX)],                            // absurdly large span
        vec![(5, 2)],                                   // inverted
        vec![(0, 0)],                                   // empty span
        vec![(0, 3), (1, 2)],                           // overlapping
        vec![(2, 3), (0, 1)],                           // unsorted
        vec![(0, 1), (0, 1)],                           // duplicated
        vec![(u32::MAX - 1, u32::MAX)],                 // out of bounds at the top
        (0..300).map(|i| (i * 2, i * 2 + 1)).collect(), // over the span cap
    ];

    for (i, ranges) in hostile_ranges.into_iter().enumerate() {
        let prefix = unique_prefix();
        let q = session
            .declare_queryable(format!("{prefix}/**"))
            .await
            .unwrap();
        let payload = wire::encode(&ranges).unwrap();
        let server = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let _ = query
                    .reply(query.key_expr().clone(), payload.clone())
                    .encoding(wire::ENC_PUSH)
                    .await;
            }
        });

        let client = test_client(session.clone(), &prefix);
        let outcome = tokio::time::timeout(
            Duration::from_secs(15),
            client.upload_file(
                zblob::BlobSpec::new("up").chunk_size(MIN_CHUNK_SIZE),
                &src_path,
                None,
                &(),
                &CancelToken::new(),
            ),
        )
        .await
        .unwrap_or_else(|_| panic!("case {i}: upload hung"));

        // Success is only legitimate if the peer genuinely wanted nothing and
        // the blob is empty — which it isn't here. Anything else must be a
        // clean typed error, never a panic (the test surviving proves that).
        match outcome {
            Ok(_) => assert!(
                ranges_cover_nothing(&ranges),
                "case {i}: upload claimed success against a hostile peer"
            ),
            Err(e) => assert!(
                matches!(
                    e,
                    BlobError::Protocol(_)
                        | BlobError::PushDenied(_)
                        | BlobError::Incomplete { .. }
                ),
                "case {i}: unexpected error kind {e:?}"
            ),
        }
        server.abort();
    }
    session.close().await.unwrap();
}

fn ranges_cover_nothing(ranges: &[(u32, u32)]) -> bool {
    ranges.iter().all(|(a, b)| a >= b)
}
