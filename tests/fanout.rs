//! Fanout tier: one publication reaches multiple subscribers, including a
//! late joiner replaying the publisher's cache; every receiver verifies each
//! slice against the pinned root.
#![cfg(feature = "fanout")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::fanout::{demo_manifest, hand_rolled_frames, republish_until};
use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::fanout::{FanoutConfig, fanout_file, receive_fanout};
use zblob::{BlobSpec, CancelToken, MIN_CHUNK_SIZE};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_reaches_live_and_late_subscribers() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 15 + 4321, 41);
    let src = tempfile::tempdir().unwrap();
    let src_path = src.path().join("rollout.bin");
    std::fs::write(&src_path, &data).unwrap();

    // Live subscriber joins before publication.
    let live_dir = tempfile::tempdir().unwrap();
    let live_dest = live_dir.path().join("live.bin");
    let live = {
        let session = session.clone();
        let prefix = prefix.clone();
        let dest = live_dest.clone();
        let expected = zblob::Hash::of(&data);
        tokio::spawn(async move {
            receive_fanout(
                &session,
                &common::query(prefix),
                "rollout",
                Some(expected),
                &dest,
                &(),
                &CancelToken::new(),
                FanoutConfig {
                    stall_timeout: Duration::from_secs(10),
                    ..Default::default()
                },
            )
            .await
        })
    };
    // Best-effort head start so the live subscriber usually sees the stream
    // live; if publishing wins the race anyway, the subscriber's declare-time
    // history query replays the cache — either path must produce the same
    // result, so this cannot flake, it only varies which path is exercised.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (manifest, handle) = fanout_file(
        &session,
        &common::serve(prefix.clone()),
        BlobSpec::new("rollout").chunk_size(MIN_CHUNK_SIZE),
        &src_path,
        FanoutConfig::default(),
    )
    .await
    .expect("fanout");
    assert_eq!(manifest.root, content_hash(&data));

    let live_stats = tokio::time::timeout(Duration::from_secs(20), live)
        .await
        .expect("live subscriber timed out")
        .unwrap()
        .expect("live receive");
    assert_eq!(live_stats.chunks_fetched, 16);
    assert_eq!(std::fs::read(&live_dest).unwrap(), data);

    // Late joiner: subscribes *after* everything was published — the
    // publisher's cache replays the whole stream.
    let late_dir = tempfile::tempdir().unwrap();
    let late_dest = late_dir.path().join("late.bin");
    let late_stats = tokio::time::timeout(
        Duration::from_secs(20),
        receive_fanout(
            &session,
            &common::query(prefix.clone()),
            "rollout",
            Some(manifest.root),
            &late_dest,
            &(),
            &CancelToken::new(),
            FanoutConfig {
                stall_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("late subscriber timed out")
    .expect("late receive");
    assert_eq!(late_stats.chunks_fetched, 16);
    assert_eq!(std::fs::read(&late_dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}

/// A hostile publisher on the fanout key must not make a receiver write wrong
/// bytes.
///
/// "Every receiver verifies" is the claim that makes this tier safe to point
/// at a fleet — a fanout has no query/reply handshake, so a receiver's only
/// defence is the bao proof — and nothing tested it. Unlike the query tiers
/// there is no honest replier to fall back on, so the required outcome is
/// narrower: fail cleanly, and leave nothing at the destination.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tampered_fanout_slice_is_never_written() {
    use std::sync::atomic::{AtomicBool, Ordering};

    for tamper in ["flip", "truncate", "garbage"] {
        let session = open_session().await;
        let prefix = unique_prefix();
        let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 88);
        let manifest = demo_manifest(&data);
        let root = manifest.root;

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("out.bin");

        let done = Arc::new(AtomicBool::new(false));
        let pub_task = {
            let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
            let frames = hand_rolled_frames(&manifest, &data, Some(tamper));
            tokio::spawn(async move {
                republish_until(&session, &prefix, "rollout", frames, done).await;
            })
        };

        let outcome = receive_fanout(
            &session,
            &common::query(prefix.clone()),
            "rollout",
            Some(root),
            &dest,
            &(),
            &CancelToken::new(),
            FanoutConfig {
                stall_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        )
        .await;
        done.store(true, Ordering::Relaxed);
        let _ = pub_task.await;

        assert!(
            outcome.is_err(),
            "{tamper}: a tampered fanout must not report success"
        );
        assert!(
            !dest.exists(),
            "{tamper}: nothing must be left at the destination"
        );
        // The partial goes too — fanout has no resume, so keeping one built
        // from tampered frames would be worse than useless.
        assert!(
            !dest.with_extension("bin.part").exists(),
            "{tamper}: no partial must survive"
        );

        session.close().await.unwrap();
    }
}

/// Discriminating power for the test above: the identical harness publishing
/// *honest* slices completes with the right bytes. Without this the rejection
/// could be of the hand-rolled framing — or of nothing having arrived at all,
/// which is exactly what the first version of these two tests did.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_hand_rolled_frames_succeed_when_honest() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 89);
    let manifest = demo_manifest(&data);
    let root = manifest.root;

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let pub_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        let frames = hand_rolled_frames(&manifest, &data, None);
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };

    let stats = receive_fanout(
        &session,
        &common::query(prefix.clone()),
        "rollout",
        Some(root),
        &dest,
        &(),
        &CancelToken::new(),
        FanoutConfig {
            stall_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .await
    .expect("honest hand-rolled frames must be accepted");
    done.store(true, Ordering::Relaxed);
    let _ = pub_task.await;

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert_eq!(stats.rejected, 0, "honest frames must not be rejected");

    session.close().await.unwrap();
}

/// `fanout_file` validates its inputs before declaring a publisher: a missing
/// path, a malformed chunk size, and an invalid id each fail early rather than
/// spawning a serving task that can never work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fanout_file_rejects_bad_inputs() {
    let session = open_session().await;
    let prefix = unique_prefix();

    // Nonexistent source path.
    let missing = fanout_file(
        &session,
        &common::serve(prefix.clone()),
        BlobSpec::new("x").chunk_size(MIN_CHUNK_SIZE),
        "/definitely/not/here.bin",
        FanoutConfig::default(),
    )
    .await;
    assert!(missing.is_err(), "a missing source path must fail");

    // A real file, but a malformed chunk size.
    let src = tempfile::tempdir().unwrap();
    let path = src.path().join("f.bin");
    std::fs::write(&path, b"hello").unwrap();
    let bad_chunk = fanout_file(
        &session,
        &common::serve(prefix.clone()),
        BlobSpec::new("x").chunk_size(3),
        &path,
        FanoutConfig::default(),
    )
    .await;
    assert!(bad_chunk.is_err(), "a malformed chunk size must fail");

    // A reserved/invalid id.
    let bad_id = fanout_file(
        &session,
        &common::serve(prefix.clone()),
        BlobSpec::new("bad/id").chunk_size(MIN_CHUNK_SIZE),
        &path,
        FanoutConfig::default(),
    )
    .await;
    assert!(bad_id.is_err(), "an invalid id must fail");

    // Discriminating power: a well-formed call succeeds through the same path.
    let (_m, handle) = fanout_file(
        &session,
        &common::serve(prefix.clone()),
        BlobSpec::new("good").chunk_size(MIN_CHUNK_SIZE),
        &path,
        FanoutConfig::default(),
    )
    .await
    .expect("a well-formed fanout must succeed");
    handle.shutdown().await.unwrap();

    session.close().await.unwrap();
}
