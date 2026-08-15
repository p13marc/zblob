//! Adversarial fanout receivers: a hostile co-publisher on the fanout key
//! must not be able to smuggle, deny, or corrupt — the receiver's oracle is
//! the same as every hostile suite's ("succeed with exactly the right bytes,
//! or fail cleanly"), narrowed for a tier with no second responder: fail
//! cleanly, with the *specific* error, and leave nothing at the destination.
#![cfg(feature = "fanout")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common::fanout::{demo_manifest, hand_rolled_frames, republish_until, republish_until_enc};
use common::{open_session, pseudo_random, unique_prefix};
use zblob::fanout::{FanoutConfig, receive_fanout};
use zblob::{BlobError, CancelToken, MIN_CHUNK_SIZE, Overwrite};

/// Phase B must apply the same filter-on-the-tag-before-decoding rule as
/// phase A. Before the fix, only phase A filtered: a co-publisher whose
/// frames a receiver would drop at the front door could inject them once the
/// manifest was through, because the slice loop decoded any payload that
/// happened to parse. The bao proof still protects the *bytes*, but the rule
/// exists so a foreign sample is rejected for what it *is*, not for failing
/// deep inside a transfer ("opaque error" failure mode, v2's fix).
///
/// The setup splits the slices: an honest, correctly-tagged publisher sends
/// the manifest and the first half; a second publisher sends the rest under
/// `application/octet-stream`. If the mistagged frames are honored, the
/// transfer completes — which is exactly the bug. Filtered, the receiver
/// stalls at half and reports `Incomplete`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn phase_b_ignores_frames_phase_a_would_reject() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 90);
    let manifest = demo_manifest(&data);
    let root = manifest.root;

    let frames = hand_rolled_frames(&manifest, &data, None);
    let (manifest_and_first_half, second_half) = {
        let mid = 1 + frames.len() / 2; // frame 0 is the manifest
        (frames[..mid].to_vec(), frames[mid..].to_vec())
    };

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let honest_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        let frames = manifest_and_first_half;
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };
    let mistagged_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        let frames = second_half;
        tokio::spawn(async move {
            republish_until_enc(
                &session,
                &prefix,
                "rollout",
                frames,
                done,
                zenoh::bytes::Encoding::APPLICATION_OCTET_STREAM,
            )
            .await;
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
    let _ = honest_task.await;
    let _ = mistagged_task.await;

    match outcome {
        Err(BlobError::Incomplete { received, total }) => {
            assert_eq!(total, 4);
            assert!(
                received < total,
                "the mistagged half must not have counted as progress"
            );
            // Non-vacuity: the honest half *did* arrive — the stall is at the
            // half-way mark, not at zero, so the filter (and not a dead
            // publisher) is what stopped the transfer.
            assert!(
                received >= 2,
                "the correctly-tagged half must have been accepted (got {received}/4)"
            );
        }
        other => panic!(
            "mistagged phase-B frames must be ignored, leaving the transfer \
             incomplete; got {other:?}"
        ),
    }
    assert!(!dest.exists(), "nothing must be left at the destination");

    session.close().await.unwrap();
}

/// Discriminating power for the test above: the identical harness with the
/// second publisher *correctly* tagged completes with the right bytes — so
/// the `Incomplete` above is the tag filter at work, not a broken split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_same_split_succeeds_when_correctly_tagged() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 4, 91);
    let manifest = demo_manifest(&data);
    let root = manifest.root;

    let frames = hand_rolled_frames(&manifest, &data, None);
    let (manifest_and_first_half, second_half) = {
        let mid = 1 + frames.len() / 2;
        (frames[..mid].to_vec(), frames[mid..].to_vec())
    };

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let first_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        let frames = manifest_and_first_half;
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };
    let second_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        let frames = second_half;
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
    .expect("the same split, correctly tagged, must complete");
    done.store(true, Ordering::Relaxed);
    let _ = first_task.await;
    let _ = second_task.await;

    assert_eq!(stats.chunks_fetched, 4);
    assert_eq!(std::fs::read(&dest).unwrap(), data);

    session.close().await.unwrap();
}

/// `Overwrite::Refuse` on a destination that already exists fails *before*
/// subscribing — no stream is consumed and no `.part` is disturbed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overwrite_refuse_rejects_a_preexisting_destination() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("occupied.bin");
    std::fs::write(&dest, b"i was here first").unwrap();

    let err = receive_fanout(
        &session,
        &common::query(prefix),
        "rollout",
        None,
        &dest,
        &(),
        &CancelToken::new(),
        FanoutConfig {
            overwrite: Overwrite::Refuse,
            stall_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .expect_err("a pre-existing destination must be refused");
    assert!(matches!(err, BlobError::DestinationExists(_)), "{err}");
    // The existing file is untouched.
    assert_eq!(std::fs::read(&dest).unwrap(), b"i was here first");

    session.close().await.unwrap();
}

/// A co-publisher's malformed manifests must be skipped, not fatal: an honest
/// publisher sharing the key still completes the rollout. This is the fanout
/// analogue of "a refusing co-server cannot deny an accepting one".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn foreign_and_malformed_manifests_are_skipped_not_fatal() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 92);
    let honest = demo_manifest(&data);
    let root = honest.root;

    // A pile of manifest frames the receiver must each reject and move past.
    let mut bad = Vec::new();
    let mut wrong_id = honest.clone();
    wrong_id.id = zblob::BlobId::new("someone-else").unwrap();
    bad.push(common::fanout::manifest_frame(&wrong_id));
    let mut wrong_root = honest.clone();
    wrong_root.root = zblob::Hash::of(b"a different blob entirely");
    bad.push(common::fanout::manifest_frame(&wrong_root));
    let mut oversize = honest.clone();
    oversize.total_len = 1 << 30; // far over the max_blob_size below
    bad.push(common::fanout::manifest_frame(&oversize));
    let mut bad_version = honest.clone();
    bad_version.version = 99;
    bad.push(common::fanout::manifest_frame(&bad_version));
    bad.push(vec![0xFFu8; 40]); // undecodable, but correctly tagged

    let honest_frames = hand_rolled_frames(&honest, &data, None);

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let bad_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", bad, done).await;
        })
    };
    let good_task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", honest_frames, done).await;
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
            // Fits the honest 3-chunk blob, rejects the 1 GiB oversize claim.
            max_blob_size: 1 << 20,
            ..Default::default()
        },
    )
    .await
    .expect("the honest manifest must carry the rollout through the junk");
    done.store(true, Ordering::Relaxed);
    let _ = bad_task.await;
    let _ = good_task.await;

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert_eq!(stats.chunks_fetched, 3);

    session.close().await.unwrap();
}

/// When nothing acceptable ever arrives, the stall returns the *specific*
/// deferred rejection — not a generic `Incomplete`. That specificity is the
/// whole reason `deferred_reject` exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stall_returns_the_specific_deferred_variant() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 93);
    let honest = demo_manifest(&data);

    // Only a wrong-root manifest is ever published (no slices): the receiver
    // pins the honest root, defers the RootMismatch, and returns it on stall.
    let mut wrong_root = honest.clone();
    wrong_root.root = zblob::Hash::of(b"not this");
    let frames = vec![common::fanout::manifest_frame(&wrong_root)];

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };

    let err = receive_fanout(
        &session,
        &common::query(prefix.clone()),
        "rollout",
        Some(honest.root),
        &dest,
        &(),
        &CancelToken::new(),
        FanoutConfig {
            stall_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .expect_err("a wrong-root-only stream must fail");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;

    assert!(
        matches!(err, BlobError::RootMismatch { .. }),
        "the stall must surface the deferred variant, not Incomplete: {err}"
    );
    assert!(!dest.exists());

    session.close().await.unwrap();
}

/// An honest manifest with no slices behind it stalls to `Incomplete` — the
/// anti-liveness property: a publisher that names a blob but never sends it
/// cannot hold a receiver forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_manifest_with_no_slices_stalls_to_incomplete() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 94);
    let honest = demo_manifest(&data);
    let frames = vec![common::fanout::manifest_frame(&honest)];

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };

    let err = receive_fanout(
        &session,
        &common::query(prefix.clone()),
        "rollout",
        Some(honest.root),
        &dest,
        &(),
        &CancelToken::new(),
        FanoutConfig {
            stall_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await
    .expect_err("a manifest with no slices must not hang");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;

    assert!(
        matches!(
            err,
            BlobError::Incomplete {
                received: 0,
                total: 2
            }
        ),
        "{err}"
    );
    assert!(!dest.exists());

    session.close().await.unwrap();
}

/// Slices arriving before the manifest, past a tiny early-buffer cap, are
/// dropped — and then recovered from the live stream, so the transfer still
/// completes. The frame order puts the manifest last, so early buffering is
/// exercised on every republish cycle.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn early_slice_buffer_caps_are_respected() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 95);
    let manifest = demo_manifest(&data);
    let root = manifest.root;

    // Manifest last: on each cycle the slices arrive first and hit the early
    // buffer, which a 1-byte cap refuses outright.
    let mut frames = hand_rolled_frames(&manifest, &data, None);
    let manifest_frame = frames.remove(0);
    frames.push(manifest_frame);

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
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
            stall_timeout: Duration::from_secs(6),
            max_early_bytes: 1, // every early slice overflows and is dropped
            ..Default::default()
        },
    )
    .await
    .expect("a starved early buffer must still complete from the live stream");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert_eq!(stats.chunks_fetched, 3);

    session.close().await.unwrap();
}

/// Cancellation in either phase returns `Cancelled` and leaves nothing behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_in_both_phases_leaves_no_partial() {
    // Phase A: cancelled before any manifest is accepted.
    {
        let session = open_session().await;
        let prefix = unique_prefix();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("a.bin");
        let cancel = CancelToken::new();
        cancel.cancel(); // fire before the call: the first check exits phase A

        let err = receive_fanout(
            &session,
            &common::query(prefix),
            "rollout",
            None,
            &dest,
            &(),
            &cancel,
            FanoutConfig {
                stall_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .expect_err("a pre-cancelled receive must not proceed");
        assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");
        assert!(!dest.exists() && !dest.with_extension("bin.part").exists());
        session.close().await.unwrap();
    }

    // Phase B: cancelled the moment the stream starts (Progress::Started), via
    // the sink — a signal, not a timer. Only the manifest is published, so
    // phase B enters with an empty early buffer and the first loop iteration
    // observes the cancel deterministically (no race with a drained buffer).
    {
        let session = open_session().await;
        let prefix = unique_prefix();
        let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 96);
        let manifest = demo_manifest(&data);
        let root = manifest.root;
        let frames = vec![common::fanout::manifest_frame(&manifest)];

        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("b.bin");

        let done = Arc::new(AtomicBool::new(false));
        let task = {
            let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
            tokio::spawn(async move {
                republish_until(&session, &prefix, "rollout", frames, done).await;
            })
        };

        let cancel = CancelToken::new();
        let sink = {
            let cancel = cancel.clone();
            move |p: zblob::Progress| {
                if matches!(p, zblob::Progress::Started { .. }) {
                    cancel.cancel();
                }
            }
        };
        let err = receive_fanout(
            &session,
            &common::query(prefix.clone()),
            "rollout",
            Some(root),
            &dest,
            &sink,
            &cancel,
            FanoutConfig {
                stall_timeout: Duration::from_secs(10),
                ..Default::default()
            },
        )
        .await
        .expect_err("a phase-B cancel must stop the transfer");
        done.store(true, Ordering::Relaxed);
        let _ = task.await;
        assert!(matches!(err, BlobError::Cancelled { .. }), "{err}");
        assert!(!dest.exists() && !dest.with_extension("bin.part").exists());
        session.close().await.unwrap();
    }
}

/// Out-of-range and duplicate slice frames are ignored, not counted as
/// rejections, and the honest slices still complete the transfer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_slice_ignores_out_of_range_and_duplicate_indices() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 97);
    let manifest = demo_manifest(&data);
    let root = manifest.root;

    let ob = common::bao::outboard(&data);
    let mut frames = hand_rolled_frames(&manifest, &data, None);
    // A valid slice at an out-of-range index, and a duplicate of index 0.
    let slice0 = common::bao::slice(&data, &ob, MIN_CHUNK_SIZE, 0);
    frames.push(common::fanout::slice_frame(999, &slice0));
    frames.push(common::fanout::slice_frame(0, &slice0));

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
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
    .expect("out-of-range and duplicate frames must not derail the transfer");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert_eq!(stats.chunks_fetched, 3);
    assert_eq!(stats.rejected, 0, "ignored frames are not rejections");

    session.close().await.unwrap();
}

/// A destination that appears *during* the transfer (TOCTOU) is refused at the
/// end under `Overwrite::Refuse`, and the finished `.part` is preserved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_destination_created_mid_transfer_preserves_the_part() {
    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3, 98);
    let manifest = demo_manifest(&data);
    let root = manifest.root;
    let frames = hand_rolled_frames(&manifest, &data, None);

    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let part = dest.with_extension("bin.part");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };

    // Create the destination on the first chunk — after the pre-check passed,
    // before the transfer finishes.
    let sink = {
        let dest = dest.clone();
        move |p: zblob::Progress| {
            if matches!(p, zblob::Progress::Chunk { .. }) && !dest.exists() {
                let _ = std::fs::write(&dest, b"snuck in");
            }
        }
    };

    let err = receive_fanout(
        &session,
        &common::query(prefix.clone()),
        "rollout",
        Some(root),
        &dest,
        &sink,
        &CancelToken::new(),
        FanoutConfig {
            overwrite: Overwrite::Refuse,
            stall_timeout: Duration::from_secs(6),
            ..Default::default()
        },
    )
    .await
    .expect_err("a destination that appeared mid-transfer must be refused");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;

    assert!(matches!(err, BlobError::DestinationExists(_)), "{err}");
    // The finished transfer is not thrown away: the .part survives.
    assert!(part.exists(), "the completed .part must be preserved");
    assert_eq!(std::fs::read(&dest).unwrap(), b"snuck in");

    session.close().await.unwrap();
}

/// A destination whose parent directory is read-only fails cleanly at file
/// creation, leaving nothing behind.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_readonly_destination_dir_fails_cleanly() {
    use std::os::unix::fs::PermissionsExt;

    let session = open_session().await;
    let prefix = unique_prefix();
    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 2, 99);
    let manifest = demo_manifest(&data);
    let root = manifest.root;
    let frames = hand_rolled_frames(&manifest, &data, None);

    let dir = tempfile::tempdir().unwrap();
    let locked = dir.path().join("locked");
    std::fs::create_dir(&locked).unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
    let dest = locked.join("out.bin");

    let done = Arc::new(AtomicBool::new(false));
    let task = {
        let (session, prefix, done) = (session.clone(), prefix.clone(), done.clone());
        tokio::spawn(async move {
            republish_until(&session, &prefix, "rollout", frames, done).await;
        })
    };

    let err = receive_fanout(
        &session,
        &common::query(prefix.clone()),
        "rollout",
        Some(root),
        &dest,
        &(),
        &CancelToken::new(),
        FanoutConfig {
            stall_timeout: Duration::from_secs(4),
            ..Default::default()
        },
    )
    .await
    .expect_err("a read-only destination directory must fail");
    done.store(true, Ordering::Relaxed);
    let _ = task.await;
    assert!(matches!(err, BlobError::Io(_)), "{err}");

    // Restore permissions so the tempdir can be cleaned up.
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();
    session.close().await.unwrap();
}
