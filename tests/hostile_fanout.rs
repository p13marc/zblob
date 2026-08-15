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
use zblob::{BlobError, CancelToken, MIN_CHUNK_SIZE};

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
