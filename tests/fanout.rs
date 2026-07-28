//! Fanout tier: one publication reaches multiple subscribers, including a
//! late joiner replaying the publisher's cache; every receiver verifies each
//! slice against the pinned root.
#![cfg(feature = "fanout")]

mod common;

use std::time::Duration;

use common::{content_hash, open_session, pseudo_random, unique_prefix};
use zblob::fanout::{FanoutConfig, fanout_file, receive_fanout};
use zblob::{BlobSpec, CancelToken, MIN_CHUNK_SIZE};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_reaches_live_and_late_subscribers() {
    let session = open_session().await;
    let prefix = unique_prefix();

    let data = pseudo_random(MIN_CHUNK_SIZE as usize * 3 + 4321, 41);
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
                session,
                &prefix,
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
    // Give the live subscriber time to declare before publishing.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let (manifest, handle) = fanout_file(
        session.clone(),
        &prefix,
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
    assert_eq!(live_stats.chunks_fetched, 4);
    assert_eq!(std::fs::read(&live_dest).unwrap(), data);

    // Late joiner: subscribes *after* everything was published — the
    // publisher's cache replays the whole stream.
    let late_dir = tempfile::tempdir().unwrap();
    let late_dest = late_dir.path().join("late.bin");
    let late_stats = tokio::time::timeout(
        Duration::from_secs(20),
        receive_fanout(
            session.clone(),
            &prefix,
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
    assert_eq!(late_stats.chunks_fetched, 4);
    assert_eq!(std::fs::read(&late_dest).unwrap(), data);

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
