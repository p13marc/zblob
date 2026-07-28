//! One-to-many fanout tier (optional `fanout` feature).
//!
//! The Tier-1 queryable model degrades to N independent transfers when one
//! artifact must reach many nodes at once (a config rollout, a firmware
//! push). This tier publishes the blob **once** as a sample stream on
//! `<prefix>/<id>/fanout` through a `zenoh-ext` `AdvancedPublisher`:
//!
//! - the publisher **caches** every frame, so late joiners replay history;
//! - **sample-miss detection** + heartbeat let subscribers recover losses;
//! - every slice frame is still a self-verifying bao slice, so each receiver
//!   verifies against the manifest root exactly like a Tier-1 download.
//!
//! The cache holds the entire encoded blob in publisher memory for the
//! handle's lifetime — this tier is for rollout-sized artifacts, not
//! arbitrarily large blobs.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use zenoh::qos::{CongestionControl, Priority, Reliability};
use zenoh_ext::{
    AdvancedPublisherBuilderExt, AdvancedSubscriberBuilderExt, CacheConfig, HistoryConfig,
    MissDetectionConfig, RecoveryConfig,
};

use crate::cancel::CancelToken;
use crate::chunk::TransferChunks;
use crate::client::Overwrite;
use crate::error::{BlobError, Result};
use crate::hash::Hash;
use crate::manifest::{BlobSpec, Manifest};
use crate::obs::{TransferStats, zdebug};
use crate::progress::{Progress, ProgressSink};
use crate::verify;

/// Key the fanout stream of blob `id` is published on.
pub fn fanout_key(prefix: &str, id: &str) -> String {
    format!("{prefix}/{id}/fanout")
}

/// One sample of the fanout stream.
#[derive(Serialize, Deserialize)]
enum FanoutFrame {
    /// Frame 0: the manifest.
    Manifest(Manifest),
    /// A bao slice for one transfer chunk.
    Slice {
        /// Transfer chunk index.
        index: u32,
        /// Bao slice (self-verifying against the manifest root).
        bao: Vec<u8>,
    },
}

/// Fanout tuning.
#[derive(Debug, Clone)]
pub struct FanoutConfig {
    /// Publisher heartbeat period for sample-miss detection (default 500 ms).
    pub heartbeat: Duration,
    /// Receiver-side stall timeout: give up if no useful frame arrives for
    /// this long (default 30 s).
    pub stall_timeout: Duration,
    /// Receiver-side policy when `dest` already exists (default
    /// [`Overwrite::Refuse`], matching `download_to`).
    pub overwrite: Overwrite,
}

impl Default for FanoutConfig {
    fn default() -> Self {
        FanoutConfig {
            heartbeat: Duration::from_millis(500),
            stall_timeout: Duration::from_secs(30),
            overwrite: Overwrite::default(),
        }
    }
}

/// How many pre-manifest slice frames a receiver will buffer (and their total
/// byte cap) while waiting for the manifest frame. Frames beyond the cap are
/// dropped; heartbeat recovery re-delivers samples the *transport* missed but
/// not ones the application discarded, so a receiver that joins mid-stream
/// may need the publisher's cache replay (which the initial history query
/// performs) to fill what it dropped here.
const EARLY_SLICE_MAX_FRAMES: usize = 512;
const EARLY_SLICE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Keeps a fanout publication (and its replay cache) alive.
pub struct FanoutHandle {
    stop: Arc<tokio::sync::Notify>,
    join: tokio::task::JoinHandle<Result<()>>,
}

impl FanoutHandle {
    /// Stop serving the cache and release its memory.
    pub async fn shutdown(self) -> Result<()> {
        self.stop.notify_one();
        self.join
            .await
            .map_err(|e| BlobError::Protocol(format!("fanout task join: {e}")))?
    }
}

/// Fan out the file at `path` under `spec`: hash it, publish the manifest and
/// every bao slice once (cached for late joiners), and keep serving until the
/// returned handle is shut down. Returns the manifest — distribute
/// `(id, root)` so receivers can pin.
pub async fn fanout_file(
    session: Arc<zenoh::Session>,
    prefix: &str,
    spec: BlobSpec,
    path: impl Into<PathBuf>,
    cfg: FanoutConfig,
) -> Result<(Manifest, FanoutHandle)> {
    let path = path.into();
    let hash_path = path.clone();
    let (outboard, total_len) =
        tokio::task::spawn_blocking(move || -> std::io::Result<(verify::MemOutboard, u64)> {
            let file = std::fs::File::open(&hash_path)?;
            let total_len = file.metadata()?.len();
            Ok((verify::compute_outboard(file)?, total_len))
        })
        .await
        .map_err(|e| BlobError::Protocol(format!("hash task: {e}")))??;
    let outboard = Arc::new(outboard);
    let chunks = TransferChunks::new(spec.chunk_size, total_len)?;
    let count = chunks.count();
    let manifest = Manifest {
        version: crate::wire::WIRE_VERSION,
        id: spec.id.clone(),
        filename: spec.filename,
        total_len,
        chunk_size: spec.chunk_size,
        root: Hash::from(outboard.root),
        created_ms: spec.created_ms,
    };
    manifest.validate(u64::MAX)?;

    let stop = Arc::new(tokio::sync::Notify::new());
    let stop2 = stop.clone();
    let key = fanout_key(prefix, &manifest.id);
    let task_manifest = manifest.clone();
    let join = tokio::spawn(async move {
        let publisher = session
            .declare_publisher(key)
            // Publications default to CongestionControl::Drop — for bulk
            // fanout that would shed slices under local backpressure and
            // leave subscribers to limp along on recovery. Block instead
            // (reliable delivery is the point of this tier).
            .congestion_control(CongestionControl::Block)
            .reliability(Reliability::Reliable)
            // Bulk rollout yields to telemetry on shared links (see
            // `BlobClientBuilder::priority`).
            .priority(Priority::DataLow)
            .cache(CacheConfig::default().max_samples(count as usize + 1))
            .sample_miss_detection(MissDetectionConfig::default().heartbeat(cfg.heartbeat))
            // Liveliness token: lets subscribers' `detect_late_publishers`
            // notice this publisher (re)appearing and re-query its history.
            .publisher_detection()
            .await
            .map_err(BlobError::zenoh)?;

        publisher
            .put(crate::wire::encode(&(
                crate::wire::WIRE_VERSION,
                FanoutFrame::Manifest(task_manifest.clone()),
            ))?)
            .await
            .map_err(BlobError::zenoh)?;

        let mut reader = tokio::task::spawn_blocking(move || std::fs::File::open(&path))
            .await
            .map_err(|e| BlobError::Protocol(format!("open task: {e}")))??;
        for index in 0..count {
            let ob = outboard.clone();
            let byte_range = chunks.byte_range(index);
            let (r, slice) = tokio::task::spawn_blocking(
                move || -> (std::fs::File, std::io::Result<Vec<u8>>) {
                    let slice =
                        verify::encode_slice(&reader, &*ob, verify::chunk_range(byte_range));
                    (reader, slice)
                },
            )
            .await
            .map_err(|e| BlobError::Protocol(format!("encode task: {e}")))?;
            reader = r;
            publisher
                .put(crate::wire::encode(&(
                    crate::wire::WIRE_VERSION,
                    FanoutFrame::Slice { index, bao: slice? },
                ))?)
                .await
                .map_err(BlobError::zenoh)?;
        }
        zdebug!(id = %task_manifest.id, chunks = count, "fanout published; serving cache");

        // All frames are in the cache; keep the publisher (and thus replay +
        // heartbeat) alive until shutdown.
        stop2.notified().await;
        Ok(())
    });

    Ok((manifest, FanoutHandle { stop, join }))
}

/// Receive a fanned-out blob into `dest` (via `<dest>.part`, atomically
/// renamed on completion, honoring [`FanoutConfig::overwrite`]), verifying
/// every slice against the manifest root — pin it by passing `expected_root`.
/// Works for subscribers that join *after* publication (history replay) and
/// across losses (recovery + heartbeat). Slices arriving before the manifest
/// frame are buffered (bounded); unacceptable manifest frames from hostile
/// co-publishers are skipped, not fatal.
///
/// Unlike `download_to` there is **no resume**: on error or cancellation the
/// partial is removed and a retry starts over (the publisher's cache replay
/// makes that cheap).
#[allow(clippy::too_many_arguments)] // transfer surface mirrors download_to
pub async fn receive_fanout(
    session: Arc<zenoh::Session>,
    prefix: &str,
    id: &str,
    expected_root: Option<Hash>,
    dest: &Path,
    sink: &dyn ProgressSink,
    cancel: &CancelToken,
    cfg: FanoutConfig,
) -> Result<TransferStats> {
    let started_at = tokio::time::Instant::now();
    let subscriber = session
        .declare_subscriber(fanout_key(prefix, id))
        // The declare-time history query replays the publisher's whole cache
        // (no depth limit) — this is what makes late joiners work; the
        // default 10 s query timeout is too tight for a large replay.
        .history(HistoryConfig::default().detect_late_publishers())
        .recovery(RecoveryConfig::default().heartbeat())
        .query_timeout(cfg.stall_timeout)
        .await
        .map_err(BlobError::zenoh)?;

    // Phase A: acquire an acceptable manifest, buffering early slices.
    let mut early: Vec<(u32, Vec<u8>)> = Vec::new();
    let mut early_bytes = 0usize;
    let mut deferred_reject: Option<BlobError> = None;
    let (m, chunks, root) = loop {
        if cancel.is_cancelled() {
            sink.emit(Progress::Cancelled {
                received: 0,
                total: 0,
            });
            return Err(BlobError::Cancelled {
                received: 0,
                total: 0,
            });
        }
        let sample = match tokio::time::timeout(cfg.stall_timeout, subscriber.recv_async()).await {
            Ok(Ok(sample)) => sample,
            _ => {
                return Err(deferred_reject.unwrap_or(BlobError::Incomplete {
                    received: 0,
                    total: 0,
                }));
            }
        };
        let Ok((version, frame)) =
            crate::wire::decode::<(u16, FanoutFrame)>(&sample.payload().to_bytes())
        else {
            continue;
        };
        if version != crate::wire::WIRE_VERSION {
            continue;
        }
        match frame {
            FanoutFrame::Manifest(m) => {
                let verdict = m
                    .validate(1 << 40)
                    .and_then(|()| {
                        if m.id != id {
                            return Err(BlobError::Protocol(format!(
                                "fanout manifest id {:?} does not match {id:?}",
                                m.id
                            )));
                        }
                        if let Some(expected) = expected_root
                            && m.root != expected
                        {
                            return Err(BlobError::RootMismatch {
                                expected,
                                actual: m.root,
                            });
                        }
                        // An empty blob carries no slices, so nothing later
                        // proves the root; check it here or a publisher could
                        // serve "verified" emptiness under any claimed root.
                        if m.total_len == 0 && m.root != Hash::of(b"") {
                            return Err(BlobError::RootMismatch {
                                expected: Hash::of(b""),
                                actual: m.root,
                            });
                        }
                        Ok(())
                    })
                    .and_then(|()| m.chunks());
                match verdict {
                    Ok(chunks) => {
                        let root: blake3::Hash = m.root.into();
                        break (m, chunks, root);
                    }
                    Err(e) => {
                        deferred_reject.get_or_insert(e);
                        continue; // a hostile co-publisher must not be fatal
                    }
                }
            }
            FanoutFrame::Slice { index, bao } => {
                if early.len() < EARLY_SLICE_MAX_FRAMES
                    && early_bytes + bao.len() <= EARLY_SLICE_MAX_BYTES
                {
                    early_bytes += bao.len();
                    early.push((index, bao));
                }
            }
        }
    };

    let count = chunks.count();
    sink.emit(Progress::Started {
        total_len: m.total_len,
        chunk_count: count,
    });
    if let Some(parent) = dest.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let part = {
        let mut p = dest.as_os_str().to_os_string();
        p.push(".part");
        PathBuf::from(p)
    };
    {
        let f = tokio::fs::File::create(&part).await?;
        f.set_len(m.total_len).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&part)
        .await?;
    let mut present: Vec<bool> = vec![false; count as usize];
    let mut received = 0u32;
    let mut stats = TransferStats::default();

    // Phase B: drain the early buffer, then the live/replayed stream. Any
    // error or cancellation removes the partial (fanout has no resume).
    let result: Result<()> = async {
        for (index, bao) in early.drain(..) {
            apply_slice(
                &m,
                &chunks,
                &root,
                &mut file,
                &mut present,
                &mut received,
                &mut stats,
                sink,
                index,
                &bao,
            )
            .await?;
        }
        while received < count {
            if cancel.is_cancelled() {
                sink.emit(Progress::Cancelled {
                    received,
                    total: count,
                });
                return Err(BlobError::Cancelled {
                    received,
                    total: count,
                });
            }
            let sample =
                match tokio::time::timeout(cfg.stall_timeout, subscriber.recv_async()).await {
                    Ok(Ok(sample)) => sample,
                    _ => {
                        return Err(BlobError::Incomplete {
                            received,
                            total: count,
                        });
                    }
                };
            let Ok((version, frame)) =
                crate::wire::decode::<(u16, FanoutFrame)>(&sample.payload().to_bytes())
            else {
                continue;
            };
            if version != crate::wire::WIRE_VERSION {
                continue;
            }
            if let FanoutFrame::Slice { index, bao } = frame {
                apply_slice(
                    &m,
                    &chunks,
                    &root,
                    &mut file,
                    &mut present,
                    &mut received,
                    &mut stats,
                    sink,
                    index,
                    &bao,
                )
                .await?;
            }
        }
        file.sync_data().await?;
        if cfg.overwrite == Overwrite::Refuse && tokio::fs::try_exists(dest).await? {
            return Err(BlobError::DestinationExists(dest.to_path_buf()));
        }
        Ok(())
    }
    .await;
    drop(file);
    if let Err(e) = result {
        let _ = tokio::fs::remove_file(&part).await;
        return Err(e);
    }
    tokio::fs::rename(&part, dest).await?;
    sink.emit(Progress::Completed {
        path: dest.to_path_buf(),
    });
    stats.elapsed = started_at.elapsed();
    Ok(stats)
}

/// Verify one slice frame against the root and write its leaves; duplicates
/// and out-of-range indices are ignored, tampered frames counted + dropped.
#[allow(clippy::too_many_arguments)]
async fn apply_slice(
    m: &Manifest,
    chunks: &TransferChunks,
    root: &blake3::Hash,
    file: &mut tokio::fs::File,
    present: &mut [bool],
    received: &mut u32,
    stats: &mut TransferStats,
    sink: &dyn ProgressSink,
    index: u32,
    bao: &[u8],
) -> Result<()> {
    let count = chunks.count();
    if index >= count || present[index as usize] {
        return Ok(());
    }
    let mut leaves: Vec<(u64, Vec<u8>)> = Vec::new();
    let decoded = verify::decode_slice(
        root,
        m.total_len,
        verify::chunk_range(chunks.byte_range(index)),
        bao,
        |off, data| {
            leaves.push((off, data.to_vec()));
            Ok(())
        },
    );
    if decoded.is_err() {
        stats.rejected += 1;
        return Ok(());
    }
    for (off, data) in leaves {
        file.seek(SeekFrom::Start(off)).await?;
        file.write_all(&data).await?;
    }
    present[index as usize] = true;
    *received += 1;
    stats.chunks_fetched += 1;
    stats.bytes_fetched += chunks.len_of(index) as u64;
    sink.emit(Progress::Chunk {
        index,
        received: *received,
        total: count,
        bytes_received: stats.bytes_fetched,
    });
    Ok(())
}
