//! Serving several snapshots durably from one store, and reclaiming the disk.
//!
//! The question every producer eventually asks: *how do I keep serving more
//! than one snapshot, survive a restart, and not grow without bound?* The
//! pieces have always been here — [`DirStore`], [`gc::SnapshotTags`],
//! [`gc::sweep`], [`seed`] — and they were easy to miss, so this puts them in
//! one place.
//!
//! ```bash
//! cargo run --example durable_store
//! ```
//!
//! What it demonstrates, in order:
//!
//! 1. **`DirStore`, not `MemoryStore`.** A `MemoryStore` caps a producer at one
//!    live snapshot (build the next and the previous one's chunks must be
//!    dropped first) and loses everything on restart, so nothing survives to
//!    dedup against and every snapshot re-transfers in full. `DirStore` is the
//!    server-side default for anything that outlives a process.
//! 2. **Several snapshots, one store.** Content addressing means the shared
//!    chunks are stored once no matter how many snapshots reference them.
//! 3. **Incremental builds.** `build_tree_from` reuses a parent's chunk
//!    references for files whose size and mtime are unchanged, so the producer
//!    stops re-hashing a mostly-static tree on every snapshot.
//! 4. **Tags and sweeping.** A tag is what keeps a snapshot's chunks alive.
//!    Unregister and untag one, sweep, and only its *unshared* chunks go.
//! 5. **The temp tag.** Downloads registered against the same `TempTags` are
//!    protected from a concurrent sweep — the chunks they have fetched but not
//!    yet materialized look exactly like garbage otherwise.

use std::sync::Arc;

use zblob::{
    CancelToken, CdcParams, ContentStore, DirStore, DownloadRequest, QueryPrefix, ServePrefix,
    TreeClient, TreeServer, build_tree, build_tree_from, gc,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(
        zenoh::open(zenoh::Config::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let store_serve = ServePrefix::new("demo/store")?;
    let tree_serve = ServePrefix::new("demo/tree")?;

    // --- a durable store, shared by every snapshot this producer serves -----
    let state = tempfile::tempdir()?;
    let store: Arc<dyn ContentStore> = Arc::new(DirStore::open(state.path().join("chunks"))?);
    let tags = gc::SnapshotTags::open(state.path().join("tags"))?;
    let temps = Arc::new(gc::TempTags::new());
    let cdc = CdcParams::default();

    // --- snapshot 1 ---------------------------------------------------------
    let src = tempfile::tempdir()?;
    std::fs::create_dir_all(src.path().join("etc"))?;
    // Varied bytes, so content-defined chunking actually cuts this into many
    // chunks — a constant buffer gives the gear hash nothing to trigger on and
    // makes a dedup demo look like nothing is happening.
    let mut payload = Vec::with_capacity(3 * 1024 * 1024);
    let mut x: u64 = 0x2545F4914F6CDD1D;
    while payload.len() < 3 * 1024 * 1024 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        payload.extend_from_slice(&x.to_le_bytes());
    }
    std::fs::write(src.path().join("big.bin"), &payload)?;
    std::fs::write(src.path().join("etc/config.toml"), b"mode = 'a'\n")?;
    let v1 = build_tree(src.path(), "v1", &cdc, &*store)?;
    tags.set("v1", &v1)?;
    println!(
        "v1: {} files, {} chunks, root {}",
        v1.file_count(),
        v1.needed_chunks().len(),
        v1.root_hash
    );

    // --- snapshot 2: one small file changed --------------------------------
    //
    // Note the size change. `build_tree_from` decides "unchanged" from
    // (size, mtime), so a same-length edit within the same second would be
    // *missed* — a real hazard for exactly this kind of config file, and one
    // this example hit on its first draft. A producer that cannot wait out the
    // filesystem's mtime granularity should use `build_tree`, which reads
    // everything and cannot be fooled.
    std::fs::write(
        src.path().join("etc/config.toml"),
        b"mode = 'b'\nextra = true\n",
    )?;
    // Incremental: `big.bin` is untouched, so its chunks are reused and the
    // file is never read. Only the changed file is re-chunked.
    let v2 = build_tree_from(src.path(), "v2", &cdc, &*store, Some(&v1))?;
    tags.set("v2", &v2)?;

    let stored = store.hashes()?.len();
    let shared = v1
        .needed_chunks()
        .iter()
        .filter(|h| v2.needed_chunks().contains(h))
        .count();
    println!(
        "v2: {} chunks, of which {shared} shared with v1 — {stored} distinct chunks on disk \
         for two snapshots totalling {} references",
        v2.needed_chunks().len(),
        v1.needed_chunks().len() + v2.needed_chunks().len()
    );

    // --- serve both at once -------------------------------------------------
    let server = TreeServer::new(
        &session,
        store_serve.clone(),
        tree_serve.clone(),
        store.clone(),
    );
    server.register(v1.clone()).await?;
    server.register(v2.clone()).await?;
    let handle = server.clone().spawn().await?;

    // --- a consumer, protected from the sweep below --------------------------
    let dest = tempfile::tempdir()?;
    let client = TreeClient::builder(
        &session,
        QueryPrefix::from(&store_serve),
        QueryPrefix::from(&tree_serve),
    )
    // The same registry the sweep uses: without this, a sweep running during
    // the download is free to collect chunks it has already fetched.
    .temp_tags(temps.clone())
    .build();
    let client_store: Arc<dyn ContentStore> = Arc::new(DirStore::open(dest.path().join("chunks"))?);
    let out = dest.path().join("tree");
    let stats = client
        .download_tree(
            &DownloadRequest::pinned("v2", v2.root_hash),
            &out,
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await?;
    println!(
        "consumer: fetched {} chunks in {} queries",
        stats.chunks_fetched, stats.queries
    );

    // --- retire v1 and reclaim only what it alone held ----------------------
    server.unregister("v1").await;
    tags.remove("v1")?;
    // v2 is still registered, so pass it as an extra root: a sweep is not
    // atomic against concurrent registration, and `extra_roots` is how a
    // caller says "this one is live even if it is not tagged".
    let swept = gc::sweep(&*store, &tags, &temps, [&v2])?;
    println!(
        "after retiring v1: kept {} chunks, removed {} — the shared ones stayed",
        swept.kept, swept.removed
    );
    assert!(
        v2.needed_chunks().iter().all(|h| store.has(h).unwrap()),
        "v2 must still be fully servable after the sweep"
    );

    // A second sweep with nothing tagged and nothing live reclaims the rest.
    tags.remove("v2")?;
    server.unregister("v2").await;
    let swept = gc::sweep(&*store, &tags, &temps, [])?;
    println!("after retiring v2: removed {} more", swept.removed);

    handle.shutdown().await?;
    session.close().await.map_err(|e| e.to_string())?;
    Ok(())
}
