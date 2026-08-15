//! Every "after" snippet in `docs/MIGRATION-v3.md`, compiled.
//!
//! Three repos will follow that document, and its previous version described
//! signatures that no longer existed — because nothing checked it. These
//! functions are never called; the assertion is that they *build*, so a
//! signature change that invalidates the guide fails here rather than in a
//! consumer.
//!
//! Keep them in the guide's order, and keep the section numbers in the names.

// These functions exist to be compiled, not called or read as exemplary
// Rust: the long parameter lists are the *inputs a consumer already has*, not
// a signature this crate proposes.
#![allow(dead_code, unused_variables, clippy::too_many_arguments)]

mod common;

use std::path::Path;
use std::sync::Arc;

use zblob::keys::{
    Tier2Tail, manifest_key, parse_id, parse_ranges, parse_tier2_tail, slice_selector, store_key,
    tree_key,
};
use zblob::wire::{ENC_SLICE, Ext};
use zblob::{
    BlobClient, BlobError, BlobId, BlobServer, BlobSpec, CancelToken, ChunkCompression,
    ContentStore, DownloadRequest, ErrorKind, Hash, HashAlgo, Manifest, Overwrite, Progress,
    ProgressSink, Publisher, PushConfig, PushPolicy, QueryPrefix, ServePrefix, SettleCoverage,
    StoreClient, TransferStats, TreeClient, TreeIndex, TreeServer, progress_channel,
};

/// §1 — prefixes are typed, and serving implies querying.
fn s1_typed_prefixes(session: &zenoh::Session) -> zblob::Result<()> {
    let serve = ServePrefix::new("demo/blobs")?;
    let query = QueryPrefix::from(&serve);
    // A client may name several origins; a server may not.
    let wildcard = QueryPrefix::new("v1/*/@blob/artifact")?;
    assert!(!wildcard.is_concrete());
    assert!(ServePrefix::try_from(wildcard).is_err());

    let _server = BlobServer::new(session, serve);
    let _client = BlobClient::new(session, query);
    Ok(())
}

/// §5 — transfers are call builders.
async fn s5_call_builders(
    client: &BlobClient,
    tree: &TreeClient,
    req: &DownloadRequest,
    dest: &Path,
    store: &Arc<dyn ContentStore>,
    holders: &[zblob::BlobProbe],
    sink: &dyn ProgressSink,
    cancel: &CancelToken,
    spec: BlobSpec,
    path: &Path,
    token: Vec<u8>,
) -> zblob::Result<()> {
    let _: TransferStats = client
        .download_to(req, dest)
        .progress(sink)
        .cancel(cancel)
        .await?;
    let _: TransferStats = client.download_to(req, dest).await?;
    let _: TransferStats = client
        .download_to(req, dest)
        .striped(holders)
        .progress(sink)
        .cancel(cancel)
        .await?;
    let _: Manifest = client
        .upload_file(spec, path)
        .token(token)
        .progress(sink)
        .cancel(cancel)
        .await?;
    let _: TransferStats = tree
        .download_tree(req, dest, store)
        .progress(sink)
        .cancel(cancel)
        .await?;

    // Overwrite is per transfer as well as per client.
    let _ = client
        .download_to(req, dest)
        .overwrite(Overwrite::Replace)
        .await?;
    Ok(())
}

/// §6 — publishing goes through `Publisher`.
async fn s6_publisher(
    session: &zenoh::Session,
    store_prefix: ServePrefix,
    tree_prefix: ServePrefix,
    index: &TreeIndex,
    store: &Arc<dyn ContentStore>,
    settle: std::time::Duration,
    hash: &Hash,
    bytes: &[u8],
) -> zblob::Result<()> {
    Publisher::new(session, store_prefix.clone())
        .snapshots(tree_prefix.clone())
        .coverage(SettleCoverage::All)
        .settle(settle)
        .publish(index, store)
        .await?;

    // The narrower forms the free functions used to provide.
    let p = Publisher::new(session, store_prefix.clone()).compression(ChunkCompression::default());
    p.chunk(hash, bytes).await?;
    let _: u32 = p.chunks(&[*hash], store).await?;
    let _: u32 = p.store(store).await?;

    let sp = Publisher::new(session, store_prefix).snapshots(tree_prefix);
    sp.index(index).await?;
    let _: u32 = sp.chunks_for(index, store).await?;
    // A `SnapshotPublisher` derefs to `Publisher`.
    let _: u32 = sp.store(store).await?;
    Ok(())
}

/// §7 — three fields became types.
fn s7_newtypes(manifest: &Manifest, index: &TreeIndex) -> zblob::Result<()> {
    let id: BlobId = BlobId::new("report-01")?;
    let parsed: BlobId = "report-01".parse()?;
    assert_eq!(id, parsed);
    assert!(BlobId::new("bad/id").is_err());

    // Reading one.
    let _: &str = id.as_str();
    let _: &str = &id;
    assert!(id == "report-01");
    let _: &BlobId = &manifest.id;

    // Keyed by `BlobId`, looked up by `&str`.
    let mut map = std::collections::HashMap::new();
    map.insert(id.clone(), ());
    assert!(map.contains_key("report-01"));

    // The algorithm is a type, and key builders take it.
    let _: HashAlgo = index.algo;
    let _ = store_key("p", HashAlgo::Blake3, &Hash::of(b"x"));

    // Ext accessors moved onto the type.
    let mut ext = Ext::new();
    ext.set_u32(zblob::wire::EXT_MAX_CHUNKS_PER_QUERY, 256)?;
    ext.set_u64(zblob::wire::EXT_MAX_BLOB_SIZE, 1 << 30)?;
    let _: Option<u32> = ext.get_u32(zblob::wire::EXT_MAX_CHUNKS_PER_QUERY);
    let _: Option<u64> = ext.get_u64(zblob::wire::EXT_MAX_BLOB_SIZE);
    let _: Option<u32> = manifest.max_chunks_per_query();
    Ok(())
}

/// §8 — key builders moved to `zblob::keys`, and two changed shape.
fn s8_keys() -> zblob::Result<()> {
    let _ = manifest_key("p", "id");
    let _ = tree_key("p", "id");
    let _ = slice_selector("p", "id", &[0..4, 9..12]);
    let _: Vec<std::ops::Range<u32>> = parse_ranges("ranges=0-4", 10, 512)?;

    // `parse_id` borrows.
    let borrowed: Option<&str> = parse_id("p", "p/id/manifest");
    assert_eq!(borrowed, Some("id"));

    // `parse_tier2_tail` is typed.
    match parse_tier2_tail("p", "p/blake3/abc") {
        Some(Tier2Tail::Two(algo, hex)) => assert_eq!((algo, hex), ("blake3", "abc")),
        other => panic!("unexpected tail: {other:?}"),
    }

    // Not keys, so still at the root.
    let framed = zblob::frame_chunk(b"x", ChunkCompression::default())?;
    assert_eq!(zblob::unframe_chunk(&framed)?, b"x");
    Ok(())
}

/// §9 — `BlobError` splits, and classifies.
fn s9_errors(err: BlobError) {
    if err.is_retriable() { /* transport; try again */ }
    if err.is_cancelled() { /* the caller's own decision */ }
    match err.kind() {
        ErrorKind::Integrity | ErrorKind::Protocol => {}
        ErrorKind::Usage => {}
        _ => {}
    }
    // The five that used to be `Protocol(String)`.
    let _ = matches!(
        err,
        BlobError::UnsafePath(_)
            | BlobError::InvalidPrefix(_)
            | BlobError::MalformedMessage(_)
            | BlobError::Usage(_)
            | BlobError::NotSettled(_)
            | BlobError::Task(_)
    );
}

/// §10 — the signature-change table.
async fn s10_signatures(
    session: &zenoh::Session,
    policy: Arc<dyn PushPolicy>,
    spool: &Path,
    store: Arc<dyn ContentStore>,
    index: TreeIndex,
    hash: &Hash,
) -> zblob::Result<()> {
    // `register` returns a Result (it may shard a large index).
    let tree = TreeServer::new(
        session,
        ServePrefix::new("p/store")?,
        ServePrefix::new("p/tree")?,
        store.clone(),
    );
    tree.register(index).await?;

    // Sessions are borrowed, not `Arc`ed.
    let _ = StoreClient::new(session, QueryPrefix::new("p/store")?);

    // Push bounds cannot be set into the void.
    let _ = BlobServer::builder(session, ServePrefix::new("p/blobs")?)
        .accept_push(
            PushConfig::new(policy, spool)
                .max_concurrent(2)
                .max_blob_size(64 << 20)
                .idle_timeout(std::time::Duration::from_secs(600)),
        )
        .build();

    // `ContentStore` reports I/O failure, and can batch.
    let _: bool = store.has(hash)?;
    let _: Option<Vec<u8>> = store.get(hash)?;
    let _: bool = store.remove(hash)?;
    let _: Vec<bool> = store.has_many(&[*hash])?;
    let _: Vec<Option<Vec<u8>>> = store.get_many(&[*hash])?;
    store.put_many(&[(*hash, b"x".as_slice())])?;
    store.for_each_hash(&mut |_h| Ok(()))?;

    // Encoding tags are typed.
    let enc = zenoh::bytes::Encoding::from(&ENC_SLICE);
    assert!(ENC_SLICE.matches(&enc));
    Ok(())
}

/// §11 — behaviour changes a consumer can observe.
fn s11_observable(mut a: TransferStats, b: TransferStats) {
    // `TransferStats` is summable.
    a += &b;
    let _ = a.clone() + b.clone();
    let _: TransferStats = [a, b].into_iter().sum();
}

/// §12 — what to adopt beyond the mechanical port.
async fn s12_new_capabilities(
    tree: &TreeClient,
    server: &BlobServer,
    client: &BlobClient,
    req: &DownloadRequest,
    store: &Arc<dyn ContentStore>,
    index: &TreeIndex,
    spec: BlobSpec,
    bytes: Vec<u8>,
    token: Vec<u8>,
) -> zblob::Result<()> {
    // Push from anything positional — no staging file.
    let _: Manifest = client
        .upload_source(spec, Arc::new(zblob::MemoryBlobSource::new(bytes)))
        .token(token)
        .await?;

    // One file out of a snapshot, no tree materialized.
    let _: Vec<u8> = tree.fetch_file(req, "etc/app.conf", store).await?;

    // A progress sink that reaches another task.
    let (sink, mut events) = progress_channel(64);
    sink.emit(Progress::Verifying);
    let _ = events.try_recv();

    // Server introspection.
    let _: Vec<BlobId> = server.registered().await;
    let _: Option<Manifest> = server.manifest("id").await;
    let _: bool = server.serves("id").await;

    // Snapshot navigation.
    let _: Option<&zblob::Entry> = index.entry("etc/app.conf");
    let _: &[zblob::Entry] = index.entries();
    let _: Option<&[zblob::ChunkRef]> = index.file_chunks("etc/app.conf");
    for (path, size, chunks) in index.files() {
        let _ = (path, size, chunks.len());
    }
    Ok(())
}

/// The guide claims `ReadAtSize` is implementable downstream. It is only true
/// if its supertraits are reachable, which is why they are re-exported.
struct MySource(Vec<u8>);
impl zblob::ReadAt for MySource {
    fn read_at(&self, pos: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.0.as_slice().read_at(pos, buf)
    }
}
impl zblob::Size for MySource {
    fn size(&self) -> std::io::Result<Option<u64>> {
        Ok(Some(self.0.len() as u64))
    }
}
fn a_downstream_blob_source(bytes: Vec<u8>) -> Box<dyn zblob::ReadAtSize> {
    Box::new(MySource(bytes))
}

/// Not a compile check: one end-to-end run, so this file fails if the shapes
/// above compile but do not actually work together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_guide_s_shapes_work_end_to_end() {
    let session = common::open_session().await;
    let prefix = common::unique_prefix();
    let data = common::pseudo_random(64 * 1024, 990);

    let serve = ServePrefix::new(&prefix).unwrap();
    let server = BlobServer::new(&session, serve.clone());
    let manifest = server
        .register_source(
            BlobSpec::new("guide"),
            Arc::new(zblob::MemoryBlobSource::new(data.clone())),
        )
        .await
        .unwrap();
    assert!(server.serves("guide").await);
    assert_eq!(server.manifest("guide").await.unwrap(), manifest);
    let handle = server.spawn().await.unwrap();

    let (sink, mut events) = progress_channel(256);
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.bin");
    let stats = BlobClient::new(&session, QueryPrefix::from(&serve))
        .download_to(&DownloadRequest::pinned("guide", manifest.root), &dest)
        .progress(&sink)
        .overwrite(Overwrite::Replace)
        .await
        .unwrap();

    assert_eq!(std::fs::read(&dest).unwrap(), data);
    assert!(stats.queries > 0, "the query counter must count");
    drop(sink);
    let mut seen = 0;
    while events.recv().await.is_some() {
        seen += 1;
    }
    assert!(seen > 0, "progress must reach the channel");

    handle.shutdown().await.unwrap();
    session.close().await.unwrap();
}
