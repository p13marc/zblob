//! End-to-end Tier-2 directory sync in one process: snapshot a tree, serve
//! it, sync it (twice — the second pull after an edit transfers only the
//! changed chunks).
//!
//! ```bash
//! cargo run --example tree_sync
//! ```

use std::sync::Arc;

use zblob::{
    CancelToken, CdcParams, ContentStore, DownloadRequest, MemoryStore, TreeClient, TreeServer,
    build_tree,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(
        zenoh::open(zenoh::Config::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let (store_prefix, tree_prefix) = ("demo/store", "demo/tree");

    // --- producer: build + serve a snapshot ---------------------------------
    let src = tempfile::tempdir()?;
    std::fs::create_dir_all(src.path().join("sub"))?;
    std::fs::write(src.path().join("big.bin"), vec![7u8; 2 * 1024 * 1024])?;
    std::fs::write(src.path().join("sub/notes.md"), b"# hello\n")?;

    let server_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());
    let cdc = CdcParams::default();
    let index = build_tree(src.path(), "snap-1", &cdc, &*server_store)?;
    println!(
        "snapshot snap-1: {} files, {} chunks, root {}",
        index.file_count(),
        index.needed_chunks().len(),
        index.root_hash
    );

    let server = TreeServer::new(
        session.clone(),
        store_prefix,
        tree_prefix,
        server_store.clone(),
    );
    server.register(index.clone()).await;
    let handle = server.clone().spawn().await?;

    // --- consumer: sync (pinned), then re-sync after an edit ----------------
    let dest = tempfile::tempdir()?;
    let client = TreeClient::new(session.clone(), store_prefix, tree_prefix);
    let client_store: Arc<dyn ContentStore> = Arc::new(MemoryStore::new());

    let stats = client
        .download_tree(
            &DownloadRequest::pinned("snap-1", index.root_hash),
            dest.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await?;
    println!("first sync: fetched {} chunks", stats.chunks_fetched);

    // Edit one file, snapshot again, re-sync: only the delta moves.
    std::fs::write(src.path().join("sub/notes.md"), b"# hello, edited\n")?;
    let index2 = build_tree(src.path(), "snap-2", &cdc, &*server_store)?;
    server.register(index2.clone()).await;

    let stats = client
        .download_tree(
            &DownloadRequest::pinned("snap-2", index2.root_hash),
            dest.path(),
            &client_store,
            &(),
            &CancelToken::new(),
        )
        .await?;
    println!(
        "re-sync after edit: fetched {} chunks, reused {}",
        stats.chunks_fetched, stats.chunks_resumed
    );
    assert_eq!(
        std::fs::read(dest.path().join("sub/notes.md"))?,
        b"# hello, edited\n"
    );

    handle.shutdown().await?;
    session.close().await.map_err(|e| e.to_string())?;
    Ok(())
}
