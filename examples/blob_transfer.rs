//! End-to-end Tier-1 transfer in one process: register a file, serve it,
//! download it back with a pinned root, print stats.
//!
//! ```bash
//! cargo run --example blob_transfer
//! ```

use std::sync::Arc;

use zblob::{BlobClient, BlobServer, BlobSpec, CancelToken, DownloadRequest, Progress};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(
        zenoh::open(zenoh::Config::default())
            .await
            .map_err(|e| e.to_string())?,
    );
    let prefix = "demo/blobs";

    // --- producer: write a source file and serve it -------------------------
    let dir = tempfile::tempdir()?;
    let src = dir.path().join("artifact.bin");
    std::fs::write(&src, vec![42u8; 3 * 1024 * 1024])?;

    let server = BlobServer::new(session.clone(), prefix);
    let manifest = server
        .register_file(BlobSpec::new("demo-blob").filename("artifact.bin"), &src)
        .await?;
    let handle = server.spawn().await?;
    println!("serving id={} root={}", manifest.id, manifest.root);

    // --- consumer: download with the root pinned ----------------------------
    let dest = dir.path().join("downloaded.bin");
    let client = BlobClient::new(session.clone(), prefix);
    let stats = client
        .download_to(
            &DownloadRequest::pinned("demo-blob", manifest.root),
            &dest,
            &|p: Progress| {
                if let Progress::Chunk {
                    received, total, ..
                } = p
                {
                    println!("  chunk {received}/{total}");
                }
            },
            &CancelToken::new(),
        )
        .await?;

    println!(
        "done: {} bytes in {:?} ({} MiB/s), verified against {}",
        stats.bytes_fetched,
        stats.elapsed,
        stats.throughput_bps() / (1024 * 1024),
        manifest.root
    );
    assert_eq!(std::fs::read(&src)?, std::fs::read(&dest)?);

    handle.shutdown().await?;
    session.close().await.map_err(|e| e.to_string())?;
    Ok(())
}
