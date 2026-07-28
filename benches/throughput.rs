//! Criterion benchmarks for the throughput-critical primitives: hashing,
//! outboard computation, bao slice encode/verify, and CDC chunking.
//!
//! Run with `cargo bench`; CI only compiles them (`cargo bench --no-run`).

use std::io::Cursor;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};

/// Deterministic pseudo-random bytes (xorshift64).
fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut x = seed | 1;
    for _ in 0..len {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.push((x & 0xff) as u8);
    }
    out
}

const MB8: usize = 8 * 1024 * 1024;

fn bench_hash(c: &mut Criterion) {
    let data = pseudo_random(MB8, 1);
    let mut g = c.benchmark_group("hash");
    g.throughput(Throughput::Bytes(MB8 as u64));
    g.bench_function("blake3_8MiB", |b| b.iter(|| zblob::Hash::of(&data)));
    g.finish();
}

fn bench_cdc(c: &mut Criterion) {
    let data = pseudo_random(MB8, 2);
    let params = zblob::CdcParams::default();
    let mut g = c.benchmark_group("cdc");
    g.throughput(Throughput::Bytes(MB8 as u64));
    g.bench_function("fastcdc_level2_8MiB", |b| {
        b.iter(|| {
            let mut n = 0usize;
            for chunk in params.chunk_reader(Cursor::new(&data)) {
                n += chunk.unwrap().len();
            }
            n
        })
    });
    g.finish();
}

fn bench_tree_build(c: &mut Criterion) {
    // End-to-end Tier-2 snapshot build: walk + CDC + hash + store.
    let dir = tempfile::tempdir().unwrap();
    for i in 0..4 {
        std::fs::write(
            dir.path().join(format!("f{i}.bin")),
            pseudo_random(MB8 / 4, 3 + i),
        )
        .unwrap();
    }
    let mut g = c.benchmark_group("tree");
    g.throughput(Throughput::Bytes(MB8 as u64));
    g.sample_size(20);
    g.bench_function("build_tree_8MiB", |b| {
        b.iter(|| {
            let store = zblob::MemoryStore::new();
            zblob::build_tree(dir.path(), "bench", &zblob::CdcParams::default(), &store).unwrap()
        })
    });
    g.finish();
}

criterion_group!(benches, bench_hash, bench_cdc, bench_tree_build);
criterion_main!(benches);
