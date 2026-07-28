//! Contract tests every [`ContentStore`] implementation must satisfy,
//! executed against *all* of them (memory, dir, dir+zstd, dir+encryption,
//! dir+verify-on-read).
//!
//! These exist because a store's methods are consulted independently by the
//! download path — `has` decides whether to fetch, `get` decides what to
//! materialize — so any disagreement between them is a bug that no
//! single-implementation happy-path test can see. (One did hide here: a
//! sealed store opened without its key answered `has` = true and `get` =
//! None, which wedged downloads permanently.)

mod common;

use std::sync::Arc;

use zblob::{ChunkCompression, ContentStore, DirStore, Hash, MemoryStore};

/// Constructs one store configuration in a fresh directory.
type StoreFactory = Box<dyn Fn(&std::path::Path) -> Arc<dyn ContentStore>>;

/// Every store configuration under test, by name.
fn stores() -> Vec<(&'static str, StoreFactory)> {
    // `mut` is only exercised by the feature-gated pushes below; with default
    // features there are none.
    #[allow(unused_mut)]
    let mut v: Vec<(&'static str, StoreFactory)> = vec![
        (
            "memory",
            Box::new(|_: &std::path::Path| Arc::new(MemoryStore::new()) as Arc<dyn ContentStore>),
        ),
        (
            "dir",
            Box::new(|p: &std::path::Path| {
                Arc::new(DirStore::open(p).unwrap()) as Arc<dyn ContentStore>
            }),
        ),
        (
            "dir+verify_on_read",
            Box::new(|p: &std::path::Path| {
                Arc::new(DirStore::open(p).unwrap().with_verify_on_read(true))
                    as Arc<dyn ContentStore>
            }),
        ),
    ];
    #[cfg(feature = "zstd")]
    v.push((
        "dir+zstd",
        Box::new(|p: &std::path::Path| {
            Arc::new(
                DirStore::open(p)
                    .unwrap()
                    .with_compression(ChunkCompression::Zstd { level: 3 }),
            ) as Arc<dyn ContentStore>
        }),
    ));
    #[cfg(feature = "encryption")]
    v.push((
        "dir+encryption",
        Box::new(|p: &std::path::Path| {
            Arc::new(
                DirStore::open(p)
                    .unwrap()
                    .with_encryption(zblob::StoreKey([9u8; 32])),
            ) as Arc<dyn ContentStore>
        }),
    ));
    let _ = ChunkCompression::default();
    v
}

/// A spread of payload shapes: empty, tiny, compressible, incompressible,
/// and one large enough to cross buffer boundaries.
fn payloads() -> Vec<Vec<u8>> {
    vec![
        Vec::new(),
        b"x".to_vec(),
        vec![b'a'; 100_000],                 // highly compressible
        common::pseudo_random(64 * 1024, 5), // incompressible
        common::pseudo_random(300_000, 6),
    ]
}

#[test]
fn every_store_satisfies_the_contract() {
    for (name, make) in stores() {
        let dir = tempfile::tempdir().unwrap();
        let store = make(dir.path());

        for payload in payloads() {
            let h = Hash::of(&payload);
            let label = format!("{name}/{}B", payload.len());

            // 1. Absent before put.
            assert!(!store.has(&h), "{label}: has() true before put");
            assert!(store.get(&h).is_none(), "{label}: get() Some before put");
            assert!(
                !store.remove(&h).unwrap(),
                "{label}: remove() reported an absent chunk as removed"
            );

            // 2. put → has ⇒ get, and get returns exactly what was put.
            store.put(&h, &payload).unwrap();
            assert!(store.has(&h), "{label}: has() false after put");
            let got = store
                .get(&h)
                .unwrap_or_else(|| panic!("{label}: has()=true but get()=None"));
            assert_eq!(got, payload, "{label}: get() returned different bytes");

            // 3. put is idempotent (re-publishing a chunk must be a no-op).
            store.put(&h, &payload).unwrap();
            assert_eq!(
                store.get(&h).unwrap(),
                payload,
                "{label}: re-put changed it"
            );

            // 4. hashes() includes it, exactly once.
            let listed = store.hashes().unwrap();
            assert_eq!(
                listed.iter().filter(|x| **x == h).count(),
                1,
                "{label}: hashes() must list each chunk exactly once"
            );

            // 5. remove → absent again, and reports what it did.
            assert!(store.remove(&h).unwrap(), "{label}: remove() said absent");
            assert!(!store.has(&h), "{label}: still present after remove");
            assert!(
                store.get(&h).is_none(),
                "{label}: still gettable after remove"
            );
            assert!(
                !store.hashes().unwrap().contains(&h),
                "{label}: hashes() still lists a removed chunk"
            );
        }

        // 6. hashes() is complete: everything put and not removed is listed.
        let all: Vec<Hash> = payloads()
            .iter()
            .map(|p| {
                let h = Hash::of(p);
                store.put(&h, p).unwrap();
                h
            })
            .collect();
        let listed = store.hashes().unwrap();
        for h in &all {
            assert!(listed.contains(h), "{name}: hashes() missed a stored chunk");
        }
    }
}

/// Concurrent puts of the *same* chunk (the v1 fixed-temp-name race) and of
/// different chunks must both leave every chunk intact and readable.
#[test]
fn concurrent_puts_are_safe() {
    for (name, make) in stores() {
        let dir = tempfile::tempdir().unwrap();
        let store = make(dir.path());
        let shared = common::pseudo_random(200_000, 7);
        let shared_hash = Hash::of(&shared);

        std::thread::scope(|scope| {
            for t in 0..8 {
                let store = store.clone();
                let shared = shared.clone();
                scope.spawn(move || {
                    // Same chunk from every thread…
                    store.put(&shared_hash, &shared).unwrap();
                    // …plus a thread-unique one.
                    let unique = common::pseudo_random(50_000, 100 + t);
                    store.put(&Hash::of(&unique), &unique).unwrap();
                });
            }
        });

        assert_eq!(
            store.get(&shared_hash).unwrap(),
            shared,
            "{name}: concurrent puts of one chunk corrupted it"
        );
        for t in 0..8u64 {
            let unique = common::pseudo_random(50_000, 100 + t);
            assert_eq!(
                store.get(&Hash::of(&unique)).unwrap(),
                unique,
                "{name}: concurrent distinct puts lost data"
            );
        }
        // No temp-file debris counted as content.
        for h in store.hashes().unwrap() {
            assert!(store.get(&h).is_some(), "{name}: hashes() listed a phantom");
        }
    }
}

/// A store must never claim a chunk it cannot actually return. This is the
/// exact shape of the bug that made a sealed store wedge downloads forever:
/// `has` looked only at file existence.
#[cfg(feature = "encryption")]
#[test]
fn a_store_never_claims_what_it_cannot_decode() {
    let dir = tempfile::tempdir().unwrap();
    let payload = common::pseudo_random(80_000, 8);
    let h = Hash::of(&payload);

    // Written sealed…
    let sealed = DirStore::open(dir.path())
        .unwrap()
        .with_encryption(zblob::StoreKey([1u8; 32]));
    sealed.put(&h, &payload).unwrap();
    assert!(sealed.has(&h) && sealed.get(&h).unwrap() == payload);

    // …reopened without the key, or with the wrong one: `has` must agree with
    // `get`, and the data must survive untouched for the rightful key.
    for other in [
        DirStore::open(dir.path()).unwrap(),
        DirStore::open(dir.path())
            .unwrap()
            .with_encryption(zblob::StoreKey([2u8; 32])),
    ] {
        assert_eq!(
            other.has(&h),
            other.get(&h).is_some(),
            "has()/get() disagree for an undecodable chunk"
        );
        assert!(!other.has(&h), "claimed a chunk it cannot decode");
    }
    let reopened = DirStore::open(dir.path())
        .unwrap()
        .with_encryption(zblob::StoreKey([1u8; 32]));
    assert_eq!(
        reopened.get(&h).unwrap(),
        payload,
        "a keyless reader destroyed sealed data"
    );
}

/// Whatever a store hands back must hash to the key it was asked for — the
/// invariant the whole content-addressed model rests on.
#[test]
fn returned_bytes_always_match_their_address() {
    for (name, make) in stores() {
        let dir = tempfile::tempdir().unwrap();
        let store = make(dir.path());
        for payload in payloads() {
            let h = Hash::of(&payload);
            store.put(&h, &payload).unwrap();
        }
        for h in store.hashes().unwrap() {
            if let Some(bytes) = store.get(&h) {
                assert_eq!(
                    Hash::of(&bytes),
                    h,
                    "{name}: get() returned bytes that do not hash to their key"
                );
            }
        }
    }
}
