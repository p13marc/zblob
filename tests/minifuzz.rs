//! Deterministic randomized ("mini-fuzz") coverage of every public parser and
//! decoder: adversarial bytes must produce errors, never panics. Real
//! libFuzzer targets live in `fuzz/`; these run on every `cargo test`.

use zblob::{Hash, Manifest, TreeIndex, parse_ranges, wire};

/// xorshift64 byte stream (no rand dependency, reproducible).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = (self.next() as usize) % (max_len + 1);
        (0..len).map(|_| (self.next() & 0xff) as u8).collect()
    }
    fn ascii(&mut self, max_len: usize) -> String {
        let len = (self.next() as usize) % (max_len + 1);
        (0..len)
            .map(|_| (b' ' + (self.next() % 95) as u8) as char)
            .collect()
    }
}

#[test]
fn hash_from_str_never_panics() {
    let mut rng = Rng(0xF00D);
    for _ in 0..20_000 {
        let s = rng.ascii(80);
        let _ = s.parse::<Hash>(); // must not panic
    }
    // Valid hex always roundtrips.
    for i in 0..100u64 {
        let h = Hash::of(&i.to_le_bytes());
        assert_eq!(h.to_string().parse::<Hash>().unwrap(), h);
        assert_eq!(
            h.to_string().to_uppercase().parse::<Hash>().unwrap(),
            h,
            "uppercase hex accepted"
        );
    }
}

#[test]
fn parse_ranges_never_panics_and_roundtrips() {
    let mut rng = Rng(0xBEEF);
    for _ in 0..20_000 {
        let params = rng.ascii(60);
        let _ = parse_ranges(&params, 1000, 512); // must not panic
        let _ = parse_ranges(&format!("v=2&ranges={params}"), 1000, 512);
    }
    // Structured adversarial digits.
    for _ in 0..5_000 {
        let a = rng.next() % 2000;
        let b = rng.next() % 2000;
        let c = rng.next() % 2000;
        let spec = format!("v=2&ranges={a}-{b},{c}");
        if let Ok(ranges) = parse_ranges(&spec, 1000, 512) {
            // Anything accepted must be sorted, disjoint, in bounds.
            let mut last = 0;
            for r in &ranges {
                assert!(
                    r.start >= last && r.start < r.end && r.end <= 1000,
                    "{spec}"
                );
                last = r.end;
            }
            // And re-format to the same accepted value.
            let refmt = zblob::format_ranges(&ranges);
            assert_eq!(
                parse_ranges(&format!("v=2&ranges={refmt}"), 1000, 512).unwrap(),
                ranges
            );
        }
    }
}

#[test]
fn wire_decoders_never_panic_on_garbage() {
    let mut rng = Rng(0xCAFE);
    for _ in 0..20_000 {
        let bytes = rng.bytes(200);
        assert!(
            wire::decode::<Manifest>(&bytes).is_err() || !bytes.is_empty(),
            "decoding must never panic"
        );
        let _ = wire::decode::<TreeIndex>(&bytes);
        let _ = wire::decode::<wire::Availability>(&bytes);
    }
    // Truncations of a *valid* encoding must error, not panic.
    let m = Manifest {
        version: 2,
        id: "fuzz".into(),
        filename: Some("f".into()),
        total_len: 123_456,
        chunk_size: 65_536,
        root: Hash::of(b"x"),
        created_ms: 1,
    };
    let full = wire::encode(&m).unwrap();
    for cut in 0..full.len() {
        assert!(wire::decode::<Manifest>(&full[..cut]).is_err());
    }
    assert_eq!(wire::decode::<Manifest>(&full).unwrap(), m);
}

#[test]
fn index_validation_never_panics_on_hostile_paths() {
    let mut rng = Rng(0xD00D);
    for _ in 0..5_000 {
        let path = rng.ascii(40);
        let target = rng.ascii(40);
        let entries = vec![
            zblob::Entry::Dir {
                path: path.clone(),
                mode: (rng.next() & 0xffff) as u32,
                mtime: rng.next() as i64,
            },
            zblob::Entry::Symlink {
                path: path.clone(),
                target,
            },
        ];
        let index = TreeIndex {
            version: 2,
            id: "fuzz".into(),
            algo: Hash::ALGO.into(),
            cdc: zblob::CdcParams::default(),
            entries,
            root_hash: Hash::of(b"whatever"),
        };
        let _ = index.validate(); // must not panic (root mismatch or path error)
    }
}
