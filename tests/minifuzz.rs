//! Deterministic randomized ("mini-fuzz") coverage of every public parser and
//! decoder: adversarial bytes must produce errors, never panics. Real
//! libFuzzer targets live in `fuzz/`; these run on every `cargo test`.

use zblob::keys::parse_ranges;
use zblob::{BlobId, Hash, HashAlgo, Manifest, TreeIndex, wire};

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
            let refmt = zblob::keys::format_ranges(&ranges);
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
        // Reaching the next line at all is the assertion: a decoder that
        // panicked would abort the test. (This used to read
        // `decode(..).is_err() || !bytes.is_empty()`, whose second arm is true
        // for every input the generator produces — so it asserted nothing, and
        // would have held even for a decoder that accepted random bytes as a
        // manifest.) What *is* checked is that garbage never decodes
        // successfully into a structure the rest of the crate would trust.
        if let Ok(m) = wire::decode::<Manifest>(&bytes) {
            assert!(
                m.validate(u64::MAX).is_err(),
                "random bytes decoded to a manifest that passes validation: {m:?}"
            );
        }
        if let Ok(i) = wire::decode::<TreeIndex>(&bytes) {
            assert!(
                i.validate().is_err(),
                "random bytes decoded to an index that passes validation"
            );
        }
        if let Ok(a) = wire::decode::<wire::Availability>(&bytes)
            && a.validate(u32::MAX).is_ok()
        {
            // Anything that validates must be self-consistent, or the client
            // would index past the end of a bitfield a peer sent.
            assert_eq!(
                a.bits.len(),
                a.chunk_count.div_ceil(8) as usize,
                "a validated availability must size its bitfield to its count"
            );
        }
        // The four validators added in v3 are attacker-input boundaries too,
        // and had no fuzz coverage at all.
        let _ = wire::decode::<wire::WantList>(&bytes).map(|w| w.validate(4096));
        let _ = wire::decode::<wire::HaveBits>(&bytes).map(|h| h.validate(4096));
        let _ = wire::decode::<wire::IndexDescriptor>(&bytes).map(|d| d.validate(1 << 20));
        let _ = wire::decode::<wire::TreeProbe>(&bytes).map(|p| p.validate());
    }
    // Truncations of a *valid* encoding must error, not panic.
    let m = Manifest {
        version: wire::WIRE_VERSION,
        id: BlobId::new("fuzz").unwrap(),
        filename: Some("f".into()),
        total_len: 123_456,
        chunk_size: 65_536,
        root: Hash::of(b"x"),
        created_ms: 1,
        ext: zblob::wire::Ext::new(),
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
        // Two *distinct* paths. They used to be the same string, so
        // `validate` returned `duplicate entry path` before it ever looked at
        // the symlink — and the generated `target`, the hostile input this
        // test exists for, was never examined.
        let dir_path = rng.ascii(40);
        let link_path = rng.ascii(40);
        let target = rng.ascii(40);
        let entries = vec![
            zblob::Entry::Dir {
                path: dir_path,
                mode: (rng.next() & 0xffff) as u32,
                mtime: rng.next() as i64,
            },
            zblob::Entry::Symlink {
                path: link_path,
                target,
            },
        ];
        let index = TreeIndex {
            version: wire::WIRE_VERSION,
            id: BlobId::new("fuzz").unwrap(),
            algo: HashAlgo::Blake3,
            cdc: zblob::CdcParams::default(),
            entries,
            root_hash: Hash::of(b"whatever"),
        };
        // Must not panic — and must not stop at "duplicate entry path", which
        // is what the same-path version did on *every* iteration, so the
        // generated symlink target was never examined.
        if let Err(e) = index.validate() {
            assert!(
                !e.to_string().contains("duplicate entry path"),
                "the two entries must have distinct paths, or the symlink arm \
                 is unreachable: {e}"
            );
        }
    }

    // Discriminating power: the loop above only proves "no panic", which a
    // validator that accepted everything would also satisfy. These are the
    // shapes it must actually refuse, through the same entry point.
    for (name, path, target) in [
        ("traversal", "a", "../../etc/passwd"),
        ("absolute target", "a", "/etc/passwd"),
        ("traversing path", "../escape", "x"),
        ("absolute path", "/abs", "x"),
    ] {
        let index = TreeIndex {
            version: wire::WIRE_VERSION,
            id: BlobId::new("fuzz").unwrap(),
            algo: HashAlgo::Blake3,
            cdc: zblob::CdcParams::default(),
            entries: vec![zblob::Entry::Symlink {
                path: path.into(),
                target: target.into(),
            }],
            root_hash: Hash::of(b"whatever"),
        };
        assert!(index.validate().is_err(), "{name} must be refused");
    }
}
