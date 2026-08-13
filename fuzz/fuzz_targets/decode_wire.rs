//! Fuzz the wire-v3 control messages that arrive from untrusted peers.
//!
//! Each of these has a `validate` whose job is to bound what a remote peer can
//! make this process do — allocate, iterate, believe. Decoding must never
//! panic, and whatever validation *accepts* must satisfy the invariants the
//! rest of the crate relies on.
#![no_main]
use libfuzzer_sys::fuzz_target;
use zblob::wire;

fuzz_target!(|data: &[u8]| {
    // A want-list bounds a server's work before it touches its store.
    if let Ok(w) = wire::decode::<wire::WantList>(data)
        && w.validate(wire::MAX_WANT_LIST).is_ok()
    {
        assert!(!w.hashes.is_empty() && w.hashes.len() <= wire::MAX_WANT_LIST);
        let mut seen = std::collections::HashSet::new();
        assert!(w.hashes.iter().all(|h| seen.insert(*h)), "duplicates accepted");
    }

    // A probe reply must never claim more than the question it answers.
    if let Ok(b) = wire::decode::<wire::HaveBits>(data) {
        let asked = b.count as usize;
        if b.validate(asked).is_ok() {
            assert_eq!(b.bits.len(), b.count.div_ceil(8) as usize);
            assert!(b.count_set() <= b.count);
        }
        // Accessors are total whatever the bytes said.
        let _ = b.is_set(u32::MAX);
        let _ = b.count_set();
    }

    // Availability likewise.
    if let Ok(a) = wire::decode::<wire::Availability>(data) {
        if a.validate(u32::MAX).is_ok() {
            assert_eq!(a.bits.len(), a.chunk_count.div_ceil(8) as usize);
        }
        assert!(a.count() <= a.chunk_count, "over-reported availability");
        let _ = a.is_set(u32::MAX);
    }

    // A snapshot probe's counts must be self-consistent.
    if let Ok(t) = wire::decode::<wire::TreeProbe>(data)
        && t.validate().is_ok()
    {
        assert!(t.chunks_present <= t.chunks_total);
        let _ = t.is_complete();
    }

    // An index descriptor decides what gets fetched, so its bounds matter.
    if let Ok(d) = wire::decode::<wire::IndexDescriptor>(data) {
        const CAP: usize = 64 * 1024 * 1024;
        if d.validate(CAP).is_ok() {
            assert!(!d.index_chunks.is_empty());
            assert!(d.index_len <= CAP as u64);
            let summed: u64 = d.index_chunks.iter().map(|c| c.len as u64).sum();
            assert_eq!(summed, d.index_len, "parts must add up to the whole");
        }
    }
});
