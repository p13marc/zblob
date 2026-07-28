//! Fuzz the slice-query `ranges` parameter grammar.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        if let Ok(ranges) = zblob::parse_ranges(s, 10_000, 512) {
            // Whatever the parser accepts must be sorted, disjoint, bounded.
            let mut last = 0u32;
            for r in &ranges {
                assert!(r.start >= last && r.start < r.end && r.end <= 10_000);
                last = r.end;
            }
        }
    }
});
