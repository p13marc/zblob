//! Fuzz the postcard `TreeIndex` decoder + full validation (paths, sizes,
//! root recomputation).
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(index) = zblob::wire::decode::<zblob::TreeIndex>(data) {
        let _ = index.validate();
        let _ = index.needed_chunks();
        let _ = index.total_size();
    }
});
