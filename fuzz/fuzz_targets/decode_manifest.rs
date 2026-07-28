//! Fuzz the postcard `Manifest` decoder + validation.
#![no_main]
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(m) = zblob::wire::decode::<zblob::Manifest>(data) {
        let _ = m.validate(1 << 40);
        let _ = m.suggested_filename();
    }
});
