//! Encryption at rest (optional `encryption` feature): per-chunk
//! XChaCha20-Poly1305, the borg/restic model.
//!
//! A [`DirStore`](crate::DirStore) configured with a [`StoreKey`] seals every
//! chunk container (after compression framing) before it touches disk:
//!
//! ```text
//! [0x02][24-byte nonce][XChaCha20-Poly1305 ciphertext + tag]
//! ```
//!
//! - The cipher key and a nonce key are derived from the store key with
//!   domain-separated BLAKE3 `derive_key` contexts.
//! - The nonce is `keyed_hash(nonce_key, chunk_hash ‖ container)[..24]` —
//!   deterministic, so sealing an identical container yields identical bytes:
//!   dedup keeps working and re-`put`s stay idempotent.
//!
//!   **It must cover the container, not just the chunk hash.** Deriving it
//!   from `(key, chunk_hash)` alone assumes one (key, hash) pair only ever
//!   seals one plaintext, and that is false: what gets sealed is the
//!   *compression container*, and `DirStore::put` re-packs and re-seals
//!   unconditionally. Re-`put` one chunk into a store reconfigured with a
//!   different [`ChunkCompression`](crate::ChunkCompression) — or merely a
//!   different zstd level — and two distinct plaintexts would go under one
//!   (key, nonce): XChaCha20 keystream reuse, and a reused Poly1305 one-time
//!   key. Hashing the container in costs nothing and removes the assumption.
//! - The chunk's content hash is the AAD, binding each ciphertext to its
//!   store address: ciphertexts cannot be swapped between keys.
//!
//! **Caveat (by design of content addressing, not of the cipher):** chunk
//! *hashes* remain plaintext in keys, indices, and file names — anyone who can
//! guess a chunk's content can confirm its presence. Content-addressed dedup
//! across trust domains is a membership oracle; pair a private store with a
//! private CDC gear seed ([`CdcParams::with_seed`](crate::CdcParams::with_seed))
//! so chunk boundaries — and therefore hashes — are not predictable from
//! public content.

use chacha20poly1305::aead::{Aead, Payload};
use chacha20poly1305::{Key, KeyInit, XChaCha20Poly1305, XNonce};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use crate::compress::TAG_SEALED;
use crate::hash::Hash;

/// A 32-byte store encryption key. Generate it randomly, keep it outside the
/// store directory, and give every store its own key.
///
/// Deliberately **not** `Clone` and **not** `Copy`: `ZeroizeOnDrop` can only
/// scrub the value it is dropping, so every copy left behind would outlive the
/// scrub and defeat the point.
#[derive(Zeroize, ZeroizeOnDrop)]
pub struct StoreKey([u8; 32]);

impl StoreKey {
    /// Wrap 32 bytes of key material.
    pub fn new(bytes: [u8; 32]) -> Self {
        StoreKey(bytes)
    }
}

impl std::fmt::Debug for StoreKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreKey(…)") // never print key material
    }
}

const CIPHER_CONTEXT: &str = "zblob v2 2026-07 store chunk cipher key";
const NONCE_CONTEXT: &str = "zblob v2 2026-07 store chunk nonce key";

fn cipher_for(key: &StoreKey) -> XChaCha20Poly1305 {
    let k = Zeroizing::new(blake3::derive_key(CIPHER_CONTEXT, &key.0));
    XChaCha20Poly1305::new(Key::from_slice(&*k))
}

/// Derive the sealing nonce from the key, the chunk address **and the exact
/// container being sealed** — see the module docs for why the container has to
/// be in there.
fn nonce_for(key: &StoreKey, hash: &Hash, container: &[u8]) -> [u8; 24] {
    let nk = Zeroizing::new(blake3::derive_key(NONCE_CONTEXT, &key.0));
    let mut h = blake3::Hasher::new_keyed(&nk);
    h.update(hash.as_bytes());
    h.update(container);
    let full = h.finalize();
    full.as_bytes()[..24].try_into().expect("24 bytes")
}

/// Seal a (already compression-framed) chunk container for storage.
pub(crate) fn seal(key: &StoreKey, hash: &Hash, container: &[u8]) -> std::io::Result<Vec<u8>> {
    let nonce = nonce_for(key, hash, container);
    let ciphertext = cipher_for(key)
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: container,
                aad: hash.as_bytes(),
            },
        )
        .map_err(|_| std::io::Error::other("chunk seal failed"))?;
    let mut out = Vec::with_capacity(1 + 24 + ciphertext.len());
    out.push(TAG_SEALED);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&ciphertext);
    Ok(out)
}

/// Open a sealed container; `None` means tampered/corrupt/wrong key.
pub(crate) fn open(key: &StoreKey, hash: &Hash, sealed: &[u8]) -> Option<Vec<u8>> {
    let rest = sealed.strip_prefix(&[TAG_SEALED])?;
    if rest.len() < 24 {
        return None;
    }
    let (nonce, ciphertext) = rest.split_at(24);
    cipher_for(key)
        .decrypt(
            XNonce::from_slice(nonce),
            Payload {
                msg: ciphertext,
                aad: hash.as_bytes(),
            },
        )
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(byte: u8) -> StoreKey {
        StoreKey([byte; 32])
    }

    #[test]
    fn seal_open_roundtrip_and_determinism() {
        let k = key(1);
        let h = Hash::of(b"chunk contents");
        let container = b"\x00chunk contents"; // raw-framed
        let sealed = seal(&k, &h, container).unwrap();
        assert_eq!(sealed[0], TAG_SEALED);
        assert_eq!(open(&k, &h, &sealed).unwrap(), container);
        // Deterministic: idempotent re-puts produce identical bytes.
        assert_eq!(seal(&k, &h, container).unwrap(), sealed);
        // And the plaintext does not appear in the sealed form.
        assert!(
            !sealed
                .windows(b"chunk contents".len())
                .any(|w| w == b"chunk contents")
        );
    }

    #[test]
    fn tamper_wrong_key_wrong_address_all_fail() {
        let k = key(2);
        let h = Hash::of(b"data");
        let sealed = seal(&k, &h, b"\x00data").unwrap();

        let mut flipped = sealed.clone();
        let n = flipped.len();
        flipped[n - 1] ^= 0xff;
        assert!(open(&k, &h, &flipped).is_none(), "tampered must fail");

        assert!(open(&key(3), &h, &sealed).is_none(), "wrong key must fail");

        let other = Hash::of(b"other chunk");
        assert!(
            open(&k, &other, &sealed).is_none(),
            "ciphertext is bound to its address (AAD)"
        );
    }
}
