//! Content hashing — BLAKE3, the crate-wide algorithm since v2.
//!
//! v1 shipped a nominally-pluggable `Digest` trait, but every code path
//! hardcoded SHA-256, so v2 drops the pretense: BLAKE3 everywhere. What
//! survives is *algorithm agility on the wire*: Tier-2 chunk keys carry an
//! `<algo>` segment (`"blake3"` since v2), so stores populated by older
//! producers under `sha256/…` keys coexist untouched next to `blake3/…` ones.
//!
//! Tier-1 blob identity is the BLAKE3 *bao root* (see [`crate::verify`]) —
//! identical to `blake3::hash` of the content; the bao block size only affects
//! slice/outboard granularity, never the root.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A 32-byte content hash, rendered as lowercase hex on the wire and in keys.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Hash([u8; 32]);

impl Hash {
    /// The wire/key name of the crate's hash algorithm.
    pub const ALGO: &'static str = "blake3";

    /// BLAKE3 hash of `data` in one shot.
    pub fn of(data: &[u8]) -> Hash {
        blake3::hash(data).into()
    }

    /// The raw bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Build a hash from raw bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Hash {
        Hash(bytes)
    }
}

impl From<blake3::Hash> for Hash {
    fn from(h: blake3::Hash) -> Hash {
        Hash(*h.as_bytes())
    }
}

impl From<Hash> for blake3::Hash {
    fn from(h: Hash) -> blake3::Hash {
        blake3::Hash::from_bytes(h.0)
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({self})")
    }
}

/// Error parsing a [`struct@Hash`] from a hex string.
#[derive(Debug, thiserror::Error)]
#[error("invalid hex hash: {0}")]
pub struct HashParseError(String);

impl FromStr for Hash {
    type Err = HashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 64 {
            return Err(HashParseError(format!(
                "expected 64 hex chars, got {}",
                s.len()
            )));
        }
        let mut out = [0u8; 32];
        for (i, byte) in out.iter_mut().enumerate() {
            let hi = s.as_bytes()[i * 2];
            let lo = s.as_bytes()[i * 2 + 1];
            *byte = (hex_val(hi).ok_or_else(|| HashParseError(s.to_string()))? << 4)
                | hex_val(lo).ok_or_else(|| HashParseError(s.to_string()))?;
        }
        Ok(Hash(out))
    }
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

impl Serialize for Hash {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&self.to_string())
        } else {
            s.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        if d.is_human_readable() {
            let s = String::deserialize(d)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            let bytes: [u8; 32] = serde_bytes_deserialize(d)?;
            Ok(Hash(bytes))
        }
    }
}

/// Deserialize exactly 32 raw bytes (postcard/binary formats).
fn serde_bytes_deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = [u8; 32];
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("32 raw hash bytes")
        }
        fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
            v.try_into()
                .map_err(|_| E::invalid_length(v.len(), &"32 bytes"))
        }
    }
    d.deserialize_bytes(V)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blake3_known_vector() {
        // BLAKE3("abc") — official test vector.
        let h = Hash::of(b"abc");
        assert_eq!(
            h.to_string(),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }

    #[test]
    fn hash_hex_roundtrip() {
        let h = Hash::of(b"abc");
        let parsed: Hash = h.to_string().parse().unwrap();
        assert_eq!(h, parsed);
    }

    #[test]
    fn blake3_conversions_roundtrip() {
        let b = blake3::hash(b"xyz");
        let h: Hash = b.into();
        let back: blake3::Hash = h.into();
        assert_eq!(b, back);
        assert_eq!(h.as_bytes(), b.as_bytes());
    }

    #[test]
    fn human_readable_serde_is_hex_string() {
        let h = Hash::of(b"abc");
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.starts_with('"') && json.contains("6437b3ac"));
        let back: Hash = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn binary_serde_is_raw_bytes() {
        let h = Hash::of(b"abc");
        let bytes = postcard::to_stdvec(&h).unwrap();
        // Length-prefixed 32 raw bytes, not 64 hex chars.
        assert_eq!(bytes.len(), 33);
        let back: Hash = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(h, back);
    }

    #[test]
    fn bad_hex_rejected() {
        assert!("nothex".parse::<Hash>().is_err());
        assert!("zz".repeat(32).parse::<Hash>().is_err());
    }
}
