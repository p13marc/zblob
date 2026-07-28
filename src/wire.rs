//! v2 wire encoding — postcard for every control message.
//!
//! v1 let each server/client pair pick JSON or CBOR at construction time,
//! which meant a `Format` mismatch surfaced as an opaque decode error deep in
//! a transfer. v2 has exactly one wire encoding: **postcard** (compact varint
//! framing, the iroh choice). Postcard is positional — nothing on the wire
//! names its fields — so every control struct carries an explicit schema
//! `version` as its **first field**, and any future shape change bumps it.
//! Replies also tag their Zenoh [`Encoding`](zenoh::bytes::Encoding) with the
//! constants below, so a foreign or stale peer is diagnosable instead of
//! producing garbage.

use serde::{Serialize, de::DeserializeOwned};

use crate::error::{BlobError, Result};

/// The wire schema version this crate speaks. Carried as the first field of
/// every control struct; any shape change bumps it.
pub const WIRE_VERSION: u16 = 2;

/// Zenoh [`Encoding`](zenoh::bytes::Encoding) tag of a manifest reply.
pub const ENC_MANIFEST: &str = "zblob/manifest;v=2";
/// Encoding tag of a bao slice reply (BlockSize 4 = 16 KiB groups).
pub const ENC_SLICE: &str = "zblob/bao4;v=2";
/// Encoding tag of a Tier-2 tree index reply.
pub const ENC_INDEX: &str = "zblob/index;v=2";
/// Encoding tag of a Tier-2 raw content-addressed chunk reply.
pub const ENC_CHUNK: &str = "zblob/chunk";
/// Encoding tag of push-protocol acknowledgement replies.
pub const ENC_PUSH: &str = "zblob/push;v=2";
/// Encoding tag of availability (`…/have`) replies.
pub const ENC_AVAIL: &str = "zblob/have;v=2";

/// A responder's chunk availability for one blob: which transfer chunks it
/// can serve right now. A full server answers all-ones; the shape exists so
/// partial holders (caches, in-progress replicas) can participate and so a
/// client can pick the best-stocked peer before fetching.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Availability {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// Total transfer chunks of the blob.
    pub chunk_count: u32,
    /// LSB-first presence bitfield (`ceil(chunk_count / 8)` bytes).
    pub bits: Vec<u8>,
}

impl Availability {
    /// An all-chunks-present availability.
    pub fn full(chunk_count: u32) -> Self {
        let mut bits = vec![0xffu8; chunk_count.div_ceil(8) as usize];
        // Zero the padding bits so `count()` is exact.
        if let Some(last) = bits.last_mut()
            && !chunk_count.is_multiple_of(8)
        {
            *last = (1u8 << (chunk_count % 8)) - 1;
        }
        Availability {
            version: WIRE_VERSION,
            chunk_count,
            bits,
        }
    }

    /// Whether chunk `i` is available.
    pub fn is_set(&self, i: u32) -> bool {
        i < self.chunk_count
            && (i / 8) < self.bits.len() as u32
            && self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0
    }

    /// How many chunks are available.
    pub fn count(&self) -> u32 {
        self.bits.iter().map(|b| b.count_ones()).sum()
    }
}

/// Encode a control message to postcard bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    postcard::to_stdvec(value).map_err(BlobError::encode)
}

/// Decode a control message from postcard bytes.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    postcard::from_bytes(data).map_err(BlobError::encode)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Sample {
        version: u16,
        a: u32,
        b: String,
    }

    #[test]
    fn postcard_roundtrip() {
        let s = Sample {
            version: 2,
            a: 7,
            b: "hi".into(),
        };
        let bytes = encode(&s).unwrap();
        assert_eq!(decode::<Sample>(&bytes).unwrap(), s);
    }

    #[test]
    fn truncated_input_rejected() {
        let s = Sample {
            version: 2,
            a: 7,
            b: "hello world".into(),
        };
        let bytes = encode(&s).unwrap();
        assert!(decode::<Sample>(&bytes[..bytes.len() - 3]).is_err());
    }
}
