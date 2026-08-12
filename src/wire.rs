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
pub const WIRE_VERSION: u16 = 3;

/// Zenoh [`Encoding`](zenoh::bytes::Encoding) tag of a manifest reply.
pub const ENC_MANIFEST: &str = "zblob/manifest;v=3";
/// Encoding tag of a bao slice reply (BlockSize 4 = 16 KiB groups).
pub const ENC_SLICE: &str = "zblob/bao4;v=3";
/// Encoding tag of a Tier-2 tree index reply.
pub const ENC_INDEX: &str = "zblob/index;v=3";
/// Encoding tag of a Tier-2 content-addressed chunk reply.
///
/// Versioned as of v3. It was the one tag that carried no version, on the
/// reasoning that a chunk container is self-describing — which is true of the
/// *container* and says nothing about the surrounding protocol. A tag whose
/// job is "diagnosable instead of garbage" should not have an exception.
pub const ENC_CHUNK: &str = "zblob/chunk;v=3";
/// Encoding tag of push-protocol acknowledgement replies.
pub const ENC_PUSH: &str = "zblob/push;v=3";
/// Encoding tag of availability (`…/have`) replies.
pub const ENC_AVAIL: &str = "zblob/have;v=3";

/// A trailing, length-prefixed extension list carried by the *metadata*
/// messages ([`crate::Manifest`]).
///
/// postcard is positional, so without this every additive field costs a wire
/// break — and a wire break costs a fleet a coordinated upgrade. Unknown ids
/// are skipped, order is irrelevant, and duplicates take the first.
///
/// Deliberately **not** on the slice or chunk path, which stays exactly as
/// tight as it is: this exists so metadata can grow, not so the bulk path can.
pub type Ext = Vec<(u16, Vec<u8>)>;

/// Extension id: the server's `max_chunks_per_query`, as a little-endian `u32`.
pub const EXT_MAX_CHUNKS_PER_QUERY: u16 = 1;

/// Extension id: the server's `max_blob_size`, as a little-endian `u64`.
pub const EXT_MAX_BLOB_SIZE: u16 = 2;

/// Read a `u32` extension value, if present and well-formed.
pub fn ext_u32(ext: &Ext, id: u16) -> Option<u32> {
    let (_, v) = ext.iter().find(|(k, _)| *k == id)?;
    Some(u32::from_le_bytes(v.as_slice().try_into().ok()?))
}

/// Read a `u64` extension value, if present and well-formed.
pub fn ext_u64(ext: &Ext, id: u16) -> Option<u64> {
    let (_, v) = ext.iter().find(|(k, _)| *k == id)?;
    Some(u64::from_le_bytes(v.as_slice().try_into().ok()?))
}

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
    ///
    /// Counts only bits within `chunk_count`. Summing the whole byte vector
    /// would let a responder over-report by setting the final byte's padding
    /// bits, or by sending more bytes than the count needs — this is a value
    /// off the network, so it is not permitted to exceed its own bound.
    pub fn count(&self) -> u32 {
        (0..self.chunk_count).filter(|i| self.is_set(*i)).count() as u32
    }

    /// Check an availability reply against its own claims: the schema version,
    /// and a bitfield exactly as long as `chunk_count` requires.
    ///
    /// A decoded `Availability` is remote input. Without the length check a
    /// responder can answer `chunk_count = 1` with megabytes of `bits`, and
    /// callers accumulating one reply per responder pay for all of it.
    pub fn validate(&self, max_chunks: u32) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.chunk_count > max_chunks {
            return Err(BlobError::Protocol(format!(
                "availability claims {} chunks, over the limit of {max_chunks}",
                self.chunk_count
            )));
        }
        let want = self.chunk_count.div_ceil(8) as usize;
        if self.bits.len() != want {
            return Err(BlobError::Protocol(format!(
                "availability bitfield is {} bytes, expected {want} for {} chunks",
                self.bits.len(),
                self.chunk_count
            )));
        }
        Ok(())
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
mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        /// An availability reply is a remote peer's claim about itself, so it
        /// must never be able to claim more than it declared — whatever bytes
        /// arrive, and whether or not they are well-formed. Summing the byte
        /// vector, the obvious implementation, fails this the moment the final
        /// byte has padding bits set.
        #[test]
        fn count_never_exceeds_the_declared_chunk_count(
            chunk_count in 0u32..5000,
            bits in prop::collection::vec(any::<u8>(), 0..800),
        ) {
            let avail = Availability { version: WIRE_VERSION, chunk_count, bits };
            prop_assert!(avail.count() <= chunk_count);
            // …and it must agree with the per-chunk accessor it is derived from.
            let by_hand = (0..chunk_count).filter(|i| avail.is_set(*i)).count() as u32;
            prop_assert_eq!(avail.count(), by_hand);
        }

        /// `validate` accepts exactly the bitfields whose length matches the
        /// count they claim — the check that stops a responder answering
        /// "1 chunk" with megabytes of bits.
        #[test]
        fn validate_accepts_only_well_sized_bitfields(
            chunk_count in 0u32..5000,
            len in 0usize..800,
        ) {
            let avail = Availability {
                version: WIRE_VERSION,
                chunk_count,
                bits: vec![0u8; len],
            };
            prop_assert_eq!(
                avail.validate(u32::MAX).is_ok(),
                len == chunk_count.div_ceil(8) as usize
            );
        }
    }

    /// The honest constructor must satisfy its own validator — otherwise the
    /// check above is just rejecting everything.
    #[test]
    fn full_availability_is_valid() {
        for n in [0u32, 1, 7, 8, 9, 4095] {
            let a = Availability::full(n);
            a.validate(u32::MAX).expect("full() must validate");
            assert_eq!(a.count(), n, "full() must report every chunk present");
        }
    }
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
