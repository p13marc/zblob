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
use crate::hash::Hash;

/// The wire schema version this crate speaks. Carried as the first field of
/// every control struct; any shape change bumps it.
pub const WIRE_VERSION: u16 = 3;

/// Zenoh [`Encoding`](zenoh::bytes::Encoding) tag of a manifest reply.
pub const ENC_MANIFEST: &str = "zblob/manifest;v=3";
/// Encoding tag of a bao slice reply (BlockSize 4 = 16 KiB groups).
pub const ENC_SLICE: &str = "zblob/bao4;v=3";
/// Encoding tag of a Tier-2 tree index reply.
pub const ENC_INDEX: &str = "zblob/index;v=3";
/// Encoding tag of a Tier-2 *index descriptor* reply (a large index, served
/// as content-addressed chunks — see [`IndexDescriptor`]).
pub const ENC_INDEX_DESC: &str = "zblob/indexdesc;v=3";
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
/// Encoding tag of tier-2 probe replies ([`HaveBits`]).
pub const ENC_HAVEBITS: &str = "zblob/havebits;v=3";
/// Encoding tag of tier-2 snapshot probe replies ([`TreeProbe`]).
pub const ENC_TREEPROBE: &str = "zblob/treeprobe;v=3";
/// Encoding tag of `fanout` tier samples (feature-gated).
pub const ENC_FANOUT: &str = "zblob/fanout;v=3";

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

/// A set of content addresses a client is asking about: the request body of
/// the tier-2 batch fetch and the tier-2 chunk probe.
///
/// One question, two answers of very different size — the batch endpoint
/// replies with the chunks themselves, the probe with one bit each. Sharing
/// the request type is deliberate: probe to choose a holder, then batch-fetch
/// from the one you chose.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WantList {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// The addresses being asked about, in the order the answer must use.
    pub hashes: Vec<Hash>,
}

/// Largest want-list a server will accept in one query.
///
/// Bounds the work a single request can commit a server to, the same way
/// `MAX_RANGE_SPANS` does for tier 1. Advertised, so a client clamps rather
/// than guessing.
pub const MAX_WANT_LIST: usize = 512;

impl WantList {
    /// A want-list for `hashes`.
    pub fn new(hashes: Vec<Hash>) -> Self {
        WantList {
            version: WIRE_VERSION,
            hashes,
        }
    }

    /// Check a want-list received from the network *before* doing any I/O for
    /// it: schema version, non-empty, within `max`, and free of duplicates.
    ///
    /// Duplicates are refused rather than deduplicated because they can only
    /// be a mistake or an attempt to multiply the reply volume for a given
    /// request size, and silently accepting either makes the cap a lie.
    pub fn validate(&self, max: usize) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.hashes.is_empty() {
            return Err(BlobError::MalformedMessage("empty want list".into()));
        }
        if self.hashes.len() > max {
            return Err(BlobError::MalformedMessage(format!(
                "want list of {} exceeds the limit of {max}",
                self.hashes.len()
            )));
        }
        let mut seen = std::collections::HashSet::with_capacity(self.hashes.len());
        if let Some(dup) = self.hashes.iter().find(|h| !seen.insert(**h)) {
            return Err(BlobError::MalformedMessage(format!(
                "want list repeats {dup}"
            )));
        }
        Ok(())
    }
}

/// One bit per entry of a [`WantList`], in the same order: the tier-2 probe's
/// reply.
///
/// The reply is a function of the *question*, never of the objects asked
/// about — `hashes.len() / 8` bytes — which is what makes a wildcard-origin
/// tier-2 probe as legitimate as tier 1's, and a wildcard-origin tier-2
/// *fetch* still forbidden.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct HaveBits {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// How many entries the answered want-list had.
    pub count: u32,
    /// LSB-first presence bitfield (`ceil(count / 8)` bytes).
    pub bits: Vec<u8>,
}

impl HaveBits {
    /// Build a reply from a presence predicate over the want-list.
    pub fn from_presence(present: impl IntoIterator<Item = bool>) -> Self {
        let mut count = 0u32;
        let mut bits: Vec<u8> = Vec::new();
        for (i, yes) in present.into_iter().enumerate() {
            if i % 8 == 0 {
                bits.push(0);
            }
            if yes {
                let last = bits.len() - 1;
                bits[last] |= 1 << (i % 8);
            }
            count += 1;
        }
        HaveBits {
            version: WIRE_VERSION,
            count,
            bits,
        }
    }

    /// Whether entry `i` of the answered want-list is held.
    pub fn is_set(&self, i: u32) -> bool {
        i < self.count
            && (i / 8) < self.bits.len() as u32
            && self.bits[(i / 8) as usize] & (1 << (i % 8)) != 0
    }

    /// How many entries are held.
    ///
    /// Bounded by the bitfield's length, not by the declared count — see
    /// [`Availability::count`].
    pub fn count_set(&self) -> u32 {
        count_bits(&self.bits, self.count)
    }

    /// Check a probe reply against the question it answers: right version,
    /// right length, and a bitfield sized to its own count.
    pub fn validate(&self, asked: usize) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.count as usize != asked {
            return Err(BlobError::MalformedMessage(format!(
                "probe answered {} entries for a want-list of {asked}",
                self.count
            )));
        }
        let want = self.count.div_ceil(8) as usize;
        if self.bits.len() != want {
            return Err(BlobError::MalformedMessage(format!(
                "probe bitfield is {} bytes, expected {want}",
                self.bits.len()
            )));
        }
        Ok(())
    }
}

/// A pointer to an index too large to send whole: the encoded
/// [`TreeIndex`](crate::TreeIndex),
/// cut into ordinary content-addressed chunks.
///
/// Served on the tree key *instead of* the index itself, and distinguished
/// from it by the reply's encoding tag. The threshold matters: an index costs
/// about 0.05–0.10% of the payload it describes, so the overwhelming majority
/// are a few KB and are best sent as they always were — one reply, one round
/// trip. What a descriptor buys is for the minority that are not:
///
/// - **Resumability.** Zenoh fragments anything over 64 KiB and a dropped
///   fragment discards the whole message, so a 1.6 MiB index is 26 fragments
///   re-fetched in full on every loss. As chunks it resumes hole-by-hole like
///   everything else.
/// - **No ceiling.** A monolithic index is bounded by what one reply may
///   carry; a chunked one is not.
/// - **Metadata dedup.** Two snapshots of a mostly-unchanged tree share their
///   index chunks, as restic's model does.
///
/// The descriptor is untrusted like everything else: its chunks are verified
/// individually by address, and the reassembled index is verified by
/// recomputing the root.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexDescriptor {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// The snapshot's identity — what the reassembled index must recompute to.
    pub root: Hash,
    /// Hash algorithm of `index_chunks`.
    pub algo: String,
    /// The encoded index, in order.
    ///
    /// Cut at a **fixed** size, not by CDC: the CDC parameters live *inside*
    /// the index, so content-defined chunking of the index itself would be
    /// circular — and an index is written once, so CDC buys nothing on it.
    pub index_chunks: Vec<crate::tree::ChunkRef>,
    /// Total encoded length of the index, for bounds-checking before assembly.
    pub index_len: u64,
    /// Trailing extension list — see [`Ext`].
    pub ext: Ext,
}

impl IndexDescriptor {
    /// Check a descriptor before fetching anything it points at.
    pub fn validate(&self, max_index_bytes: usize) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.algo != Hash::ALGO {
            return Err(BlobError::MalformedMessage(format!(
                "index descriptor uses unsupported algo {}",
                self.algo
            )));
        }
        if self.index_chunks.is_empty() {
            return Err(BlobError::MalformedMessage(
                "index descriptor has no chunks".into(),
            ));
        }
        if self.index_len > max_index_bytes as u64 {
            return Err(BlobError::InvalidManifest(format!(
                "index of {} bytes exceeds the {max_index_bytes} byte limit",
                self.index_len
            )));
        }
        // The parts must add up to the whole, or assembly is not defined.
        let summed: u64 = self.index_chunks.iter().map(|c| c.len as u64).sum();
        if summed != self.index_len {
            return Err(BlobError::MalformedMessage(format!(
                "index chunks total {summed} bytes, declared {}",
                self.index_len
            )));
        }
        Ok(())
    }
}

/// What one holder has of a snapshot: the tier-2 tree probe's reply.
///
/// Four small numbers, whatever the size of the snapshot — so an explorer can
/// ask "who has this, and how much of it" across origins without any of them
/// shipping a tree. Before this, the honest answer available to a consumer was
/// `not_probed`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TreeProbe {
    /// Wire schema version (first field; postcard is positional).
    pub version: u16,
    /// Whether this holder serves the snapshot index itself.
    pub have_index: bool,
    /// Distinct chunks of the snapshot this holder has.
    pub chunks_present: u32,
    /// Distinct chunks the snapshot references in total.
    pub chunks_total: u32,
}

impl TreeProbe {
    /// Check a probe reply for internal consistency.
    pub fn validate(&self) -> Result<()> {
        if self.version != WIRE_VERSION {
            return Err(BlobError::UnsupportedVersion(self.version));
        }
        if self.chunks_present > self.chunks_total {
            return Err(BlobError::MalformedMessage(format!(
                "probe claims {} of {} chunks",
                self.chunks_present, self.chunks_total
            )));
        }
        Ok(())
    }

    /// Whether this holder can serve the whole snapshot on its own.
    pub fn is_complete(&self) -> bool {
        self.have_index && self.chunks_present == self.chunks_total
    }
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
    /// Counts only bits within `chunk_count`, so a responder cannot
    /// over-report by setting the final byte's padding bits or by sending more
    /// bytes than its count needs.
    ///
    /// Note the loop bound: over the **bytes**, not over `chunk_count`.
    /// `chunk_count` is an unvalidated `u32` off the network, so iterating it
    /// would let a four-byte field buy four billion iterations — which is how
    /// the first version of this went, and what the fuzzer noticed.
    pub fn count(&self) -> u32 {
        count_bits(&self.bits, self.chunk_count)
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
            return Err(BlobError::MalformedMessage(format!(
                "availability claims {} chunks, over the limit of {max_chunks}",
                self.chunk_count
            )));
        }
        let want = self.chunk_count.div_ceil(8) as usize;
        if self.bits.len() != want {
            return Err(BlobError::MalformedMessage(format!(
                "availability bitfield is {} bytes, expected {want} for {} chunks",
                self.bits.len(),
                self.chunk_count
            )));
        }
        Ok(())
    }
}

/// Population count of the first `limit` bits of `bits`, LSB-first.
///
/// Walks the bytes actually present, masking the final partial byte, so the
/// cost is bounded by the data rather than by the declared count — which is an
/// unvalidated number from a remote peer.
fn count_bits(bits: &[u8], limit: u32) -> u32 {
    let full = (limit / 8) as usize;
    let mut n: u32 = bits.iter().take(full).map(|b| b.count_ones()).sum();
    if let Some(last) = bits.get(full) {
        let rem = limit % 8;
        if rem > 0 {
            n += (last & ((1u8 << rem) - 1)).count_ones();
        }
    }
    n
}

/// Encode a control message to postcard bytes.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(postcard::to_stdvec(value)?)
}

/// Decode a control message from postcard bytes.
pub fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    Ok(postcard::from_bytes(data)?)
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

    /// Counting must cost the *bitfield*, not the declared count.
    ///
    /// A `chunk_count` of `u32::MAX` with an empty bitfield is four bytes on
    /// the wire; if counting iterated the count it would buy four billion
    /// iterations per message. The first version of `count()` did exactly
    /// that, and the fuzzer found it — as a 750x slowdown, which is what a
    /// denial of service looks like from the inside.
    #[test]
    fn counting_is_bounded_by_the_bitfield_not_the_claim() {
        let hostile = Availability {
            version: WIRE_VERSION,
            chunk_count: u32::MAX,
            bits: vec![0xff; 4],
        };
        let started = std::time::Instant::now();
        assert_eq!(hostile.count(), 32, "only the bits present may count");
        assert!(
            started.elapsed() < std::time::Duration::from_millis(50),
            "counting took {:?} — it is iterating the claim, not the data",
            started.elapsed()
        );

        let hostile = HaveBits {
            version: WIRE_VERSION,
            count: u32::MAX,
            bits: vec![0xff; 4],
        };
        let started = std::time::Instant::now();
        assert_eq!(hostile.count_set(), 32);
        assert!(started.elapsed() < std::time::Duration::from_millis(50));
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
