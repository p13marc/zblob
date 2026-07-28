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
