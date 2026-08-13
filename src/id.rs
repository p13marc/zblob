//! [`BlobId`] — a validated blob identifier.
//!
//! A blob's id appears verbatim in a Zenoh key expression, so its shape is not
//! cosmetic: a `/` splits one key into two, a `*` turns a point query into a
//! wildcard, and a leading `@` collides with the reserved administrative
//! planes. Until 0.3 `Manifest.id` was a `String` and those rules were enforced
//! by `Manifest::validate`, *if* somebody called it — which put the guarantee
//! one convention away from a key-injection bug, on a field that arrives
//! straight off the network.
//!
//! `BlobId` moves the check into deserialization, so a manifest that decoded
//! has an id safe to build a key from. It serializes exactly as its `String`
//! did (`#[serde(try_from, into)]` with no wrapper), so this is a source break
//! and not a wire break.
//!
//! # Crafting an invalid id anyway
//!
//! The adversarial suites exist to send this crate things it should refuse, and
//! a type that cannot hold a bad id cannot express "a peer sent a bad id".
//! postcard is positional, so encoding a tuple of the same shape produces the
//! same bytes — `wire::encode(&(WIRE_VERSION, "bad/id", …))` is how to build
//! that message, and `tests/hostile_peer.rs` is where it belongs.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{BlobError, Result};

/// A validated blob identifier: one non-empty, non-reserved key segment.
///
/// The rules, all of them consequences of appearing in a key expression:
///
/// - non-empty, and at most [`BlobId::MAX_LEN`] bytes;
/// - not `..`, and not starting with `@` (the reserved plane prefix);
/// - free of `/`, `\`, `*`, `?`, `#`, `$` — the separator and the wildcard,
///   selector and verbatim-chunk operators;
/// - free of whitespace.
///
/// The `@` rule is not cosmetic: Zenoh treats a segment beginning with `@` as
/// *verbatim*, and `**` does not match it. A server declares its queryable on
/// `<prefix>/**`, so an id like `@thing` would register successfully and then
/// never be servable — every download would time out as `NotFound` with
/// nothing in any log to explain it. Reject it at the door instead.
/// (`x@y` is fine; only the leading position is special.)
///
/// `/` and `\` are refused for a second reason beyond key structure: ids are
/// joined into spool and tag *file* names.
///
/// ```
/// # use zblob::BlobId;
/// let id = BlobId::new("report-01")?;
/// assert_eq!(id.as_str(), "report-01");
///
/// // The shapes that would change what a key means are refused.
/// assert!(BlobId::new("a/b").is_err());
/// assert!(BlobId::new("a*").is_err());
/// assert!(BlobId::new("@admin").is_err());
/// # Ok::<(), zblob::BlobError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct BlobId(String);

impl BlobId {
    /// Longest id accepted, in bytes.
    ///
    /// Keys are compared and matched per query; an unbounded segment is a cheap
    /// way to make every match expensive.
    pub const MAX_LEN: usize = 200;

    /// Validate `id` and wrap it.
    ///
    /// # Errors
    ///
    /// [`BlobError::InvalidManifest`] if it is not a single usable key segment.
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if Self::is_valid(&id) {
            Ok(BlobId(id))
        } else {
            Err(BlobError::InvalidManifest(format!(
                "id {id:?} is not a valid single key segment"
            )))
        }
    }

    /// Whether `id` would be accepted by [`BlobId::new`].
    #[must_use]
    pub fn is_valid(id: &str) -> bool {
        !id.is_empty()
            && id.len() <= Self::MAX_LEN
            && id != ".."
            && !id.starts_with('@')
            && !id.contains(['/', '\\', '*', '?', '#', '$'])
            && !id.chars().any(char::is_whitespace)
    }

    /// The id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the id, returning the inner `String`.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::ops::Deref for BlobId {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

/// Lets a `HashMap<BlobId, _>` be looked up by `&str` — the shape the server
/// registry needs, since an incoming query carries a key segment, not an id.
impl std::borrow::Borrow<str> for BlobId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for BlobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for BlobId {
    type Error = BlobError;
    fn try_from(s: String) -> Result<Self> {
        BlobId::new(s)
    }
}

impl TryFrom<&str> for BlobId {
    type Error = BlobError;
    fn try_from(s: &str) -> Result<Self> {
        BlobId::new(s)
    }
}

impl std::str::FromStr for BlobId {
    type Err = BlobError;
    fn from_str(s: &str) -> Result<Self> {
        BlobId::new(s)
    }
}

impl From<BlobId> for String {
    fn from(id: BlobId) -> String {
        id.0
    }
}

impl PartialEq<str> for BlobId {
    fn eq(&self, other: &str) -> bool {
        self.0 == other
    }
}

impl PartialEq<&str> for BlobId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<BlobId> for str {
    fn eq(&self, other: &BlobId) -> bool {
        self == other.0
    }
}

impl PartialEq<BlobId> for &str {
    fn eq(&self, other: &BlobId) -> bool {
        *self == other.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_ids_and_refuses_key_breaking_ones() {
        for good in ["a", "report-01", "01J8Z", "a.b_c", &"x".repeat(200)] {
            assert!(BlobId::new(good).is_ok(), "{good:?} should be accepted");
        }
        for bad in [
            "",
            "..",
            "@rpc",
            "a/b",
            "a\\b",
            "a*",
            "a?",
            "a#b",
            "a$b",
            "a b",
            "a\tb",
            "a\nb",
            &"x".repeat(201),
        ] {
            assert!(BlobId::new(bad).is_err(), "{bad:?} should be refused");
        }
    }

    /// The conversions are the API's whole ergonomic surface — a caller
    /// reaches an id through one of them, so a wrong one is a wrong id.
    #[test]
    fn every_conversion_agrees_on_the_same_id() {
        use std::str::FromStr;

        let id = BlobId::new("report-01").unwrap();
        assert_eq!(id.as_str(), "report-01");
        assert_eq!(&*id, "report-01"); // Deref
        assert_eq!(id.as_ref() as &str, "report-01"); // AsRef
        assert_eq!(id.to_string(), "report-01"); // Display
        assert_eq!(format!("{id:?}"), "BlobId(\"report-01\")");
        assert_eq!(String::from(id.clone()), "report-01");
        assert_eq!(id.clone().into_string(), "report-01");

        assert_eq!(BlobId::try_from("report-01").unwrap(), id);
        assert_eq!(BlobId::try_from(String::from("report-01")).unwrap(), id);
        assert_eq!(BlobId::from_str("report-01").unwrap(), id);
        assert_eq!("report-01".parse::<BlobId>().unwrap(), id);

        // …and the fallible ones fail, rather than being infallible in
        // disguise.
        assert!(BlobId::try_from("bad/id").is_err());
        assert!("bad/id".parse::<BlobId>().is_err());

        // Comparison against bare strings, in both directions — the shape
        // every `manifest.id == requested` site uses.
        assert!(id == "report-01");
        assert!(id == *"report-01");
        assert!("report-01" == id);
        assert!(*"report-01" == id);
        assert!(id != "other");

        // `Borrow<str>` is what lets the server registry be keyed by id and
        // looked up by a key segment; if it disagreed with `Eq`/`Hash` the
        // lookup would silently miss.
        let mut map = std::collections::HashMap::new();
        map.insert(id.clone(), 7);
        assert_eq!(map.get("report-01"), Some(&7));
        assert_eq!(map.get("nope"), None);

        // Ord, for anything sorting ids.
        let mut v = [
            BlobId::new("c").unwrap(),
            BlobId::new("a").unwrap(),
            BlobId::new("b").unwrap(),
        ];
        v.sort();
        assert_eq!(
            v.iter().map(BlobId::as_str).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
    }

    /// The whole point: an id off the wire is checked by *decoding*, not by
    /// remembering to call a validator afterwards.
    #[test]
    fn decoding_refuses_an_id_that_would_break_a_key() {
        // postcard is positional, so a bare String encodes exactly as the
        // newtype does — this is a peer sending a hostile id.
        let hostile = crate::wire::encode(&String::from("blob/../@evil")).unwrap();
        assert!(crate::wire::decode::<BlobId>(&hostile).is_err());

        // Discriminating power: the identical construction with a legal id
        // decodes, so the rejection above is about the id and not the framing.
        let honest = crate::wire::encode(&String::from("blob-1")).unwrap();
        assert_eq!(
            crate::wire::decode::<BlobId>(&honest).unwrap().as_str(),
            "blob-1"
        );
    }

    /// Wire-transparency is what makes this a source break and not a wire
    /// break; if it ever stops holding, `WIRE_VERSION` must move.
    #[test]
    fn encodes_exactly_as_the_string_it_replaced() {
        let id = BlobId::new("report-01").unwrap();
        assert_eq!(
            crate::wire::encode(&id).unwrap(),
            crate::wire::encode(&String::from("report-01")).unwrap()
        );
    }
}
