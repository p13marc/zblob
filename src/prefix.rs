//! Typed key prefixes: the role a prefix may play, made unrepresentable
//! otherwise.
//!
//! The rules were always there and always correct — they were just enforced at
//! call time, one `validate_*` at a time, which meant every caller could hold a
//! `String` that looked usable and would be refused later (or, worse, accepted
//! by one side and unanswerable by the other). Two newtypes move the decision
//! to construction:
//!
//! - [`ServePrefix`] — what a server or publisher may own. **Concrete only**: a
//!   server declared on `<prefix>/**` built from a wildcard would answer for
//!   keys it does not own, and a publisher would PUT to a key *expression*
//!   rather than a key.
//! - [`QueryPrefix`] — what a client may ask. Single-segment wildcards are
//!   allowed, because "which origin holds this?" is a legitimate question and
//!   content is verified against a root regardless of who answers.
//!
//! Both refuse `**`. That is not style: ids and tails are resolved
//! *positionally* ([`parse_id`](crate::keys::parse_id),
//! [`parse_tier2_tail`](crate::keys::parse_tier2_tail)), and `**` spans an unknown
//! number of segments, so no server could ever locate the id inside such a
//! query. Allowing it would be a promise the protocol cannot keep — and it
//! previously *was* allowed on one path, where it silently failed.
//!
//! A `ServePrefix` converts to a `QueryPrefix` for free (serving something
//! implies being able to ask for it); the reverse is fallible.

use std::fmt;

use crate::error::{BlobError, Result};

/// A key prefix a **server or publisher** may own: concrete, no wildcards.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServePrefix(String);

/// A key prefix a **client** may query. Single-segment wildcards (`*`, `$*…`)
/// are permitted; `**` is not.
///
/// Fanning a *bulk fetch* across origins costs one full copy per responder —
/// Zenoh cannot cancel remote replies in flight — so a wildcard prefix is for
/// *probing* (tiny replies), followed by a fetch from one chosen origin's
/// concrete prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueryPrefix(String);

/// Shared shape rules: non-empty, no leading/trailing `/`, no `**`, and usable
/// as a key expression in the forms this crate builds from it.
fn validate_shape(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Err(BlobError::InvalidPrefix("empty key prefix".into()));
    }
    if prefix.starts_with('/') || prefix.ends_with('/') {
        return Err(BlobError::InvalidPrefix(format!(
            "key prefix {prefix:?} must not start or end with '/'"
        )));
    }
    if prefix.split('/').any(|seg| seg == "**") {
        return Err(BlobError::InvalidPrefix(format!(
            "key prefix {prefix:?} must not contain '**' — ids are resolved \
             positionally, so no server can answer past an unbounded span"
        )));
    }
    // Must be a key expression Zenoh accepts, and must still be one in the
    // forms this crate actually builds from it.
    zenoh::key_expr::KeyExpr::try_from(prefix)
        .map_err(|e| BlobError::InvalidPrefix(format!("invalid key prefix {prefix:?}: {e}")))?;
    zenoh::key_expr::KeyExpr::try_from(format!("{prefix}/id/**"))
        .map_err(|e| BlobError::InvalidPrefix(format!("unusable key prefix {prefix:?}: {e}")))?;
    Ok(())
}

fn has_wildcard(prefix: &str) -> bool {
    prefix.split('/').any(|seg| seg.contains('*'))
}

impl ServePrefix {
    /// Validate `prefix` as something this process may serve or publish under.
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_shape(&prefix)?;
        if has_wildcard(&prefix) {
            return Err(BlobError::InvalidPrefix(format!(
                "key prefix {prefix:?} must not contain wildcards when serving or publishing"
            )));
        }
        // The declaration form must be canonical too.
        zenoh::key_expr::KeyExpr::try_from(format!("{prefix}/**")).map_err(|e| {
            BlobError::InvalidPrefix(format!("undeclarable key prefix {prefix:?}: {e}"))
        })?;
        Ok(ServePrefix(prefix))
    }

    /// The prefix as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl QueryPrefix {
    /// Validate `prefix` as something a client may query.
    pub fn new(prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        validate_shape(&prefix)?;
        Ok(QueryPrefix(prefix))
    }

    /// The prefix as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether this prefix names exactly one origin.
    ///
    /// Some operations need a single destination rather than "whoever
    /// answers" — an upload, for instance, since the receiver spools state and
    /// the acknowledgements echo the query key.
    pub fn is_concrete(&self) -> bool {
        !has_wildcard(&self.0)
    }
}

impl From<ServePrefix> for QueryPrefix {
    /// Free: anything concrete enough to serve is acceptable to query.
    fn from(p: ServePrefix) -> Self {
        QueryPrefix(p.0)
    }
}

impl From<&ServePrefix> for QueryPrefix {
    fn from(p: &ServePrefix) -> Self {
        QueryPrefix(p.0.clone())
    }
}

impl TryFrom<QueryPrefix> for ServePrefix {
    type Error = BlobError;
    /// Fallible: a query prefix may name many origins.
    fn try_from(p: QueryPrefix) -> Result<Self> {
        ServePrefix::new(p.0)
    }
}

macro_rules! prefix_common {
    ($t:ty) => {
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }
        impl AsRef<str> for $t {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }
        impl std::str::FromStr for $t {
            type Err = BlobError;
            fn from_str(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }
        impl TryFrom<&str> for $t {
            type Error = BlobError;
            fn try_from(s: &str) -> Result<Self> {
                Self::new(s)
            }
        }
        impl TryFrom<String> for $t {
            type Error = BlobError;
            fn try_from(s: String) -> Result<Self> {
                Self::new(s)
            }
        }
    };
}

prefix_common!(ServePrefix);
prefix_common!(QueryPrefix);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_cannot_be_built_on_a_wildcard() {
        assert!(ServePrefix::new("v1/host-a/@blob/artifact").is_ok());
        assert!(ServePrefix::new("v1/*/@blob/artifact").is_err());
        assert!(ServePrefix::new("v1/**/artifact").is_err());
    }

    #[test]
    fn a_client_may_wildcard_an_origin_but_never_span() {
        assert!(QueryPrefix::new("v1/host-a/@blob/artifact").is_ok());
        assert!(QueryPrefix::new("v1/*/@blob/artifact").is_ok());
        // `**` is refused for both roles: the id's position becomes unknowable.
        assert!(QueryPrefix::new("v1/**/artifact").is_err());
    }

    /// The role rules, over the same corpus the old call-time validators were
    /// asserted against — including the `**` cases, which assert the
    /// *underlying Zenoh matching behaviour* (`parse_id` cannot resolve past
    /// an unbounded span) rather than just our own rule. That is what stops
    /// the refusal being "simplified away" later as mere strictness.
    #[test]
    fn the_role_rules_hold_over_the_whole_corpus() {
        // Ordinary and convention-style (verbatim-segment) prefixes are fine
        // for both roles.
        for good in [
            "demo/blobs",
            "v1/h-0011223344ff/@blob/artifact",
            "a",
            "a/b/c",
        ] {
            QueryPrefix::new(good).unwrap_or_else(|e| panic!("{good:?} rejected: {e}"));
            ServePrefix::new(good).unwrap_or_else(|e| panic!("{good:?} rejected: {e}"));
        }
        // Malformed prefixes are rejected for both roles.
        for bad in ["", "/leading", "trailing/", "a//b"] {
            assert!(QueryPrefix::new(bad).is_err(), "{bad:?} should be rejected");
            assert!(ServePrefix::new(bad).is_err(), "{bad:?} should be rejected");
        }
        // Single-segment wildcards are legal to *query* (a multi-holder probe,
        // RFC-sanctioned) but never to serve or publish under.
        for wild in ["v1/*/@blob/artifact", "wild/*/card", "v1/host$*/x"] {
            let q = QueryPrefix::new(wild)
                .unwrap_or_else(|e| panic!("{wild:?} must be queryable: {e}"));
            assert!(!q.is_concrete(), "{wild:?} names more than one origin");
            assert!(
                ServePrefix::new(wild).is_err(),
                "{wild:?} must not be servable"
            );
        }
        // `**` is refused for both roles: an id is resolved positionally, so
        // nothing could answer past an unbounded span.
        for span in ["wild/**", "**", "a/**/b"] {
            assert!(
                QueryPrefix::new(span).is_err(),
                "{span:?} must not be queryable"
            );
            assert!(
                ServePrefix::new(span).is_err(),
                "{span:?} must not be servable"
            );
            assert_eq!(
                crate::keys::parse_id(span, &format!("{span}/A/manifest")),
                None,
                "parse_id must not resolve an id past '**'"
            );
        }
    }

    #[test]
    fn serving_implies_querying() {
        let serve = ServePrefix::new("v1/host-a/@blob/artifact").unwrap();
        let query: QueryPrefix = serve.clone().into();
        assert_eq!(query.as_str(), serve.as_str());
        assert!(query.is_concrete());

        // …and the reverse only when the prefix names one origin.
        let wide = QueryPrefix::new("v1/*/@blob/artifact").unwrap();
        assert!(!wide.is_concrete());
        assert!(ServePrefix::try_from(wide).is_err());
        assert!(ServePrefix::try_from(query).is_ok());
    }

    /// The conversions are the whole surface: a caller reaches a prefix
    /// through one of them, so a wrong one is a wrong key expression, and
    /// `From<ServePrefix> for QueryPrefix` in particular has to preserve the
    /// string rather than re-validating it under the other role's rules.
    #[test]
    fn every_conversion_preserves_the_prefix_and_its_role() {
        use std::str::FromStr;

        let serve = ServePrefix::new("v1/host-a/@blob/art").unwrap();
        assert_eq!(serve.as_str(), "v1/host-a/@blob/art");
        assert_eq!(serve.to_string(), "v1/host-a/@blob/art");
        assert_eq!(serve.as_ref() as &str, "v1/host-a/@blob/art");

        // Serve → query is free and lossless, by value and by reference.
        assert_eq!(
            QueryPrefix::from(serve.clone()).as_str(),
            "v1/host-a/@blob/art"
        );
        assert_eq!(QueryPrefix::from(&serve).as_str(), "v1/host-a/@blob/art");

        // Query → serve is fallible, and fails on exactly the prefixes a
        // server cannot be built on.
        let concrete = QueryPrefix::new("v1/host-a/@blob/art").unwrap();
        assert!(concrete.is_concrete());
        assert_eq!(
            ServePrefix::try_from(concrete).unwrap().as_str(),
            "v1/host-a/@blob/art"
        );
        let wild = QueryPrefix::new("v1/*/@blob/art").unwrap();
        assert!(!wild.is_concrete());
        assert!(ServePrefix::try_from(wild).is_err());

        // The four parsing entry points agree, and all of them reject.
        for good in ["p/q", "v1/host-a/@blob/art"] {
            assert_eq!(ServePrefix::from_str(good).unwrap().as_str(), good);
            assert_eq!(ServePrefix::try_from(good).unwrap().as_str(), good);
            assert_eq!(
                ServePrefix::try_from(String::from(good)).unwrap().as_str(),
                good
            );
            assert_eq!(QueryPrefix::from_str(good).unwrap().as_str(), good);
            assert_eq!(QueryPrefix::try_from(good).unwrap().as_str(), good);
            assert_eq!(
                QueryPrefix::try_from(String::from(good)).unwrap().as_str(),
                good
            );
        }
        for bad in ["", "p/*", "p/**"] {
            assert!(ServePrefix::from_str(bad).is_err(), "{bad:?}");
            assert!(ServePrefix::try_from(bad).is_err(), "{bad:?}");
            assert!(ServePrefix::try_from(String::from(bad)).is_err(), "{bad:?}");
        }
        // A query prefix may wildcard an origin but never span segments.
        assert!(QueryPrefix::from_str("p/*").is_ok());
        assert!(QueryPrefix::from_str("p/**").is_err());
        assert!(QueryPrefix::from_str("").is_err());
    }

    /// Every rejection is an `InvalidPrefix`, not a generic protocol error —
    /// the distinction that tells a caller it is their configuration and not
    /// a peer's doing.
    #[test]
    fn prefix_rejections_are_classified_as_usage() {
        for bad in ["", "p/**", "p/*"] {
            let e = ServePrefix::new(bad).unwrap_err();
            assert!(matches!(e, BlobError::InvalidPrefix(_)), "{bad:?}: {e}");
            assert_eq!(e.kind(), crate::error::ErrorKind::Usage);
            assert!(!e.is_retriable(), "a bad prefix is never worth retrying");
        }
    }
}
