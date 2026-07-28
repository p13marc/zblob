//! Filesystem safety helpers.
//!
//! Everything that materializes remote-controlled paths goes through here.
//! A tree index (or a manifest filename) is attacker input: absolute paths,
//! `..`, drive prefixes, and symlink tricks must all die at this boundary, not
//! at the write site (the v1 path-traversal vector, C2).

use std::path::{Component, Path, PathBuf};

use crate::error::{BlobError, Result};

/// Validate a wire-carried relative path (`/`-separated) and return it as a
/// join-safe `PathBuf`: non-empty, relative, and made only of `Normal`
/// components (no `..`, no root, no prefix, no `.`).
pub(crate) fn sanitize_rel_path(path: &str) -> Result<PathBuf> {
    if path.is_empty() {
        return Err(BlobError::Protocol("empty entry path".into()));
    }
    let p = Path::new(path);
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::Normal(c) => out.push(c),
            other => {
                return Err(BlobError::Protocol(format!(
                    "unsafe path component {other:?} in entry path {path:?}"
                )));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(BlobError::Protocol(format!("empty entry path {path:?}")));
    }
    Ok(out)
}

/// Validate a symlink target from an index: must be relative, and resolving it
/// lexically from the link's parent directory must stay inside the tree root
/// (`link_rel` is the sanitized link path relative to the root).
pub(crate) fn sanitize_symlink_target(link_rel: &Path, target: &str) -> Result<()> {
    let t = Path::new(target);
    if t.is_absolute() || t.components().any(|c| matches!(c, Component::Prefix(_))) {
        return Err(BlobError::Protocol(format!(
            "absolute symlink target {target:?}"
        )));
    }
    // Lexical resolution: start at the link's parent depth, walk the target.
    let mut depth: i64 = link_rel.components().count() as i64 - 1;
    for comp in t.components() {
        match comp {
            Component::Normal(_) => depth += 1,
            Component::ParentDir => {
                depth -= 1;
                if depth < 0 {
                    return Err(BlobError::Protocol(format!(
                        "symlink target {target:?} escapes the tree root"
                    )));
                }
            }
            Component::CurDir => {}
            other => {
                return Err(BlobError::Protocol(format!(
                    "unsafe symlink target component {other:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Verify that `path`'s (existing) parent directory really lives under
/// `canonical_root` — the runtime backstop against writing *through* a
/// pre-existing symlink that lexical checks cannot see.
pub(crate) fn assert_parent_within(canonical_root: &Path, path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or(canonical_root);
    let canon = parent.canonicalize()?;
    if !canon.starts_with(canonical_root) {
        return Err(BlobError::Protocol(format!(
            "entry path {path:?} resolves outside the destination root"
        )));
    }
    Ok(())
}

/// Validate a caller-supplied key prefix used to *query* (client side).
///
/// Rejects: empty, leading/trailing `/`, and anything Zenoh itself rejects.
/// **Wildcards are allowed**: a `*`-origin prefix is a legitimate multi-holder
/// query (asking every origin that might hold a blob), and content-addressed
/// replies are verified against the pinned root regardless of who answers.
/// Note that fanning a *bulk fetch* out this way costs one full copy per
/// responder — Zenoh cannot cancel remote replies in flight — so use it to
/// probe (`fetch_manifest`, `fetch_availability`: tiny replies) and then
/// download from one chosen origin's concrete prefix.
///
/// Multi-segment wildcards (`**`) are **not** allowed even here: a server
/// resolves the blob id by position ([`crate::parse_id`]), and a `**` can
/// span any number of segments, so no server could ever answer such a query.
/// Allowing it would be a promise the protocol cannot keep.
///
/// Verbatim (`@`) segments are allowed: they sit in the literal part of both
/// the declaration and the keys, which is exactly how a keyspace convention
/// namespaces a bulk plane.
pub(crate) fn validate_query_prefix(prefix: &str) -> Result<()> {
    if prefix.is_empty() {
        return Err(BlobError::Protocol("empty key prefix".into()));
    }
    if prefix.starts_with('/') || prefix.ends_with('/') {
        return Err(BlobError::Protocol(format!(
            "key prefix {prefix:?} must not start or end with '/'"
        )));
    }
    if prefix.split('/').any(|seg| seg == "**") {
        return Err(BlobError::Protocol(format!(
            "key prefix {prefix:?} must not contain '**' — a blob id is resolved \
             positionally, so no server can answer past an unbounded span"
        )));
    }
    // Must be a key expression Zenoh accepts, and must still be one in the
    // forms this crate actually builds from it.
    zenoh::key_expr::KeyExpr::try_from(prefix)
        .map_err(|e| BlobError::Protocol(format!("invalid key prefix {prefix:?}: {e}")))?;
    zenoh::key_expr::KeyExpr::try_from(format!("{prefix}/id/**"))
        .map_err(|e| BlobError::Protocol(format!("unusable key prefix {prefix:?}: {e}")))?;
    Ok(())
}

/// Validate a key prefix used to *serve* or *publish* under (server side).
///
/// Everything [`validate_query_prefix`] requires, plus **no wildcards**: a
/// server declaring `<prefix>/**` from a wildcard prefix would answer for
/// keys it does not own, and a publisher would write to a key expression
/// rather than a key.
pub(crate) fn validate_serve_prefix(prefix: &str) -> Result<()> {
    validate_query_prefix(prefix)?;
    if prefix.split('/').any(|seg| seg.contains('*')) {
        return Err(BlobError::Protocol(format!(
            "key prefix {prefix:?} must not contain wildcards when serving or publishing"
        )));
    }
    // The declaration form must be canonical too.
    zenoh::key_expr::KeyExpr::try_from(format!("{prefix}/**"))
        .map_err(|e| BlobError::Protocol(format!("undeclarable key prefix {prefix:?}: {e}")))?;
    Ok(())
}

/// Create `root.join(rel)` (and its ancestors) one component at a time,
/// refusing to traverse any pre-existing symlink component — `create_dir_all`
/// happily follows a symlinked directory *out* of the root before any
/// containment check can run, so prevention has to happen per component.
pub(crate) fn create_dir_confined(root: &Path, rel: &Path) -> Result<std::path::PathBuf> {
    let mut cur = root.to_path_buf();
    for comp in rel.components() {
        cur.push(comp);
        match std::fs::symlink_metadata(&cur) {
            Ok(meta) if meta.file_type().is_symlink() => {
                return Err(BlobError::Protocol(format!(
                    "refusing to traverse symlink at {cur:?} while materializing {rel:?}"
                )));
            }
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => {
                return Err(BlobError::Protocol(format!(
                    "non-directory in the way at {cur:?} while materializing {rel:?}"
                )));
            }
            Err(_) => {
                std::fs::create_dir(&cur)?;
            }
        }
    }
    Ok(cur)
}

/// Durably record a directory-entry change (rename/create) on platforms where
/// that requires fsyncing the directory itself.
#[cfg(unix)]
pub(crate) fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}
#[cfg(not(unix))]
pub(crate) fn fsync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_paths_sanitized() {
        assert_eq!(
            sanitize_rel_path("a/b/c.txt").unwrap(),
            Path::new("a/b/c.txt")
        );
        assert_eq!(sanitize_rel_path("x").unwrap(), Path::new("x"));
        for bad in ["", "/etc/passwd", "../x", "a/../../x", "a/..", ".", "./"] {
            assert!(sanitize_rel_path(bad).is_err(), "should reject {bad:?}");
        }
    }

    #[test]
    fn key_prefixes_validated() {
        // Ordinary and convention-style (verbatim-segment) prefixes are fine
        // for both roles.
        for good in [
            "demo/blobs",
            "v1/h-0011223344ff/@blob/artifact",
            "a",
            "a/b/c",
        ] {
            validate_query_prefix(good).unwrap_or_else(|e| panic!("{good:?} rejected: {e}"));
            validate_serve_prefix(good).unwrap_or_else(|e| panic!("{good:?} rejected: {e}"));
        }
        // Malformed prefixes are rejected for both roles.
        for bad in ["", "/leading", "trailing/", "a//b"] {
            assert!(
                validate_query_prefix(bad).is_err(),
                "{bad:?} should be rejected"
            );
            assert!(
                validate_serve_prefix(bad).is_err(),
                "{bad:?} should be rejected"
            );
        }
        // Single-segment wildcards are legal to *query* (a multi-holder probe,
        // RFC-sanctioned) but never to serve or publish under.
        for wild in ["v1/*/@blob/artifact", "wild/*/card", "v1/host$*/x"] {
            validate_query_prefix(wild)
                .unwrap_or_else(|e| panic!("{wild:?} must be queryable: {e}"));
            assert!(
                validate_serve_prefix(wild).is_err(),
                "{wild:?} must not be servable"
            );
        }
        // `**` is refused for both roles: a blob id is resolved positionally,
        // so nothing could answer past an unbounded span.
        for span in ["wild/**", "**", "a/**/b"] {
            assert!(
                validate_query_prefix(span).is_err(),
                "{span:?} must not be queryable"
            );
            assert!(
                validate_serve_prefix(span).is_err(),
                "{span:?} must not be servable"
            );
            assert_eq!(
                crate::parse_id(span, &format!("{span}/A/manifest")),
                None,
                "parse_id must not resolve an id past '**'"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn confined_dir_creation_refuses_symlinks() {
        let root = tempfile::tempdir().unwrap();
        // Normal nested creation works and returns the leaf.
        let leaf = create_dir_confined(root.path(), Path::new("a/b/c")).unwrap();
        assert!(leaf.is_dir());
        // A symlinked component (even pointing inside the root) is refused.
        std::os::unix::fs::symlink(root.path().join("a"), root.path().join("link")).unwrap();
        assert!(create_dir_confined(root.path(), Path::new("link/x")).is_err());
        // A file in the way is refused, not clobbered.
        std::fs::write(root.path().join("f"), b"x").unwrap();
        assert!(create_dir_confined(root.path(), Path::new("f/child")).is_err());
    }

    #[test]
    fn symlink_targets_checked() {
        // link at "sub/link" (depth 1 parent).
        let link = Path::new("sub/link");
        assert!(sanitize_symlink_target(link, "hello.txt").is_ok());
        assert!(sanitize_symlink_target(link, "../big.bin").is_ok()); // still inside root
        assert!(sanitize_symlink_target(link, "../../evil").is_err()); // escapes
        assert!(sanitize_symlink_target(link, "/etc/passwd").is_err());
        // link at the root (depth 0 parent).
        let root_link = Path::new("link");
        assert!(sanitize_symlink_target(root_link, "file").is_ok());
        assert!(sanitize_symlink_target(root_link, "../x").is_err());
    }
}
