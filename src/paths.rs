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
