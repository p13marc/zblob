//! Filesystem safety helpers.
//!
//! Everything that materializes remote-controlled paths goes through here.
//! A tree index (or a manifest filename) is attacker input: absolute paths,
//! `..`, drive prefixes, and symlink tricks must all die at this boundary, not
//! at the write site (the v1 path-traversal vector, C2).

use std::collections::BTreeMap;
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

/// Validate a symlink target from an index in isolation: must be relative, and
/// resolving it *lexically* from the link's parent directory must stay inside
/// the tree root (`link_rel` is the sanitized link path relative to the root).
///
/// This is necessary and **not sufficient**. Counting `Normal` components as
/// +1 depth is only valid while every such component is a real directory — if
/// one is itself a symlink, the kernel resolves it before continuing and the
/// arithmetic no longer describes where the path lands. A chain of links
/// declared by one index therefore passes this check and still escapes; see
/// [`assert_symlinks_confined`], which is what actually decides containment.
/// This function stays because it is cheap, runs per entry, and produces the
/// better diagnostic for the common single-link mistake.
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

/// Substitutions allowed while resolving one index's symlinks. Bounds both
/// chain length and cycles; the kernel's own limit (`ELOOP`) is 40 on Linux.
const MAX_SYMLINK_HOPS: u32 = 32;

/// Decide whether the symlinks an index declares stay inside the tree root
/// **once they are all materialized**, by resolving each target the way the
/// kernel will: following any component that is itself a link this index
/// declares.
///
/// This is the check [`sanitize_symlink_target`] cannot make. Given
///
/// ```text
/// Symlink { path: "sub/link", target: ".."                        }
/// Symlink { path: "e",        target: "sub/link/../../etc/passwd" }
/// ```
///
/// the lexical depth walk for `e` is `sub`(1) `link`(2) `..`(1) `..`(0)
/// `etc`(1) `passwd`(2) and never goes negative — but `sub/link` resolves to
/// the root, so `sub/link/..` is the root's *parent* and `e` points outside.
/// Nothing is written through such a link during materialization (symlinks are
/// created last and nothing writes after them), but the resulting tree hands
/// the escape to whatever reads it next, which is not what the crate promises.
///
/// `links` maps each declared link's sanitized path components to its raw
/// target. Resolution is iterative-with-substitution under a hop budget, so a
/// cycle terminates as an error rather than looping.
pub(crate) fn assert_symlinks_confined(links: &BTreeMap<Vec<String>, String>) -> Result<()> {
    for (link, target) in links {
        let base = &link[..link.len() - 1];
        let mut budget = MAX_SYMLINK_HOPS;
        resolve_confined(base, target, links, &mut budget, link)?;
    }
    Ok(())
}

/// Resolve `target` relative to the directory `base` (components from the tree
/// root), substituting declared symlinks, and return where it lands. Errors if
/// it ever steps above the root or exhausts the hop budget.
fn resolve_confined(
    base: &[String],
    target: &str,
    links: &BTreeMap<Vec<String>, String>,
    budget: &mut u32,
    origin: &[String],
) -> Result<Vec<String>> {
    if *budget == 0 {
        return Err(BlobError::Protocol(format!(
            "symlink {:?} exceeds {MAX_SYMLINK_HOPS} resolution hops (cycle?)",
            origin.join("/")
        )));
    }
    *budget -= 1;

    let mut cur = base.to_vec();
    for comp in Path::new(target).components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                if cur.pop().is_none() {
                    return Err(BlobError::Protocol(format!(
                        "symlink {:?} resolves outside the tree root via {target:?}",
                        origin.join("/")
                    )));
                }
            }
            Component::Normal(c) => {
                let name = c.to_str().ok_or_else(|| {
                    BlobError::Protocol(format!("non-UTF-8 symlink component in {target:?}"))
                })?;
                cur.push(name.to_string());
                // A component that is itself a declared link is followed, just
                // as the kernel would follow it.
                if let Some(next) = links.get(&cur) {
                    let parent = cur[..cur.len() - 1].to_vec();
                    cur = resolve_confined(&parent, next, links, budget, origin)?;
                }
            }
            other => {
                return Err(BlobError::Protocol(format!(
                    "unsafe symlink target component {other:?} in {target:?}"
                )));
            }
        }
    }
    Ok(cur)
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
