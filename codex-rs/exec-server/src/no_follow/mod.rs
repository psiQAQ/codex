use crate::FileMetadata;
use std::io;
use std::path::Path;
#[cfg(windows)]
use std::time::SystemTime;
#[cfg(windows)]
use std::time::UNIX_EPOCH;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix as imp;
#[cfg(windows)]
use windows as imp;

/// Creates a no-follow cache directory. Unix permissions are owner-only;
/// Windows inherits the user's Codex home ACL.
pub fn create_private_cache_directory(path: &Path) -> io::Result<()> {
    imp::create_private_cache_directory(path)
}

/// Removes one no-follow regular cache entry.
pub fn remove_cache_file(path: &Path) -> io::Result<()> {
    imp::remove_cache_file(path)
}

/// Opens a private regular cache file without following any symlink or reparse point.
/// Creation is exclusive and never truncates an existing entry.
pub fn open_cache_file(path: &Path, create_new: bool) -> io::Result<std::fs::File> {
    imp::open_cache_file(path, create_new)
}

pub(crate) fn open_file(path: &Path) -> io::Result<std::fs::File> {
    imp::open_file_sync(path)
}

pub(crate) async fn write_file(path: &Path, contents: Vec<u8>) -> io::Result<()> {
    imp::write_file(path.to_path_buf(), contents).await
}

#[cfg(unix)]
pub(crate) async fn metadata(path: &Path) -> io::Result<FileMetadata> {
    imp::metadata(path.to_path_buf()).await
}

#[cfg(windows)]
pub(crate) async fn metadata(path: &Path) -> io::Result<FileMetadata> {
    imp::metadata(path.to_path_buf())
        .await
        .map(|metadata| FileMetadata {
            is_directory: metadata.is_dir(),
            is_file: metadata.is_file(),
            is_symlink: false,
            size: metadata.len(),
            created_at_ms: metadata.created().ok().map_or(0, system_time_to_unix_ms),
            modified_at_ms: metadata.modified().ok().map_or(0, system_time_to_unix_ms),
        })
}

#[cfg(windows)]
fn system_time_to_unix_ms(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(0)
}

pub(crate) async fn create_directory(path: &Path, recursive: bool) -> io::Result<()> {
    imp::create_directory(path.to_path_buf(), recursive).await
}

pub(crate) async fn remove(path: &Path, recursive: bool, force: bool) -> io::Result<()> {
    imp::remove(path.to_path_buf(), recursive, force).await
}

#[cfg(test)]
mod cache_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn cache_creation_is_exclusive_and_does_not_truncate() {
        let root = tempfile::tempdir().expect("temporary directory");
        let cache = root.path().join("cache");
        create_private_cache_directory(&cache).expect("private cache");
        let path = cache.join("object");
        let mut file = open_cache_file(&path, /*create_new*/ true).expect("new file");
        file.write_all(b"original").expect("write");
        assert!(open_cache_file(&path, /*create_new*/ true).is_err());
        assert_eq!(std::fs::read(&path).expect("read"), b"original");
        assert!(open_cache_file(&cache, /*create_new*/ false).is_err());
    }

    #[test]
    fn cache_removal_refuses_directories() {
        let root = tempfile::tempdir().expect("temporary directory");
        let directory = root.path().join("directory");
        std::fs::create_dir(&directory).expect("directory");
        assert!(remove_cache_file(&directory).is_err());
        assert!(directory.is_dir());
        let path = root.path().join("regular");
        drop(open_cache_file(&path, /*create_new*/ true).expect("file"));
        remove_cache_file(&path).expect("regular file removal");
        assert!(!path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn cache_operations_reject_leaf_and_ancestor_symlinks() {
        let root = tempfile::tempdir().expect("temporary directory");
        let target = root.path().join("target");
        std::fs::create_dir(&target).expect("target");
        let leaf = root.path().join("leaf");
        std::fs::write(&leaf, b"original").expect("fixture");
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&leaf, &link).expect("symlink");
        assert!(open_cache_file(&link, /*create_new*/ false).is_err());
        let parent = root.path().join("parent");
        std::os::unix::fs::symlink(&target, &parent).expect("parent symlink");
        assert!(open_cache_file(&parent.join("object"), /*create_new*/ true).is_err());
        assert!(create_private_cache_directory(&parent.join("cache")).is_err());
        assert_eq!(std::fs::read(&leaf).expect("unchanged"), b"original");
    }

    #[cfg(windows)]
    #[test]
    fn cache_operations_reject_ancestor_junction() {
        let root = tempfile::tempdir().expect("temporary directory");
        let target = root.path().join("target");
        std::fs::create_dir(&target).expect("target");
        let junction = root.path().join("junction");
        let output = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&junction)
            .arg(&target)
            .output()
            .expect("create test junction");
        assert!(
            output.status.success(),
            "mklink failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result = open_cache_file(&junction.join("object"), /*create_new*/ true);
        let directory = create_private_cache_directory(&junction.join("cache"));
        std::fs::remove_dir(&junction).expect("remove only test junction");
        assert!(result.is_err());
        assert!(directory.is_err());
        assert!(!target.join("object").exists());
    }
}
