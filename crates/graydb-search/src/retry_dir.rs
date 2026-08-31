//! RetryDirectory: MmapDirectory wrapper that retries transient PermissionDenied
//! errors with backoff. Rationale (measured on this dev box, 2026-08-17): endpoint
//! security intermittently returns ERROR_ACCESS_DENIED for freshly-created files
//! during tantivy's commit bursts — pure-fs churn at higher rates never fails, and
//! the same operation succeeds on retry milliseconds later. This is an environmental
//! tax on Windows hosts with AV minifilters, not a tantivy defect; the wrapper keeps
//! full mmap + on-disk persistence instead of retreating to a RAM index.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    DirectoryLock, FileHandle, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};
use tantivy::Directory;

const MAX_RETRIES: u32 = 40;
const BACKOFF: Duration = Duration::from_millis(25);

#[derive(Clone, Debug)]
pub struct RetryDirectory {
    inner: MmapDirectory,
}

impl RetryDirectory {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        Ok(RetryDirectory {
            inner: MmapDirectory::open(path)?,
        })
    }
}

fn eacces_write(e: &OpenWriteError) -> bool {
    matches!(e, OpenWriteError::IoError { io_error, .. }
        if io_error.kind() == io::ErrorKind::PermissionDenied)
}

fn eacces_delete(e: &DeleteError) -> bool {
    matches!(e, DeleteError::IoError { io_error, .. }
        if io_error.kind() == io::ErrorKind::PermissionDenied)
}

impl Directory for RetryDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        let mut attempt = 0;
        loop {
            match self.inner.delete(path) {
                Err(e) if eacces_delete(&e) && attempt < MAX_RETRIES => {
                    attempt += 1;
                    std::thread::sleep(BACKOFF);
                }
                other => {
                    if attempt > 0 {
                        tracing::debug!(?path, attempt, "delete succeeded after EACCES retries");
                    }
                    return other;
                }
            }
        }
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        let mut attempt = 0;
        loop {
            match self.inner.open_write(path) {
                Err(e) if eacces_write(&e) && attempt < MAX_RETRIES => {
                    attempt += 1;
                    std::thread::sleep(BACKOFF);
                }
                other => {
                    if attempt > 0 {
                        tracing::debug!(?path, attempt, "open_write succeeded after EACCES retries");
                    }
                    return other;
                }
            }
        }
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let mut attempt = 0;
        loop {
            match self.inner.atomic_write(path, data) {
                Err(e) if e.kind() == io::ErrorKind::PermissionDenied && attempt < MAX_RETRIES => {
                    attempt += 1;
                    std::thread::sleep(BACKOFF);
                }
                other => return other,
            }
        }
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }

    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }
}
