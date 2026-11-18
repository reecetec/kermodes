//! The files a store keeps (architecture.md §7.6): on the machine's disk, or a simulation's.
//!
//! Every file weir writes or reads goes through `Disk`, so a simulation can decide what each
//! operation does: lose what was not fsynced at a power loss, tear the last write, fail an
//! fsync. The operations are the ones weir needs, with the guarantees a POSIX file system gives
//! them and no more: a file's bytes are durable once it is fsynced, and a directory's entries
//! (files created, renamed, or removed in it) once the directory is.

use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nix::sys::statvfs::statvfs;

/// A store's disk.
pub type DiskRef = Arc<dyn Disk>;

pub trait Disk: Send + Sync {
    /// Creates `dir` and whatever of its ancestors are missing.
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
    /// The names of `dir`'s entries.
    fn list(&self, dir: &Path) -> io::Result<Vec<String>>;
    /// Opens `path` to write from its start: a new file, failing if one is there (`new`), or
    /// emptied if it is.
    fn create(&self, path: &Path, new: bool) -> io::Result<Box<dyn DiskFile>>;
    /// Opens the file at `path`, to read, or (`write`) to read and write.
    fn open(&self, path: &Path, write: bool) -> io::Result<Box<dyn DiskFile>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn len(&self, path: &Path) -> io::Result<u64>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn remove_dir_all(&self, dir: &Path) -> io::Result<()>;
    /// Replaces `to` with `from`, atomically.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    /// Makes `dir`'s entries durable: the files created, renamed, and removed in it.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    /// The size of the volume holding `path`, and its free space, in bytes.
    fn space(&self, path: &Path) -> io::Result<(u64, u64)>;
    /// Whether an fsync through this disk (a directory's, or a file's it opened) has failed.
    fn fsync_failed(&self) -> bool;
    /// Whether an fsync failed on this machine since it started, through any process's disk:
    /// what it held may never reach the disk while reads still find it (Linux marks the pages
    /// clean), so nothing read here can be trusted until the machine restarts.
    fn untrusted(&self) -> io::Result<bool>;
}

/// An open file. Writes are durable once synced.
pub trait DiskFile: Read + Write + Seek + Send + Sync {
    /// Makes the file's bytes durable (and its length, as `fdatasync` does).
    fn sync_data(&self) -> io::Result<()>;
    /// Makes the file's bytes and its metadata durable.
    fn sync_all(&self) -> io::Result<()>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    /// Reserves `len` bytes for the file (zeros, where not yet written), so a later fsync need
    /// not also commit its growth.
    fn allocate(&self, len: u64) -> io::Result<()>;
    /// Another handle on the same file, to sync it from elsewhere.
    fn try_clone(&self) -> io::Result<Box<dyn DiskFile>>;
}

/// Creates `dir` (and its missing ancestors) under `root`, a directory that is durably there:
/// the directories whose entries lead from `root` down to `dir`, which must be fsynced for
/// `dir` to outlast a power loss. All of them, not only those created now: a process that
/// created one may have died before it fsynced it, and the next finds it there, not durable.
pub(crate) fn create_dirs(disk: &dyn Disk, root: &Path, dir: &Path) -> io::Result<Vec<PathBuf>> {
    disk.create_dir_all(dir)?;
    Ok(dir
        .ancestors()
        .skip(1)
        .take_while(|parent| parent.starts_with(root))
        .map(Path::to_owned)
        .collect())
}

/// The machine's file system. A failed fsync is recorded at `record`, with the boot it failed
/// on, for every later process on that boot (`untrusted`).
pub fn real(record: &Path) -> DiskRef {
    Arc::new(Real {
        failures: Arc::new(Failures {
            failed: AtomicBool::new(false),
            record: record.to_owned(),
        }),
    })
}

struct Real {
    failures: Arc<Failures>,
}

impl Real {
    fn file(&self, file: std::fs::File) -> Box<dyn DiskFile> {
        Box::new(RealFile {
            file,
            failures: self.failures.clone(),
        })
    }
}

/// A disk's failed fsyncs: this process's, and the record every process on the boot reads.
struct Failures {
    failed: AtomicBool,
    record: PathBuf,
}

impl Failures {
    /// `synced`; if it failed, noted, and recorded for the boot before the failure is reported,
    /// so a process killed a moment later still leaves it. The record is not fsynced: it must
    /// outlast processes, not the boot it names.
    fn note(&self, synced: io::Result<()>) -> io::Result<()> {
        if synced.is_err() {
            self.failed.store(true, Ordering::Relaxed);
            let recorded = boot().and_then(|boot| {
                if let Some(dir) = self.record.parent() {
                    std::fs::create_dir_all(dir)?;
                }
                std::fs::write(&self.record, boot)
            });
            if let Err(e) = recorded {
                eprintln!(
                    "kermodes weir: an fsync failed, and recording it failed too ({e}): do not \
                     open weir again before the machine restarts"
                );
            }
        }
        synced
    }
}

impl Disk for Real {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        std::fs::read_dir(dir)?
            .map(|entry| Ok(entry?.file_name().to_string_lossy().into_owned()))
            .collect()
    }

    fn create(&self, path: &Path, new: bool) -> io::Result<Box<dyn DiskFile>> {
        let mut options = std::fs::OpenOptions::new();
        options.write(true);
        if new {
            options.create_new(true);
        } else {
            options.create(true).truncate(true);
        }
        Ok(self.file(options.open(path)?))
    }

    fn open(&self, path: &Path, write: bool) -> io::Result<Box<dyn DiskFile>> {
        Ok(self.file(
            std::fs::OpenOptions::new()
                .read(true)
                .write(write)
                .open(path)?,
        ))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn len(&self, path: &Path) -> io::Result<u64> {
        Ok(std::fs::metadata(path)?.len())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, dir: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(dir)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        self.failures.note(std::fs::File::open(dir)?.sync_all())
    }

    fn space(&self, path: &Path) -> io::Result<(u64, u64)> {
        let volume = statvfs(path).map_err(io::Error::from)?;
        let unit = widen(volume.fragment_size());
        Ok((
            widen(volume.blocks()) * unit,
            widen(volume.blocks_available()) * unit,
        ))
    }

    fn fsync_failed(&self) -> bool {
        self.failures.failed.load(Ordering::Relaxed)
    }

    fn untrusted(&self) -> io::Result<bool> {
        match std::fs::read(&self.failures.record) {
            Ok(recorded) => Ok(recorded == boot()?.as_bytes()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    }
}

/// Linux's id for this boot, new each time the machine starts: a page cache does not outlive
/// it.
#[cfg(target_os = "linux")]
fn boot() -> io::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?
        .trim()
        .to_owned())
}

/// Elsewhere (macOS in development), when the machine started.
#[cfg(not(target_os = "linux"))]
fn boot() -> io::Result<String> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "kern.boottime"])
        .output()?;
    if !out.status.success() {
        return Err(io::Error::other("sysctl kern.boottime failed"));
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

/// `statvfs`'s counts as u64: 32 bits on some platforms (macOS), 64 on others (Linux).
fn widen(n: impl Into<u64>) -> u64 {
    n.into()
}

/// A file on the machine's disk, which notes a failed fsync on the disk it came from.
struct RealFile {
    file: std::fs::File,
    failures: Arc<Failures>,
}

impl Read for RealFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.file.read(buf)
    }
}

impl Write for RealFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for RealFile {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        self.file.seek(to)
    }
}

impl DiskFile for RealFile {
    fn sync_data(&self) -> io::Result<()> {
        self.failures.note(self.file.sync_data())
    }

    fn sync_all(&self) -> io::Result<()> {
        self.failures.note(self.file.sync_all())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    #[cfg(target_os = "linux")]
    fn allocate(&self, len: u64) -> io::Result<()> {
        nix::fcntl::posix_fallocate(&self.file, 0, len as i64).map_err(io::Error::from)
    }

    /// Elsewhere (macOS in development), only extended: reads past what was written still
    /// find zeros.
    #[cfg(not(target_os = "linux"))]
    fn allocate(&self, len: u64) -> io::Result<()> {
        if self.file.metadata()?.len() < len {
            self.file.set_len(len)?;
        }
        Ok(())
    }

    fn try_clone(&self) -> io::Result<Box<dyn DiskFile>> {
        Ok(Box::new(RealFile {
            file: self.file.try_clone()?,
            failures: self.failures.clone(),
        }))
    }
}
