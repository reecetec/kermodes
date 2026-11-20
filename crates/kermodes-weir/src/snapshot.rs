//! Catching up a replica too far behind for the log (architecture.md §7.6): it copies a peer's
//! partition files, which hold everything up to a position the peer's log starts after, then
//! takes the log from there as any follower does.
//!
//! ```text
//! {root}/weir.snapshot/    the copy, with a log of its own, until it replaces weir/
//! {root}/weir.install      the copy is whole: replacing weir/ with it is under way
//! ```
//!
//! Raft hands a follower a snapshot as an index and term only, and only for entries before its
//! leader's log starts, so the snapshot holds every entry the follower ever acknowledged: the
//! follower stops, and its store is closed. Installing copies the peer's files into
//! `weir.snapshot/`, with a log starting after the snapshot's index that keeps the follower's
//! Raft term and vote, and makes all of it durable. Only then does the marker say the copy is
//! whole; the old files go, the marker says so, and the copy's files move into `weir/` one by
//! one, the marker going last. Opening the store finishes a replacement under way, or drops a
//! copy that never became whole (the follower is as it was, and is handed a snapshot again),
//! then recovers as from any crash: index records past the snapshot's index, copied while the
//! peer went on writing, are dropped.

use std::collections::BTreeSet;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use raft::eraftpb::HardState;

use crate::disk::{self, DiskRef};
use crate::store::lock;
use crate::{Error, wal};

/// The replica's files under its store's directory.
const WEIR: &str = "weir";
/// What a snapshot never copies or replaces: the log, which each replica writes for itself,
/// and the record of an fsync that failed, which is the machine's.
const LOG: &str = "log";
const FSYNC_FAILED: &str = "fsync-failed";
const STAGING: &str = "weir.snapshot";
const MARKER: &str = "weir.install";
/// What copying reads at a time.
const CHUNK: u64 = 8 << 20;

/// The marker's phases: the old files are still to go, or the copy's are moving in.
const OLD_GOING: u8 = 1;
const COPY_MOVING: u8 = 2;

/// A peer whose partition files a replica copies: the peer's store itself, in a simulation, or
/// its server.
pub trait SnapshotSource {
    /// The last entry the peer applied, its index and term, and every file of its tables as
    /// of it: each one's path under the peer's `weir` directory, and its length (files only
    /// grow, so that much of each is there to copy). The peer changes none of them otherwise
    /// until it is done listing; a date it drops after makes reading that date's files fail.
    #[allow(clippy::type_complexity)]
    fn files(&self) -> Result<((u64, u64), Vec<(PathBuf, u64)>), Error>;
    /// Up to `len` bytes of the file at `path`, from `offset`.
    fn read(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, Error>;
}

/// The snapshot a replica wants (`Store::snapshot_wanted`): the replica to copy from, the index
/// its leader's log starts after (the copy must hold at least that much), and the replica's own
/// Raft term and vote, which its new log keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotWanted {
    pub from: u64,
    pub(crate) index: u64,
    pub(crate) raft_term: u64,
    pub(crate) vote: u64,
}

/// Installs `wanted`, copied from `from`, in the closed store in `root` (module docs).
pub(crate) fn install(
    disk: &DiskRef,
    root: &Path,
    segment_bytes: u64,
    wanted: &SnapshotWanted,
    from: &dyn SnapshotSource,
) -> Result<(), Error> {
    let staging = root.join(STAGING);
    remove_dir(disk, root, &staging)?;
    let mut dirs = BTreeSet::new();
    // The files as of the peer's last entry applied: a drop after it would have changed them
    // under a replica that replays from the snapshot's index instead.
    let (at, files) = from.files()?;
    if at.0 < wanted.index {
        return Err(Error::Io(format!(
            "replica {} has applied up to index {}, short of the {} to copy",
            wanted.from, at.0, wanted.index
        )));
    }
    for (path, len) in files {
        let to = staging.join(&path);
        let parent = to.parent().unwrap_or(&staging).to_owned();
        dirs.extend(disk::create_dirs(&**disk, root, &parent).map_err(|e| Error::io(&parent, e))?);
        dirs.insert(parent);
        let mut file = disk.create(&to, false).map_err(|e| Error::io(&to, e))?;
        let mut offset = 0;
        while offset < len {
            let bytes = from.read(&path, offset, CHUNK.min(len - offset))?;
            if bytes.is_empty() {
                return Err(Error::Io(format!(
                    "{}: ended at {offset} bytes of {len}",
                    path.display()
                )));
            }
            file.write_all(&bytes).map_err(|e| Error::io(&to, e))?;
            offset += bytes.len() as u64;
        }
        file.sync_data().map_err(|e| Error::io(&to, e))?;
    }
    for dir in dirs.iter().rev() {
        disk.sync_dir(dir).map_err(|e| Error::io(dir, e))?;
    }
    let state = HardState {
        term: wanted.raft_term,
        vote: wanted.vote,
        commit: at.0,
    };
    wal::reset(disk, root, &staging.join(LOG), segment_bytes, at, &state)?;
    mark(disk, root, OLD_GOING)?;
    finish(disk, root)
}

/// Finishes replacing the replica's files with a whole copy, or drops a copy that never became
/// whole: what opening the store does first.
pub(crate) fn recover(disk: &DiskRef, root: &Path) -> Result<(), Error> {
    if marked(disk, root)?.is_some() {
        finish(disk, root)
    } else {
        remove_dir(disk, root, &root.join(STAGING))
    }
}

/// Replaces the replica's files with the whole copy, from wherever the marker says it got to.
fn finish(disk: &DiskRef, root: &Path) -> Result<(), Error> {
    let weir = root.join(WEIR);
    let staging = root.join(STAGING);
    if marked(disk, root)? == Some(OLD_GOING) {
        wipe(disk, &weir)?;
        mark(disk, root, COPY_MOVING)?;
    }
    for path in tree(disk, &staging)? {
        let to = weir.join(&path);
        let parent = to.parent().unwrap_or(&weir).to_owned();
        disk::create_dirs(&**disk, root, &parent).map_err(|e| Error::io(&parent, e))?;
        let from = staging.join(&path);
        disk.rename(&from, &to).map_err(|e| Error::io(&to, e))?;
    }
    // Every directory of the replica's files durable, the deepest first, however much was
    // moved: a run finishing what a crash cut short may have nothing left to move, and names
    // the stopped process moved still to make durable.
    let mut dirs = directories(disk, &weir)?;
    dirs.sort_by_key(|dir| std::cmp::Reverse(dir.components().count()));
    for dir in dirs.iter().map(PathBuf::as_path).chain([root]) {
        disk.sync_dir(dir).map_err(|e| Error::io(dir, e))?;
    }
    remove_dir(disk, root, &staging)?;
    let marker = root.join(MARKER);
    disk.remove_file(&marker)
        .map_err(|e| Error::io(&marker, e))?;
    disk.sync_dir(root).map_err(|e| Error::io(root, e))
}

/// `dir` and every directory under it; none if `dir` is not there.
fn directories(disk: &DiskRef, dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_owned()];
    while let Some(at) = pending.pop() {
        let names = match disk.list(&at) {
            Ok(names) => names,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && at == dir => return Ok(found),
            Err(e) => return Err(Error::io(&at, e)),
        };
        for name in names {
            let path = at.join(name);
            if disk.list(&path).is_ok() {
                pending.push(path);
            }
        }
        found.push(at);
    }
    Ok(found)
}

/// Records, durably, how far replacing the replica's files has got.
fn mark(disk: &DiskRef, root: &Path, phase: u8) -> Result<(), Error> {
    let path = root.join(MARKER);
    let staged = root.join(format!("{MARKER}.new"));
    let mut bytes = vec![phase];
    bytes.extend_from_slice(&crc32fast::hash(&[phase]).to_le_bytes());
    let mut file = disk
        .create(&staged, false)
        .map_err(|e| Error::io(&staged, e))?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&staged, e))?;
    disk.rename(&staged, &path)
        .map_err(|e| Error::io(&path, e))?;
    disk.sync_dir(root).map_err(|e| Error::io(root, e))
}

/// How far replacing the replica's files has got, if it is under way.
fn marked(disk: &DiskRef, root: &Path) -> Result<Option<u8>, Error> {
    let path = root.join(MARKER);
    match disk.read(&path) {
        Ok(bytes)
            if bytes.len() == 5 && crc32fast::hash(&bytes[..1]).to_le_bytes() == bytes[1..] =>
        {
            Ok(Some(bytes[0]))
        }
        Ok(_) => Err(Error::Corrupt(format!("{}: unreadable", path.display()))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(&path, e)),
    }
}

/// Deletes the replica's tables' files and log, durably: everything in `weir` but the record
/// of a failed fsync.
fn wipe(disk: &DiskRef, weir: &Path) -> Result<(), Error> {
    let names = match disk.list(weir) {
        Ok(names) => names,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::io(weir, e)),
    };
    for name in names.iter().filter(|name| *name != FSYNC_FAILED) {
        let path = weir.join(name);
        let removed = if disk.list(&path).is_ok() {
            disk.remove_dir_all(&path)
        } else {
            disk.remove_file(&path)
        };
        removed.map_err(|e| Error::io(&path, e))?;
    }
    disk.sync_dir(weir).map_err(|e| Error::io(weir, e))
}

/// Deletes `dir`, if it is there, durably.
fn remove_dir(disk: &DiskRef, root: &Path, dir: &Path) -> Result<(), Error> {
    match disk.remove_dir_all(dir) {
        Ok(()) => disk.sync_dir(root).map_err(|e| Error::io(root, e)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io(dir, e)),
    }
}

/// Every file under `dir`, its path relative to it; none if `dir` is not there.
fn tree(disk: &DiskRef, dir: &Path) -> Result<Vec<PathBuf>, Error> {
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::new()];
    while let Some(at) = dirs.pop() {
        let full = dir.join(&at);
        let names = match disk.list(&full) {
            Ok(names) => names,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && at.as_os_str().is_empty() => {
                return Ok(files);
            }
            Err(e) => return Err(Error::io(&full, e)),
        };
        for name in names {
            let path = at.join(&name);
            if disk.list(&dir.join(&path)).is_ok() {
                dirs.push(path);
            } else {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// The files of a store's tables (`SnapshotSource::files`), on `disk` under `root`.
fn files(disk: &DiskRef, root: &Path) -> Result<Vec<(PathBuf, u64)>, Error> {
    let weir = root.join(WEIR);
    tree(disk, &weir)?
        .into_iter()
        .filter(|path| {
            let first = path.components().next().map(|c| c.as_os_str());
            first != Some(LOG.as_ref()) && first != Some(FSYNC_FAILED.as_ref())
        })
        .map(|path| {
            let full = weir.join(&path);
            let len = disk.len(&full).map_err(|e| Error::io(&full, e))?;
            Ok((path, len))
        })
        .collect()
}

/// Up to `len` bytes of a store's file at `path` (`SnapshotSource::read`).
fn read(disk: &DiskRef, root: &Path, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, Error> {
    let full = root.join(WEIR).join(path);
    let mut file = disk.open(&full, false).map_err(|e| Error::io(&full, e))?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| Error::io(&full, e))?;
    let mut bytes = Vec::with_capacity(len as usize);
    file.take(len)
        .read_to_end(&mut bytes)
        .map_err(|e| Error::io(&full, e))?;
    Ok(bytes)
}

/// A peer's store in this process: a simulation's.
impl SnapshotSource for crate::Store {
    fn files(&self) -> Result<((u64, u64), Vec<(PathBuf, u64)>), Error> {
        let position = lock(self.position());
        Ok((*position, files(self.disk(), self.root())?))
    }

    fn read(&self, path: &Path, offset: u64, len: u64) -> Result<Vec<u8>, Error> {
        read(self.disk(), self.root(), path, offset, len)
    }
}
