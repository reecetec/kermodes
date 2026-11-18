//! A simulated disk (architecture.md §7.6): weir's files in memory, with what a power loss
//! would leave of them, decided by a seed.
//!
//! It keeps two states. What a running process sees: every write, at once. And what is
//! durable: a file's bytes as of its last fsync, and a directory's entries (files created,
//! renamed, removed in it, and directories created in it) as of its last fsync. A power loss
//! puts the disk back to the durable state, keeping of each file a random prefix of the writes
//! made since its last fsync, the last of them possibly torn, as a disk that had written some
//! of them by chance would.
//!
//! A failed fsync is as Linux's: the pages it held are marked clean, so they never reach the
//! disk, not even by a later fsync, while the process (and any other, until the machine
//! restarts) still reads them. A power loss is also the machine restarting.
//!
//! Two crashes: the power goes (`power_loss`), or only the process dies (`kill`, as `kill -9`
//! does), leaving what it wrote in the page cache, where the next process reads it, durable or
//! not. Either comes between two operations or in the middle of one: armed with
//! `power_loss_after` or `kill_after`, it comes at the given operation that changes the disk,
//! which fails. The process that was running is gone: its handle on the disk (`handle`) and
//! every file it has open fail from then on, so nothing it does in closing reaches the disk.
//!
//! It is deterministic: the same seed and the same operations give the same disk, so a run
//! that breaks an invariant is replayed exactly.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::disk::{self, DiskFile, DiskRef};

/// A simulated disk. Clones share it.
#[derive(Clone)]
pub struct Disk {
    state: Arc<Mutex<State>>,
    /// The crashes before the process this handle belongs to started; `None` for the
    /// simulation's own handle, which outlives every process.
    process: Option<u64>,
    /// Set by an fsync that failed through this handle or a file it opened.
    failed: Arc<AtomicBool>,
}

struct State {
    rng: u64,
    /// How many times the power went: the machine's boot.
    power_losses: u64,
    /// How many times a process died, by power loss or kill: one started before the last is
    /// gone.
    crashes: u64,
    /// The crash due at this many more operations that change the disk.
    countdown: Option<(u64, Crash)>,
    /// One in this many fsyncs fails, losing what it would have made durable (0: none).
    failing_syncs: u64,
    /// The boot (power losses) an fsync last failed on: until the next, the disk lies.
    lied: Option<u64>,
    next_inode: u64,
    files: BTreeMap<u64, File>,
    /// Names as the process sees them, and as they are durable: path to inode.
    names: BTreeMap<PathBuf, u64>,
    durable_names: BTreeMap<PathBuf, u64>,
    dirs: BTreeSet<PathBuf>,
    durable_dirs: BTreeSet<PathBuf>,
}

#[derive(Clone, Copy)]
enum Crash {
    PowerLoss,
    Kill,
}

#[derive(Clone, Default)]
struct File {
    data: Vec<u8>,
    durable: Vec<u8>,
    /// Changes since the last fsync, in order: what a power loss keeps a prefix of.
    pending: Vec<Change>,
}

#[derive(Clone)]
enum Change {
    Write { at: u64, bytes: Vec<u8> },
    SetLen(u64),
}

impl Change {
    fn apply(&self, data: &mut Vec<u8>) {
        match self {
            Change::Write { at, bytes } => {
                let end = *at as usize + bytes.len();
                if data.len() < end {
                    data.resize(end, 0);
                }
                data[*at as usize..end].copy_from_slice(bytes);
            }
            Change::SetLen(len) => data.resize(*len as usize, 0),
        }
    }
}

fn gone() -> io::Error {
    io::Error::other("the power went: this process is gone")
}

fn not_found(path: &Path, what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("{}: no such {what}", path.display()),
    )
}

impl State {
    /// xorshift64*: deterministic, and enough for choosing faults.
    fn below(&mut self, n: u64) -> u64 {
        self.rng ^= self.rng >> 12;
        self.rng ^= self.rng << 25;
        self.rng ^= self.rng >> 27;
        self.rng.wrapping_mul(0x2545_f491_4f6c_dd1d) % n.max(1)
    }

    /// Before an operation that changes the disk: the crash comes now, if it was set to, and
    /// the operation fails.
    fn tick(&mut self) -> io::Result<()> {
        match self.countdown {
            Some((0, crash)) => {
                self.countdown = None;
                match crash {
                    Crash::PowerLoss => self.lose_power(),
                    Crash::Kill => self.crashes += 1,
                }
                Err(gone())
            }
            Some((n, crash)) => {
                self.countdown = Some((n - 1, crash));
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// The disk keeps what was durable, and of each file a random prefix of the changes since
    /// its last fsync, the last of them possibly torn.
    fn lose_power(&mut self) {
        self.power_losses += 1;
        self.crashes += 1;
        self.names = self.durable_names.clone();
        self.dirs = self.durable_dirs.clone();
        // An entry whose directory is gone went with it.
        let dirs = self.dirs.clone();
        let present = |path: &Path| path.ancestors().skip(1).all(|dir| dirs.contains(dir));
        self.dirs.retain(|dir| present(dir));
        self.names.retain(|path, _| present(path));
        self.durable_dirs = self.dirs.clone();
        self.durable_names = self.names.clone();
        let named: BTreeSet<u64> = self.names.values().copied().collect();
        self.files.retain(|inode, _| named.contains(inode));
        let inodes: Vec<u64> = self.files.keys().copied().collect();
        for inode in inodes {
            let pending = std::mem::take(&mut self.files.get_mut(&inode).expect("listed").pending);
            let kept = self.below(pending.len() as u64 + 1) as usize;
            let mut data = self.files[&inode].durable.clone();
            for (i, change) in pending.iter().take(kept).enumerate() {
                match change {
                    // The last write kept may be torn: only some of its bytes reached the disk.
                    Change::Write { at, bytes } if i + 1 == kept => {
                        let torn = self.below(bytes.len() as u64 + 1) as usize;
                        Change::Write {
                            at: *at,
                            bytes: bytes[..torn].to_vec(),
                        }
                        .apply(&mut data);
                    }
                    change => change.apply(&mut data),
                }
            }
            let file = self.files.get_mut(&inode).expect("listed above");
            file.data = data.clone();
            file.durable = data;
        }
    }

    fn file(&mut self, inode: u64) -> io::Result<&mut File> {
        self.files
            .get_mut(&inode)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "the file is gone"))
    }

    fn sync(&mut self, inode: u64) -> io::Result<()> {
        self.tick()?;
        let failing = self.failing_syncs > 0 && self.below(self.failing_syncs) == 0;
        let file = self.file(inode)?;
        if failing {
            // As Linux does after a failed fsync: the pages are marked clean, so what it held
            // never reaches the disk, while the process still reads it.
            file.pending.clear();
            self.lied = Some(self.power_losses);
            return Err(io::Error::other("simulated fsync failure"));
        }
        // What the disk gets is the changes not yet written: a failed fsync's are not among
        // them, whatever the process reads.
        for change in std::mem::take(&mut file.pending) {
            change.apply(&mut file.durable);
        }
        Ok(())
    }

    fn inode(&self, path: &Path) -> io::Result<u64> {
        self.names
            .get(path)
            .copied()
            .ok_or_else(|| not_found(path, "file"))
    }

    fn dir(&self, dir: &Path) -> io::Result<()> {
        if self.dirs.contains(dir) {
            Ok(())
        } else {
            Err(not_found(dir, "directory"))
        }
    }
}

impl Disk {
    /// An empty disk whose faults are chosen by `seed`; one fsync in `failing_syncs` fails
    /// (0: none). `root` and its ancestors exist, durably, as a store's directory on a
    /// machine does before weir starts.
    pub fn new(seed: u64, failing_syncs: u64, root: &Path) -> Self {
        let dirs: BTreeSet<PathBuf> = root.ancestors().map(Path::to_owned).collect();
        Self {
            state: Arc::new(Mutex::new(State {
                rng: seed.max(1),
                power_losses: 0,
                crashes: 0,
                countdown: None,
                failing_syncs,
                lied: None,
                next_inode: 1,
                files: BTreeMap::new(),
                names: BTreeMap::new(),
                durable_names: BTreeMap::new(),
                dirs: dirs.clone(),
                durable_dirs: dirs,
            })),
            process: None,
            failed: Arc::default(),
        }
    }

    /// The disk for a process (a store) starting now: it fails once the process crashes.
    pub fn handle(&self) -> DiskRef {
        let crashes = self.lock().crashes;
        Arc::new(Self {
            state: self.state.clone(),
            process: Some(crashes),
            failed: Arc::default(),
        })
    }

    /// The power goes now.
    pub fn power_loss(&self) {
        self.lock().lose_power();
    }

    /// The process dies now; the machine, and what its page cache holds, stay.
    pub fn kill(&self) {
        self.lock().crashes += 1;
    }

    /// The power goes at the `n`th operation from now that changes the disk (0: the next),
    /// which fails: in the middle of whatever the store is doing.
    pub fn power_loss_after(&self, n: u64) {
        self.lock().countdown = Some((n, Crash::PowerLoss));
    }

    /// As `power_loss_after`, the process dying instead.
    pub fn kill_after(&self, n: u64) {
        self.lock().countdown = Some((n, Crash::Kill));
    }

    /// Forgets a crash armed and not yet happened.
    pub fn disarm(&self) {
        self.lock().countdown = None;
    }

    /// How many times the power has gone.
    pub fn power_losses(&self) -> u64 {
        self.lock().power_losses
    }

    /// How many times a process has died, by power loss or kill.
    pub fn crashes(&self) -> u64 {
        self.lock().crashes
    }

    /// A fingerprint of what the disk holds under `root` now: equal for runs that did the same,
    /// wherever their root.
    pub fn fingerprint(&self, root: &Path) -> u64 {
        let state = self.lock();
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        let mut eat = |bytes: &[u8]| {
            for &b in bytes {
                hash = (hash ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3);
            }
        };
        for dir in &state.dirs {
            if let Ok(dir) = dir.strip_prefix(root) {
                eat(dir.to_string_lossy().as_bytes());
            }
        }
        for (path, inode) in &state.names {
            if let Ok(relative) = path.strip_prefix(root) {
                eat(relative.to_string_lossy().as_bytes());
                eat(&state.files[inode].data);
            }
        }
        hash
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("simulated disk poisoned by an earlier panic")
    }

    /// The disk, if this handle's process is still running.
    fn live(&self) -> io::Result<MutexGuard<'_, State>> {
        let state = self.lock();
        match self.process {
            Some(started) if started != state.crashes => Err(gone()),
            _ => Ok(state),
        }
    }

    fn file_handle(&self, state: &State, inode: u64, write: bool) -> Box<dyn DiskFile> {
        Box::new(Handle {
            state: self.state.clone(),
            process: state.crashes,
            inode,
            at: 0,
            write,
            failed: self.failed.clone(),
        })
    }
}

impl disk::Disk for Disk {
    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.live()?;
        state.tick()?;
        for at in dir.ancestors() {
            state.dirs.insert(at.to_owned());
        }
        Ok(())
    }

    fn exists(&self, path: &Path) -> bool {
        self.live()
            .is_ok_and(|state| state.dirs.contains(path) || state.names.contains_key(path))
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<String>> {
        let state = self.live()?;
        state.dir(dir)?;
        let children = state
            .dirs
            .iter()
            .filter(|path| path.parent() == Some(dir))
            .chain(state.names.keys().filter(|path| path.parent() == Some(dir)));
        Ok(children
            .filter_map(|path| Some(path.file_name()?.to_string_lossy().into_owned()))
            .collect())
    }

    fn create(&self, path: &Path, new: bool) -> io::Result<Box<dyn DiskFile>> {
        let mut state = self.live()?;
        state.tick()?;
        state.dir(path.parent().unwrap_or(path))?;
        let inode = match state.names.get(path) {
            Some(_) if new => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("{}: exists", path.display()),
                ));
            }
            Some(&inode) => {
                let file = state.file(inode)?;
                file.data.clear();
                file.pending.push(Change::SetLen(0));
                inode
            }
            None => {
                let inode = state.next_inode;
                state.next_inode += 1;
                state.files.insert(inode, File::default());
                state.names.insert(path.to_owned(), inode);
                inode
            }
        };
        Ok(self.file_handle(&state, inode, true))
    }

    fn open(&self, path: &Path, write: bool) -> io::Result<Box<dyn DiskFile>> {
        let state = self.live()?;
        let inode = state.inode(path)?;
        Ok(self.file_handle(&state, inode, write))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut state = self.live()?;
        let inode = state.inode(path)?;
        Ok(state.file(inode)?.data.clone())
    }

    fn len(&self, path: &Path) -> io::Result<u64> {
        self.read(path).map(|data| data.len() as u64)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let mut state = self.live()?;
        state.tick()?;
        state.inode(path)?;
        state.names.remove(path);
        Ok(())
    }

    fn remove_dir_all(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.live()?;
        state.tick()?;
        state.dir(dir)?;
        state.dirs.retain(|path| !path.starts_with(dir));
        state.names.retain(|path, _| !path.starts_with(dir));
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.live()?;
        state.tick()?;
        state.dir(to.parent().unwrap_or(to))?;
        let inode = state.inode(from)?;
        state.names.remove(from);
        state.names.insert(to.to_owned(), inode);
        Ok(())
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        let mut state = self.live()?;
        state.tick()?;
        state.dir(dir)?;
        let in_dir = |path: &Path| path.parent() == Some(dir);
        let names: Vec<(PathBuf, u64)> = state
            .names
            .iter()
            .filter(|(path, _)| in_dir(path))
            .map(|(path, inode)| (path.clone(), *inode))
            .collect();
        let dirs: Vec<PathBuf> = state
            .dirs
            .iter()
            .filter(|path| in_dir(path))
            .cloned()
            .collect();
        state.durable_names.retain(|path, _| !in_dir(path));
        state.durable_names.extend(names);
        state.durable_dirs.retain(|path| !in_dir(path));
        state.durable_dirs.extend(dirs);
        Ok(())
    }

    fn space(&self, _: &Path) -> io::Result<(u64, u64)> {
        Ok((1 << 40, 1 << 39))
    }

    fn fsync_failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }

    /// Each power loss restarts the machine.
    fn untrusted(&self) -> io::Result<bool> {
        let state = self.live()?;
        Ok(state.lied == Some(state.power_losses))
    }
}

/// An open simulated file.
struct Handle {
    state: Arc<Mutex<State>>,
    /// The crashes before it was opened: one since, and it fails.
    process: u64,
    inode: u64,
    at: u64,
    write: bool,
    /// Its process's disk's (`Disk::failed`).
    failed: Arc<AtomicBool>,
}

impl Handle {
    fn noted(&self, synced: io::Result<()>) -> io::Result<()> {
        if synced.is_err() {
            self.failed.store(true, Ordering::Relaxed);
        }
        synced
    }

    /// The disk, if this handle's process is still running.
    fn live(&self) -> io::Result<MutexGuard<'_, State>> {
        let state = self
            .state
            .lock()
            .expect("simulated disk poisoned by an earlier panic");
        if state.crashes != self.process {
            return Err(gone());
        }
        Ok(state)
    }

    fn change(&self, change: Change) -> io::Result<()> {
        if !self.write {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "opened to read",
            ));
        }
        let mut state = self.live()?;
        state.tick()?;
        let file = state.file(self.inode)?;
        change.apply(&mut file.data);
        file.pending.push(change);
        Ok(())
    }
}

impl Read for Handle {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.live()?;
        let data = &state.file(self.inode)?.data;
        let at = (self.at as usize).min(data.len());
        let n = buf.len().min(data.len() - at);
        buf[..n].copy_from_slice(&data[at..at + n]);
        drop(state);
        self.at += n as u64;
        Ok(n)
    }
}

impl Write for Handle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.change(Change::Write {
            at: self.at,
            bytes: buf.to_vec(),
        })?;
        self.at += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for Handle {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let len = self.live()?.file(self.inode)?.data.len() as i64;
        let at = match to {
            SeekFrom::Start(at) => at as i64,
            SeekFrom::End(delta) => len + delta,
            SeekFrom::Current(delta) => self.at as i64 + delta,
        };
        if at < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start",
            ));
        }
        self.at = at as u64;
        Ok(self.at)
    }
}

impl DiskFile for Handle {
    fn sync_data(&self) -> io::Result<()> {
        let synced = self.live()?.sync(self.inode);
        self.noted(synced)
    }

    fn sync_all(&self) -> io::Result<()> {
        let synced = self.live()?.sync(self.inode);
        self.noted(synced)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.change(Change::SetLen(len))
    }

    /// Nothing: reading past what was written finds no bytes here, where a real preallocated
    /// file finds zeros, and weir reads both as the end of what was written. Holding hundreds
    /// of MiB of zeros per log segment would only slow the simulation.
    fn allocate(&self, _: u64) -> io::Result<()> {
        self.live().map(drop)
    }

    fn try_clone(&self) -> io::Result<Box<dyn DiskFile>> {
        // Fails if the power went since this handle was opened.
        drop(self.live()?);
        Ok(Box::new(Handle {
            state: self.state.clone(),
            process: self.process,
            inode: self.inode,
            at: self.at,
            write: self.write,
            failed: self.failed.clone(),
        }))
    }
}
