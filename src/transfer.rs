mod resume;
mod staging_batch;
mod streaming;

use anyhow::{Context, Result, bail, ensure};
use filetime::FileTime;
use resume::Checkpoints;
use ring::digest;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const STAGING_DIRECTORY: &str = ".usb-direct-transfer";
const MAX_ENTRIES: usize = 100_000;
const MAX_DEPTH: usize = 128;
const CHUNK: usize = 1024 * 1024;
const PARALLEL_CHECKSUM_MIN_BYTES: u64 = 32 * 1024 * 1024;
const PUBLICATION_BATCH_SIZE: usize = 32;
const STATE_VERSION: u32 = 1;
const STAGING_MARKER: &[u8] = b"usb-direct-transfer staging v1\n";
const PREPARING: usize = u32::MAX as usize;
const PUBLISHED: usize = 0;

pub trait ReadWrite: Read + Write {}
impl<T: Read + Write + ?Sized> ReadWrite for T {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    roots: Vec<Root>,
    skipped: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    checkpoints: BTreeMap<usize, Checkpoints>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    stream_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Root {
    name: String,
    entries: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    relative: Vec<String>,
    kind: Kind,
    modified: Modified,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Kind {
    Directory,
    File { bytes: u64, sha256: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Modified {
    seconds: i64,
    nanoseconds: u32,
}

impl Modified {
    #[allow(deprecated)] // FileTime also handles timestamps before the Unix epoch on Windows.
    fn from(metadata: &Metadata) -> Self {
        let time = FileTime::from_last_modification_time(metadata);
        Self {
            seconds: time.unix_seconds(),
            nanoseconds: time.nanoseconds(),
        }
    }

    #[allow(deprecated)] // This sets timestamps on directories on both supported platforms.
    fn apply(self, path: &Path) -> io::Result<()> {
        filetime::set_file_mtime(
            path,
            FileTime::from_unix_time(self.seconds, self.nanoseconds),
        )
    }
}

pub struct Prepared {
    pub manifest: Manifest,
    pub(crate) bytes: u64,
    sources: Vec<Option<Source>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OfferPreview {
    names: Vec<String>,
    pub bytes: u64,
}

impl OfferPreview {
    pub(crate) fn display_names(&self) -> Result<Vec<String>> {
        ensure!(
            (1..=MAX_ENTRIES).contains(&self.names.len()),
            "Invalid item count in transfer offer"
        );
        self.names
            .iter()
            .map(|name| {
                validate_component(name)?;
                Ok(display_name(name))
            })
            .collect()
    }

    pub(crate) fn verify(&self, manifest: &Manifest) -> Result<()> {
        let (_, bytes) = describe(manifest)?;
        ensure!(
            bytes == self.bytes
                && self
                    .names
                    .iter()
                    .eq(manifest.roots.iter().map(|root| &root.name)),
            "The prepared transfer differs from the approved offer"
        );
        Ok(())
    }
}

struct Source {
    path: PathBuf,
    identity: FileIdentity,
    checkpoint_states: Vec<digest::Context>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    changed: i64,
    #[cfg(unix)]
    changed_nanos: i64,
    #[cfg(not(unix))]
    created: Option<std::time::SystemTime>,
}

impl FileIdentity {
    fn from(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
                changed: metadata.ctime(),
                changed_nanos: metadata.ctime_nsec(),
            }
        }
        #[cfg(not(unix))]
        Self {
            created: metadata
                .created()
                .ok()
                .filter(|created| *created >= std::time::SystemTime::UNIX_EPOCH),
        }
    }
}

#[derive(Default)]
struct SourceScan {
    files: Vec<Option<Source>>,
    skipped: Vec<String>,
    file_count: usize,
    bytes: u64,
}

impl Prepared {
    pub(crate) fn preview(&self) -> OfferPreview {
        OfferPreview {
            names: self
                .manifest
                .roots
                .iter()
                .map(|root| root.name.clone())
                .collect(),
            bytes: self.bytes,
        }
    }

    pub(crate) fn enable_streaming(&mut self, paths: &[PathBuf]) -> Result<()> {
        // Scope saved data to the same selection, even when another folder
        // contains items with identical names, sizes and modification times.
        let mut hash = digest::Context::new(&digest::SHA256);
        hash.update(b"usb-direct-transfer streaming source");
        hash.update(&(paths.len() as u64).to_be_bytes());
        for path in paths {
            let path = std::path::absolute(path)?;
            let bytes = path.as_os_str().as_encoded_bytes();
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        for source in self.sources.iter().flatten() {
            hash.update(&serde_json::to_vec(&source.identity)?);
        }
        self.manifest.stream_id = Some(hex_digest(hash.finish()));
        describe(&self.manifest)?;
        Ok(())
    }

    pub(crate) fn omit_checkpoints(&mut self) -> bool {
        if self.manifest.checkpoints.is_empty() {
            return false;
        }
        self.manifest.checkpoints.clear();
        for source in self.sources.iter_mut().flatten() {
            source.checkpoint_states = Vec::new();
        }
        true
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub renamed: Vec<String>,
    pub skipped: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub additional_renamed: usize,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub additional_skipped: usize,
}

fn is_zero(count: &usize) -> bool {
    *count == 0
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, Clone)]
pub struct Pending {
    pub id: String,
    pub bytes: u64,
    pub partial_bytes: u64,
}

impl Manifest {
    pub(crate) fn is_streaming(&self) -> bool {
        self.stream_id.is_some()
    }

    pub(crate) fn verify_prehashed_offer(&self, final_offer: &Self) -> Result<()> {
        describe(final_offer)?;
        ensure!(
            !final_offer.is_streaming()
                && self.skipped == final_offer.skipped
                && self.roots.len() == final_offer.roots.len()
                && self
                    .roots
                    .iter()
                    .zip(&final_offer.roots)
                    .all(|(before, after)| {
                        before.name == after.name
                            && before.entries.len() == after.entries.len()
                            && before
                                .entries
                                .iter()
                                .zip(&after.entries)
                                .all(|(left, right)| {
                                    left.relative == right.relative
                                        && left.modified == right.modified
                                        && match (&left.kind, &right.kind) {
                                            (Kind::Directory, Kind::Directory) => true,
                                            (
                                                Kind::File { bytes: left, .. },
                                                Kind::File { bytes: right, .. },
                                            ) => left == right,
                                            _ => false,
                                        }
                                })
                    }),
            "The prepared transfer differs from the approved offer"
        );
        Ok(())
    }

    fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.roots.iter().flat_map(|root| root.entries.iter())
    }

    fn id(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Identity<'a> {
            roots: &'a [Root],
            skipped: &'a [String],
            #[serde(skip_serializing_if = "Option::is_none")]
            stream_id: Option<&'a str>,
        }
        // The checkpoint plan can vary between peers. Keep the existing
        // staging key so previously verified files are still reusable.
        let encoded = serde_json::to_vec(&Identity {
            roots: &self.roots,
            skipped: &self.skipped,
            stream_id: self.stream_id.as_deref(),
        })?;
        Ok(hex_digest(digest::digest(&digest::SHA256, &encoded)))
    }
}

#[cfg(test)]
pub fn prepare(paths: &[PathBuf], cancelled: &AtomicBool) -> Result<Prepared> {
    prepare_with_preview(paths, cancelled, |_| Ok(()))
}

#[cfg(test)]
pub(crate) fn prepare_with_preview(
    paths: &[PathBuf],
    cancelled: &AtomicBool,
    preview: impl FnOnce(OfferPreview) -> Result<()>,
) -> Result<Prepared> {
    let mut batch = scan_batch(paths, cancelled, |_, _| {})?;
    preview(batch.preview())?;
    batch.finish_hashes(cancelled, |_, _| {})?;
    Ok(batch)
}

pub(crate) fn scan_batch(
    paths: &[PathBuf],
    cancelled: &AtomicBool,
    mut scanned: impl FnMut(usize, u64),
) -> Result<Prepared> {
    let mut manifest = Manifest {
        roots: Vec::new(),
        skipped: Vec::new(),
        checkpoints: BTreeMap::new(),
        stream_id: None,
    };
    let mut sources = SourceScan::default();
    scanned(0, 0);
    for path in paths {
        ensure!(
            !cancelled.load(Ordering::Relaxed),
            "Transfer preparation canceled"
        );
        // stat follows a final directory symlink spelled with a trailing slash.
        let normalized: PathBuf = path.components().collect();
        let canonical;
        let path = if normalized.file_name().is_some() {
            normalized.as_path()
        } else {
            canonical = path.canonicalize()?;
            &canonical
        };
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| {
                format!(
                    "Select a named item with a UTF-8 filename: {}",
                    display_path(path)
                )
            })?;
        let mut root = Root {
            name: name.into(),
            entries: Vec::new(),
        };
        validate_component(name)?;
        scan(
            path,
            Vec::new(),
            &mut root,
            &mut sources,
            cancelled,
            &mut scanned,
        )
        .with_context(|| format!("Cannot read {}", display_path(path)))?;
        if !root.entries.is_empty() {
            manifest.roots.push(root);
        }
    }
    manifest.skipped = sources.skipped;
    ensure!(
        !manifest.roots.is_empty(),
        "No transferable files or folders. Skipped: {}",
        manifest
            .skipped
            .iter()
            .map(|line| display_name(line))
            .collect::<Vec<_>>()
            .join(", ")
    );
    Ok(Prepared {
        manifest,
        bytes: sources.bytes,
        sources: sources.files,
    })
}

impl Prepared {
    pub(crate) fn finish_hashes(
        &mut self,
        cancelled: &AtomicBool,
        mut checked: impl FnMut(u64, u64),
    ) -> Result<()> {
        let total = self.bytes;
        let mut done = 0;
        checked(done, total);
        let mut tick = |count| {
            ensure!(
                !cancelled.load(Ordering::Relaxed),
                "Transfer preparation canceled"
            );
            if count > 0 {
                done += count;
                checked(done, total);
            }
            Ok(())
        };
        self.manifest.stream_id = None;
        let manifest = &mut self.manifest;
        let mut remaining_checkpoints = resume::MAX_CHECKPOINTS;
        let mut buffer = vec![0; CHUNK];
        for (index, (entry, source)) in manifest
            .roots
            .iter_mut()
            .flat_map(|root| root.entries.iter_mut())
            .zip(&mut self.sources)
            .enumerate()
        {
            let (Kind::File { bytes, sha256 }, Some(source)) = (&mut entry.kind, source) else {
                continue;
            };
            let interval = resume::interval(*bytes, remaining_checkpoints);
            let (hash, checkpoints) = hash_source(
                &source.path,
                *bytes,
                entry.modified,
                interval,
                &mut buffer,
                &mut tick,
            )
            .with_context(|| format!("Cannot prepare {}", display_path(&source.path)))?;
            *sha256 = hash;
            if !checkpoints.is_empty() {
                remaining_checkpoints -= checkpoints.len();
                manifest.checkpoints.insert(
                    index,
                    Checkpoints {
                        interval,
                        digests: checkpoints
                            .iter()
                            .map(|state| hex_digest(state.clone().finish()))
                            .collect(),
                    },
                );
                source.checkpoint_states = checkpoints;
            }
        }
        describe(manifest)?;
        Ok(())
    }
}

fn scan(
    path: &Path,
    relative: Vec<String>,
    root: &mut Root,
    sources: &mut SourceScan,
    cancelled: &AtomicBool,
    scanned: &mut impl FnMut(usize, u64),
) -> Result<()> {
    ensure!(
        !cancelled.load(Ordering::Relaxed),
        "Transfer preparation canceled"
    );
    ensure!(relative.len() <= MAX_DEPTH, "Folder is nested too deeply");
    ensure!(
        sources.files.len() + sources.skipped.len() < MAX_ENTRIES,
        "Too many items"
    );
    let metadata = fs::symlink_metadata(path)?;
    let modified = Modified::from(&metadata);
    if is_link(&metadata) {
        sources
            .skipped
            .push(format!("{} (link)", raw_wire_path(&root.name, &relative)));
    } else if metadata.is_dir() {
        if path
            .file_name()
            .is_some_and(|name| name == STAGING_DIRECTORY)
            && verify_marker(path).is_ok()
        {
            sources.skipped.push(format!(
                "{} (transfer staging data)",
                raw_wire_path(&root.name, &relative)
            ));
            return Ok(());
        }
        root.entries.push(Entry {
            relative: relative.clone(),
            kind: Kind::Directory,
            modified,
        });
        sources.files.push(None);
        let available = MAX_ENTRIES - sources.files.len() - sources.skipped.len();
        let mut children = Vec::new();
        for child in fs::read_dir(path)? {
            ensure!(children.len() < available, "Too many items");
            ensure!(
                !cancelled.load(Ordering::Relaxed),
                "Transfer preparation canceled"
            );
            children.push(child?);
        }
        children.sort_by_cached_key(fs::DirEntry::file_name);
        for child in children {
            let name = child.file_name();
            let Some(name) = name.to_str() else {
                ensure!(
                    sources.files.len() + sources.skipped.len() < MAX_ENTRIES,
                    "Too many items"
                );
                sources.skipped.push(format!(
                    "{}/{} (filename is not UTF-8)",
                    raw_wire_path(&root.name, &relative),
                    name.to_string_lossy()
                ));
                continue;
            };
            let mut child_relative = relative.clone();
            child_relative.push(name.to_string());
            scan(
                &child.path(),
                child_relative,
                root,
                sources,
                cancelled,
                scanned,
            )?;
        }
    } else if metadata.is_file() {
        let bytes = metadata.len();
        sources.bytes = sources
            .bytes
            .checked_add(bytes)
            .context("Transfer size overflow")?;
        root.entries.push(Entry {
            relative,
            kind: Kind::File {
                bytes,
                sha256: String::new(),
            },
            modified,
        });
        sources.files.push(Some(Source {
            path: path.to_path_buf(),
            identity: FileIdentity::from(&metadata),
            checkpoint_states: Vec::new(),
        }));
        sources.file_count += 1;
        scanned(sources.file_count, sources.bytes);
    } else {
        sources.skipped.push(format!(
            "{} (special file)",
            raw_wire_path(&root.name, &relative)
        ));
    }
    Ok(())
}

fn hash_source(
    path: &Path,
    bytes: u64,
    modified: Modified,
    interval: u64,
    buffer: &mut [u8],
    mut checked: impl FnMut(u64) -> Result<()>,
) -> Result<(String, Vec<digest::Context>)> {
    checked(0)?;
    let mut file = open_regular(path)?;
    let before = file.metadata()?;
    ensure!(
        before.len() == bytes && Modified::from(&before) == modified,
        "File changed while preparing the transfer"
    );
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut checkpoints = Vec::new();
    let mut remaining = bytes;
    while remaining > 0 {
        let length = remaining.min(interval);
        copy_hashed(
            &mut file,
            &mut io::sink(),
            length,
            buffer,
            &mut hasher,
            &mut checked,
        )?;
        remaining -= length;
        if remaining > 0 {
            checkpoints.push(hasher.clone());
        }
    }
    let after = file.metadata()?;
    ensure!(
        after.len() == bytes && Modified::from(&after) == modified,
        "File changed while preparing the transfer"
    );
    Ok((hex_digest(hasher.finish()), checkpoints))
}

pub fn describe(manifest: &Manifest) -> Result<(Vec<String>, u64)> {
    ensure!(!manifest.roots.is_empty(), "Empty transfer");
    ensure!(
        manifest.entries().count() + manifest.skipped.len() <= MAX_ENTRIES,
        "Too many items"
    );
    if let Some(id) = &manifest.stream_id {
        ensure!(
            is_digest(id) && manifest.checkpoints.is_empty(),
            "Invalid streaming manifest"
        );
    }
    let mut total = 0_u64;
    let mut index = 0;
    let mut checkpoint_budget = resume::MAX_CHECKPOINTS;
    for root in &manifest.roots {
        validate_component(&root.name)?;
        ensure!(
            root.entries
                .first()
                .is_some_and(|entry| entry.relative.is_empty()),
            "Missing root item"
        );
        let mut parents = HashMap::new();
        for entry in &root.entries {
            ensure!(
                entry.relative.len() <= MAX_DEPTH,
                "Path is nested too deeply"
            );
            ensure!(
                entry.modified.nanoseconds < 1_000_000_000,
                "Invalid timestamp"
            );
            if let Some((component, parent)) = entry.relative.split_last() {
                validate_component(component)?;
                ensure!(
                    parents.get(parent) == Some(&true),
                    "Missing parent directory"
                );
            }
            ensure!(
                parents
                    .insert(
                        entry.relative.as_slice(),
                        matches!(entry.kind, Kind::Directory)
                    )
                    .is_none(),
                "Duplicate path in transfer"
            );
            if let Kind::File { bytes, sha256 } = &entry.kind {
                ensure!(
                    if manifest.is_streaming() {
                        sha256.is_empty()
                    } else {
                        is_digest(sha256)
                    },
                    "Invalid file hash"
                );
                total = total
                    .checked_add(*bytes)
                    .context("Transfer size overflow")?;
            }
            if let Some(checkpoints) = manifest.checkpoints.get(&index) {
                let Kind::File { bytes, .. } = &entry.kind else {
                    bail!("Transfer checkpoints refer to a folder");
                };
                checkpoints.validate(*bytes, &mut checkpoint_budget)?;
            }
            index += 1;
        }
    }
    ensure!(
        manifest
            .checkpoints
            .last_key_value()
            .is_none_or(|(&last, _)| last < index),
        "Transfer checkpoints refer to an unknown file"
    );
    Ok((
        manifest
            .roots
            .iter()
            .map(|root| display_name(&root.name))
            .collect(),
        total,
    ))
}

pub(crate) struct ResumeVerification<F> {
    pub parallel: bool,
    pub checked: F,
}

#[cfg(test)]
impl ResumeVerification<fn(u64, u64)> {
    pub(crate) fn silent(parallel: bool) -> Self {
        Self {
            parallel,
            checked: |_, _| {},
        }
    }
}

pub(crate) fn send(
    stream: &mut impl ReadWrite,
    prepared: &Prepared,
    resume: ResumeVerification<impl FnMut(u64, u64) + Send>,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    if prepared.manifest.is_streaming() {
        return streaming::send(stream, prepared, resume, progress);
    }
    let (_, total) = describe(&prepared.manifest)?;
    let entries: Vec<_> = prepared.manifest.entries().collect();
    let first = read_prepared(stream)?;
    let with_checkpoints = first == resume::REQUESTS_WITH_CHECKPOINTS;
    let count = if with_checkpoints {
        ensure!(
            !prepared.manifest.checkpoints.is_empty(),
            "Unexpected checkpoint request"
        );
        read_integer(stream)?
    } else {
        first
    };
    ensure!(count <= entries.len(), "Invalid requested file count");
    let mut requests = Vec::with_capacity(count);
    let mut requested_bytes = 0_u64;
    for _ in 0..count {
        let index = read_integer(stream)?;
        ensure!(
            requests.last().is_none_or(|(last, _, _)| *last < index),
            "Invalid file order"
        );
        let Some(Entry {
            kind: Kind::File { bytes, .. },
            ..
        }) = entries.get(index)
        else {
            bail!("Receiver requested an unknown file");
        };
        let checkpoint = if with_checkpoints {
            read_integer(stream)?
        } else {
            0
        };
        let offset = if checkpoint == 0 {
            0
        } else {
            prepared
                .manifest
                .checkpoints
                .get(&index)
                .context("Receiver requested an unknown checkpoint")?
                .offset(checkpoint)?
        };
        requested_bytes += bytes - offset;
        requests.push((index, checkpoint, offset));
    }
    let mut completed = total - requested_bytes;
    progress(completed, total);
    let mut buffer = vec![0; CHUNK];
    for (index, checkpoint, offset) in requests {
        let entry = entries[index];
        let Kind::File { bytes, sha256 } = &entry.kind else {
            unreachable!("Requested file type was checked");
        };
        let item = prepared.sources[index]
            .as_ref()
            .context("Missing source file")?;
        let path = &item.path;
        let mut source = open_regular(path)?;
        let metadata = source.metadata()?;
        ensure!(
            metadata.len() == *bytes && Modified::from(&metadata) == entry.modified,
            "Source changed since the offer: {}",
            display_path(path)
        );
        let mut hasher = if checkpoint != 0 {
            source.seek(SeekFrom::Start(offset))?;
            item.checkpoint_states
                .get(checkpoint - 1)
                .cloned()
                .context("Missing source checkpoint")?
        } else {
            digest::Context::new(&digest::SHA256)
        };
        copy_payload(
            &mut source,
            stream,
            *bytes - offset,
            &mut buffer,
            &mut hasher,
            |size| {
                completed += size;
                progress(completed, total);
                Ok(())
            },
        )?;
        ensure!(
            hex_digest(hasher.finish()) == *sha256,
            "Source changed while sending {}",
            display_path(path)
        );
    }
    stream.flush()?;
    ensure!(
        read_prepared(stream)? == PUBLISHED,
        "Missing publication confirmation"
    );
    Ok(())
}

struct FileRequest<'a> {
    index: usize,
    path: PathBuf,
    bytes: u64,
    sha256: &'a str,
    prefix: Option<resume::VerifiedPrefix>,
    offset: u64,
}

pub(crate) struct Destination {
    directory: PathBuf,
    base: PathBuf,
    // Hold the staging lock through publication and receipt confirmation.
    _lock: File,
}

impl Destination {
    pub(crate) fn open(to: &Path) -> Result<Self> {
        let (directory, base) = staging_base(to, true)
            .with_context(|| format!("Cannot prepare {} for receiving", display_path(to)))?;
        let lock = lock_staging(&base)?;
        Ok(Self {
            directory,
            base,
            _lock: lock,
        })
    }

    pub(crate) fn has_prehashed_progress(&self, manifest: &Manifest) -> Result<bool> {
        if !manifest.is_streaming() || directory_exists(&self.base.join(manifest.id()?))? {
            return Ok(false);
        }
        // Older journals did not record the source selection. Match the saved
        // layout and sizes before asking for hashes. Only the content-based
        // staging key can ultimately select and reuse an older journal.
        for candidate in fs::read_dir(&self.base)? {
            let candidate = candidate?;
            if !candidate.file_type()?.is_dir()
                || !candidate.file_name().to_str().is_some_and(is_digest)
            {
                continue;
            }
            let stage = candidate.path();
            let Ok(state) = read_state(&stage) else {
                continue;
            };
            if !state.streaming
                && validate_state(&state, manifest).is_ok()
                && legacy_progress_matches(&self.directory, &stage, &state, manifest)
                    .unwrap_or(false)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

pub(crate) fn receive(
    stream: &mut impl ReadWrite,
    manifest: &Manifest,
    to: &Destination,
    cancelled: &AtomicBool,
    resume: ResumeVerification<impl FnMut(u64, u64)>,
    mut progress: impl FnMut(u64, u64),
) -> Result<Summary> {
    let (_, total) = describe(manifest)?;
    let Destination {
        directory, base, ..
    } = to;
    let stage = base.join(manifest.id()?);
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    let mut tick = || preparing_pulse(stream, &mut next_pulse, cancelled);
    tick()?;
    let existing_stage = directory_exists(&stage)?;
    let mut state = if existing_stage {
        read_state(&stage)?
    } else {
        initialize_stage(base, &stage, manifest, &mut tick)?
    };
    validate_state(&state, manifest)?;
    if existing_stage {
        remove_temporary_entries(&stage, ".partial-")?;
        remove_temporary_entries(&stage, ".state-")?;
    }
    let mut summary = Summary {
        skipped: manifest
            .skipped
            .iter()
            .map(|line| display_name(line))
            .collect(),
        ..Summary::default()
    };
    let mut staged_roots = Vec::new();
    let mut files = Vec::new();
    let mut next_index = 0;
    for (index, root) in manifest.roots.iter().enumerate() {
        let staged_root = stage_item(&stage, index);
        let is_staged = path_exists(&staged_root)?;
        let saved_root = if is_staged {
            staged_roots.push(index);
            staged_root
        } else {
            let saved = state.published[index].as_deref().with_context(|| {
                format!("Staging data for {} is missing", display_name(&root.name))
            })?;
            directory.join(saved)
        };
        let mut directories: HashMap<&[String], PathBuf> = HashMap::new();
        for (entry, mapped) in root.entries.iter().zip(&state.paths[index]) {
            let file_index = next_index;
            next_index += 1;
            tick()?;
            let saved_path = match mapped.split_last() {
                Some((name, parent)) => directories
                    .get(parent)
                    .context("Missing staged parent directory")?
                    .join(name),
                None => saved_root.clone(),
            };
            if let Kind::File { bytes, sha256 } = &entry.kind {
                files.push(StagedFile {
                    index: file_index,
                    path: saved_path,
                    bytes: *bytes,
                    sha256,
                    is_staged,
                });
            } else {
                require_directory(&saved_path)?;
                directories.insert(mapped, saved_path);
            }
        }
    }
    if manifest.is_streaming() {
        streaming::receive(
            stream,
            &stage,
            files,
            total,
            cancelled,
            existing_stage.then_some(resume),
            &mut progress,
        )?;
    } else {
        receive_prehashed_files(
            stream,
            &stage,
            manifest,
            files,
            total,
            cancelled,
            &mut progress,
        )?;
    }

    let mut tick = || preparing_pulse(stream, &mut next_pulse, cancelled);
    tick()?;
    let mut reserved = HashSet::new();
    let mut choices_by_name = HashMap::new();
    for &index in &staged_roots {
        let root = &manifest.roots[index];
        let staged_root = stage_item(&stage, index);
        for (entry, mapped) in root.entries.iter().zip(&state.paths[index]).rev() {
            tick()?;
            let path = checked_path(&staged_root, mapped)?;
            if entry.modified.apply(&path).is_err() {
                summary.skipped.push(format!(
                    "{} (modification time could not be preserved)",
                    wire_path(&root.name, &entry.relative)
                ));
            }
        }
        let folder = matches!(root.entries[0].kind, Kind::Directory);
        let choices = choices_by_name
            .entry((compatible_name(&root.name), folder))
            .or_insert_with_key(|(name, folder)| publication_names(name.clone(), *folder));
        let name = unclaimed_name(
            directory,
            root,
            choices,
            |candidate| reserved.contains(candidate),
            &mut tick,
        )?;
        reserved.insert(name.clone());
        state.published[index] = Some(name);
    }
    if staged_roots.len() < manifest.roots.len() {
        // A previous process may have stopped between a rename and its sync.
        sync_publication(directory, &stage)?;
    }
    if !staged_roots.is_empty() {
        // The journal records every destination before any staged item moves.
        // The source's presence distinguishes planned items from published ones.
        write_state(&stage, &state)?;
    }

    for group in staged_roots.chunks(PUBLICATION_BATCH_SIZE) {
        publish_group(
            directory,
            &stage,
            &mut state,
            &manifest.roots,
            group,
            &mut tick,
        )?;
    }

    for (index, root) in manifest.roots.iter().enumerate() {
        tick()?;
        let saved = state.published[index]
            .as_deref()
            .context("Missing saved name")?;
        for (entry, mapped) in root.entries.iter().zip(&state.paths[index]) {
            let original_name = entry
                .relative
                .last()
                .map_or(root.name.as_str(), String::as_str);
            let saved_name = mapped.last().map_or(saved, String::as_str);
            if original_name != saved_name {
                let original = wire_path(&root.name, &entry.relative);
                let published = wire_path(saved, mapped);
                summary.renamed.push(format!("{original} -> {published}"));
            }
        }
    }
    tick()?;
    write_integer(stream, PUBLISHED)?;
    stream.flush()?;
    // Keep the publication journal until the sender confirms receipt. A dropped
    // receipt can then be retried without creating a second copy of any item.
    Ok(summary)
}

struct StagedFile<'a> {
    index: usize,
    path: PathBuf,
    bytes: u64,
    sha256: &'a str,
    is_staged: bool,
}

fn receive_prehashed_files(
    stream: &mut impl ReadWrite,
    stage: &Path,
    manifest: &Manifest,
    files: Vec<StagedFile<'_>>,
    total: u64,
    cancelled: &AtomicBool,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let mut requests = Vec::new();
    let mut remaining = 0;
    let mut buffer = vec![0; CHUNK];
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    let mut tick = || preparing_pulse(stream, &mut next_pulse, cancelled);
    for StagedFile {
        index,
        path,
        bytes,
        sha256,
        is_staged,
    } in files
    {
        tick()?;
        if matches_file(&path, bytes, sha256, &mut buffer, &mut tick)? {
            continue;
        }
        ensure!(
            is_staged,
            "Previously published data changed: {}",
            display_path(&path)
        );
        let part = resume::path(stage, index);
        if bytes <= staging_batch::MAX_SMALL_FILE
            && staging_batch::recover(&part, &path, bytes, sha256, &mut buffer, &mut tick)?
        {
            continue;
        }
        let (prefix, offset) = if let Some(checkpoints) = manifest.checkpoints.get(&index) {
            let prefix = checkpoints.verify_saved(&part, &mut buffer, &mut tick)?;
            let offset = prefix
                .as_ref()
                .map_or(Ok(0), |prefix| checkpoints.offset(prefix.checkpoint))?;
            (prefix, offset)
        } else {
            (None, 0)
        };
        requests.push(FileRequest {
            index,
            path,
            bytes,
            sha256,
            prefix,
            offset,
        });
        remaining += bytes - offset;
    }

    {
        let mut writer = io::BufWriter::new(&mut *stream);
        // Older peers never advertise checkpoints, so they receive the
        // original index-only request format.
        let resuming = requests.iter().any(|request| request.prefix.is_some());
        if resuming {
            write_integer(&mut writer, resume::REQUESTS_WITH_CHECKPOINTS)?;
        }
        write_integer(&mut writer, requests.len())?;
        for request in &requests {
            write_integer(&mut writer, request.index)?;
            if resuming {
                let checkpoint = request
                    .prefix
                    .as_ref()
                    .map_or(0, |prefix| prefix.checkpoint);
                write_integer(&mut writer, checkpoint)?;
            }
        }
        writer.flush()?;
    }
    let mut completed = total - remaining;
    progress(completed, total);
    let mut small_files = staging_batch::Batch::default();
    let received = (|| -> Result<()> {
        for request in requests {
            ensure!(!cancelled.load(Ordering::Relaxed), "Download canceled");
            let keep_partial = manifest.checkpoints.contains_key(&request.index);
            receive_file(
                stream,
                stage,
                request,
                keep_partial,
                &mut buffer,
                &mut small_files,
                |size| {
                    completed += size;
                    progress(completed, total);
                    Ok(())
                },
            )?;
        }
        Ok(())
    })();
    // Preserve the verified members of the final group on interruption too.
    received.and(small_files.flush(stage))
}

fn receive_file(
    stream: &mut impl Read,
    stage: &Path,
    request: FileRequest<'_>,
    keep_partial: bool,
    buffer: &mut [u8],
    small_files: &mut staging_batch::Batch,
    copied: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    let FileRequest {
        index,
        path,
        bytes,
        sha256,
        prefix,
        offset,
    } = request;
    let grouped = !keep_partial && bytes <= staging_batch::MAX_SMALL_FILE;
    if !grouped {
        small_files.flush(stage)?;
    }
    let verify = |actual: &str| -> Result<()> {
        ensure!(
            actual == sha256,
            "Checksum mismatch for {}",
            display_path(&path)
        );
        Ok(())
    };
    if keep_partial {
        let part = resume::path(stage, index);
        let mut file = resume::open(&part)?;
        let mut hasher = match prefix {
            Some(prefix) => {
                prefix.require_unchanged(&file.metadata()?)?;
                prefix.hasher
            }
            None => digest::Context::new(&digest::SHA256),
        };
        file.set_len(offset)?;
        file.seek(SeekFrom::Start(offset))?;
        copy_payload(
            stream,
            &mut file,
            bytes - offset,
            buffer,
            &mut hasher,
            copied,
        )?;
        verify(&hex_digest(hasher.finish()))?;
        file.sync_all()?;
        drop(file);
        fs::rename(part, &path)?;
    } else {
        let mut part = tempfile::Builder::new()
            .prefix(".partial-")
            .tempfile_in(stage)?;
        let mut hasher = digest::Context::new(&digest::SHA256);
        copy_payload(
            stream,
            part.as_file_mut(),
            bytes,
            buffer,
            &mut hasher,
            copied,
        )?;
        verify(&hex_digest(hasher.finish()))?;
        if grouped {
            return small_files.add(stage, index, path, part);
        }
        part.as_file().sync_all()?;
        part.persist(&path).map_err(|error| error.error)?;
    }
    sync_directory(path.parent().context("Missing staging parent")?)
}

pub(crate) fn confirm_receipt(to: &Destination, manifest: &Manifest) -> Result<()> {
    let base = &to.base;
    verify_marker(base)?;
    let stage = base.join(manifest.id()?);
    if !directory_exists(&stage)? {
        return Ok(());
    }
    let state = read_state(&stage)?;
    validate_state(&state, manifest)?;
    for (index, destination) in state.published.iter().enumerate() {
        ensure!(
            destination.is_some() && !path_exists(&stage_item(&stage, index))?,
            "Cannot clear the publication journal for an unfinished transfer"
        );
    }
    fs::remove_dir_all(&stage)?;
    sync_directory(base)
}

pub fn pending(to: &Path) -> Result<Vec<Pending>> {
    let (_, base) = staging_base(to, false)?;
    if !directory_exists(&base)? {
        return Ok(Vec::new());
    }
    verify_marker(&base)?;
    let mut items = Vec::new();
    for entry in fs::read_dir(base)? {
        let entry = entry?;
        let id = entry.file_name().to_string_lossy().into_owned();
        let path = entry.path();
        if is_digest(&id) && metadata_if_present(&path)?.is_some_and(|meta| is_directory(&meta)) {
            let (bytes, partial_bytes) = staged_bytes(&path, true)?;
            items.push(Pending {
                id,
                bytes,
                partial_bytes,
            });
        }
    }
    items.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(items)
}

pub fn discard(to: &Path, id: &str) -> Result<()> {
    ensure!(is_digest(id), "Expected a full pending transfer ID");
    let (_, base) = staging_base(to, false)?;
    verify_marker(&base)?;
    let _lock = lock_staging(&base)?;
    let stage = base.join(id);
    require_directory(&stage)?;
    read_state(&stage)?;
    fs::remove_dir_all(stage)?;
    sync_directory(&base)
}

#[derive(Serialize, Deserialize)]
struct State {
    version: u32,
    paths: Vec<Vec<Vec<String>>>,
    published: Vec<Option<String>>,
    #[serde(default, skip_serializing_if = "is_false")]
    streaming: bool,
}

fn initialize_stage(
    base: &Path,
    stage: &Path,
    manifest: &Manifest,
    mut tick: impl FnMut() -> Result<()>,
) -> Result<State> {
    remove_temporary_entries(base, ".initial-")?;
    let temp = tempfile::Builder::new()
        .prefix(".initial-")
        .tempdir_in(base)?;
    let mut state = State {
        version: STATE_VERSION,
        paths: Vec::new(),
        published: vec![None; manifest.roots.len()],
        streaming: manifest.is_streaming(),
    };
    for (index, root) in manifest.roots.iter().enumerate() {
        let item = stage_item(temp.path(), index);
        let mut paths = Vec::new();
        let mut mapped_paths = HashMap::new();
        for entry in &root.entries {
            tick()?;
            let folder = matches!(entry.kind, Kind::Directory);
            let mapped = if let Some((component, parent)) = entry.relative.split_last() {
                let parent: &Vec<String> = &mapped_paths[parent];
                let parent_path = checked_path(&item, parent)?;
                let name = reserve_name(&parent_path, component, folder)?;
                let mut relative = parent.clone();
                relative.push(name);
                relative
            } else {
                create_item(&item, folder)?;
                Vec::new()
            };
            mapped_paths.insert(entry.relative.as_slice(), mapped.clone());
            paths.push(mapped);
        }
        state.paths.push(paths);
    }
    write_state(temp.path(), &state)?;
    rename_noreplace(temp.path(), stage)?;
    sync_directory(base)?;
    Ok(state)
}

fn validate_state(state: &State, manifest: &Manifest) -> Result<()> {
    ensure!(state.version == STATE_VERSION, "Unsupported staging format");
    ensure!(
        state.paths.len() == manifest.roots.len() && state.published.len() == manifest.roots.len(),
        "Corrupted staging state"
    );
    for (paths, root) in state.paths.iter().zip(&manifest.roots) {
        ensure!(
            paths.len() == root.entries.len(),
            "Corrupted staged path count"
        );
        let mut parents = HashMap::new();
        let mut unique_paths = HashSet::new();
        for (entry, mapped) in root.entries.iter().zip(paths) {
            ensure!(
                mapped.len() == entry.relative.len(),
                "Corrupted staged path"
            );
            if let (Some((_, source_parent)), Some((name, saved_parent))) =
                (entry.relative.split_last(), mapped.split_last())
            {
                validate_local_component(name)?;
                ensure!(
                    parents.get(source_parent) == Some(&saved_parent),
                    "Corrupted staged parent"
                );
            }
            ensure!(
                unique_paths.insert(mapped.as_slice()),
                "Duplicate staged file"
            );
            parents.insert(entry.relative.as_slice(), mapped.as_slice());
        }
    }
    for saved in state.published.iter().flatten() {
        validate_local_component(saved)?;
        ensure!(saved != STAGING_DIRECTORY, "Invalid saved destination");
    }
    Ok(())
}

fn legacy_progress_matches(
    destination: &Path,
    stage: &Path,
    state: &State,
    manifest: &Manifest,
) -> Result<bool> {
    let mut file_index = 0;
    let mut has_progress = false;
    for (index, root) in manifest.roots.iter().enumerate() {
        let mut location = stage_item(stage, index);
        if let Some(saved) = &state.published[index] {
            if !saved_name_matches(&root.name, saved, &root.entries[0].kind) {
                return Ok(false);
            }
            if !path_exists(&location)? {
                location = destination.join(saved);
            }
        }
        for (entry, mapped) in root.entries.iter().zip(&state.paths[index]) {
            let index = file_index;
            file_index += 1;
            if let Some((original, actual)) = entry.relative.last().zip(mapped.last())
                && !saved_name_matches(original, actual, &entry.kind)
            {
                return Ok(false);
            }
            if let Kind::File { bytes, .. } = &entry.kind {
                let path = match checked_path(&location, mapped) {
                    Ok(path) => path,
                    Err(_) => return Ok(false),
                };
                for (path, partial) in [(path, false), (resume::path(stage, index), true)] {
                    if let Some(metadata) = metadata_if_present(&path)?
                        && is_regular(&metadata)
                        && metadata.len() > 0
                    {
                        let size_matches = if partial {
                            metadata.len() <= *bytes
                        } else {
                            metadata.len() == *bytes
                        };
                        if !size_matches {
                            return Ok(false);
                        }
                        has_progress = true;
                    }
                }
            }
        }
    }
    Ok(has_progress)
}

fn saved_name_matches(original: &str, saved: &str, kind: &Kind) -> bool {
    let original = compatible_name(original);
    let folder = matches!(kind, Kind::Directory);
    saved == numbered_name(&original, folder, 1)
        || saved.match_indices(" (").any(|(offset, _)| {
            saved[offset + 2..]
                .split_once(')')
                .and_then(|(copy, _)| copy.parse::<usize>().ok())
                .is_some_and(|copy| {
                    (2..=MAX_ENTRIES).contains(&copy)
                        && saved == numbered_name(&original, folder, copy)
                })
        })
}

fn publication_names(name: String, folder: bool) -> impl Iterator<Item = String> {
    (1..=MAX_ENTRIES).map(move |copy| numbered_name(&name, folder, copy))
}

fn unclaimed_name(
    directory: &Path,
    root: &Root,
    choices: impl Iterator<Item = String>,
    reserved: impl Fn(&str) -> bool,
    mut tick: impl FnMut() -> Result<()>,
) -> Result<String> {
    for name in choices {
        tick()?;
        if !reserved(&name) && !path_exists(&directory.join(&name))? {
            return Ok(name);
        }
    }
    bail!("Too many items share the name {}", display_name(&root.name))
}

fn publish_group(
    directory: &Path,
    stage: &Path,
    state: &mut State,
    roots: &[Root],
    indices: &[usize],
    mut tick: impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut moved = false;
    let published = (|| -> Result<()> {
        for &index in indices {
            let root = &roots[index];
            let source = stage_item(stage, index);
            let mut choices = None;
            loop {
                tick()?;
                let destination = directory.join(
                    state.published[index]
                        .as_deref()
                        .context("Missing destination in the publication journal")?,
                );
                match rename_noreplace(&source, &destination) {
                    Ok(()) => {
                        moved = true;
                        break;
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        // The journal write also syncs the staging directory.
                        // Flush earlier destinations before writing a new name.
                        if moved {
                            sync_publication(directory, stage)?;
                            moved = false;
                        }
                        let choices = choices.get_or_insert_with(|| {
                            publication_names(
                                compatible_name(&root.name),
                                matches!(root.entries[0].kind, Kind::Directory),
                            )
                        });
                        let name = unclaimed_name(directory, root, choices, |_| false, &mut tick)?;
                        state.published[index] = Some(name);
                        write_state(stage, state)?;
                    }
                    Err(error) => {
                        return Err(error).context("Cannot safely publish the received item");
                    }
                }
            }
        }
        Ok(())
    })();
    // Finish syncing the names that moved if cancellation or an error stopped
    // the rest of the group. The journal still covers the entire batch.
    let synced = if moved {
        sync_publication(directory, stage)
    } else {
        Ok(())
    };
    published.and(synced)
}

fn sync_publication(directory: &Path, stage: &Path) -> Result<()> {
    sync_directory(directory)?;
    sync_directory(stage)
}

fn stage_item(stage: &Path, index: usize) -> PathBuf {
    stage.join(index.to_string())
}

fn write_state(stage: &Path, state: &State) -> Result<()> {
    let mut temp = tempfile::Builder::new()
        .prefix(".state-")
        .tempfile_in(stage)?;
    {
        let mut writer = io::BufWriter::new(temp.as_file_mut());
        serde_json::to_writer(&mut writer, state)?;
        writer.flush()?;
    }
    temp.as_file().sync_all()?;
    temp.persist(stage.join("state.json"))
        .map_err(|error| error.error)?;
    sync_directory(stage)
}

fn read_state(stage: &Path) -> Result<State> {
    let file = open_regular(&stage.join("state.json"))?;
    Ok(serde_json::from_reader(io::BufReader::new(file))?)
}

fn staging_base(to: &Path, create: bool) -> Result<(PathBuf, PathBuf)> {
    let directory = to.canonicalize().with_context(|| {
        format!(
            "Choose an existing destination folder: {}",
            display_path(to)
        )
    })?;
    require_directory(&directory)?;
    let base = directory.join(STAGING_DIRECTORY);
    if create && !directory_exists(&base)? {
        let temp = tempfile::Builder::new()
            .prefix(".usb-direct-initial-")
            .tempdir_in(&directory)?;
        let mut marker = File::create_new(temp.path().join("format"))?;
        marker.write_all(STAGING_MARKER)?;
        marker.sync_all()?;
        drop(marker);
        sync_directory(temp.path())?;
        match rename_noreplace(temp.path(), &base) {
            Ok(()) => sync_directory(&directory)?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (),
            Err(error) => {
                return Err(error).context("Cannot create the destination staging folder");
            }
        }
    }
    if create {
        verify_marker(&base)?;
    }
    Ok((directory, base))
}

fn verify_marker(base: &Path) -> Result<()> {
    require_directory(base)?;
    let marker = open_regular(&base.join("format"))
        .with_context(|| format!("Unrecognized staging folder {}", display_path(base)))?;
    let mut contents = Vec::new();
    marker
        .take(STAGING_MARKER.len() as u64 + 1)
        .read_to_end(&mut contents)?;
    ensure!(
        contents == STAGING_MARKER,
        "Unrecognized staging folder {}",
        display_path(base)
    );
    Ok(())
}

fn lock_staging(base: &Path) -> Result<File> {
    require_directory(base)?;
    let path = base.join("lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    no_follow(&mut options);
    let lock = options.open(&path)?;
    ensure!(is_regular(&lock.metadata()?), "Invalid staging lock");
    fs2::FileExt::try_lock_exclusive(&lock)
        .context("This destination is busy with another transfer")?;
    Ok(lock)
}

fn remove_temporary_entries(directory: &Path, prefix: &str) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_name().to_string_lossy().starts_with(prefix) {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if is_directory(&metadata) {
            fs::remove_dir_all(entry.path())?;
        } else if is_regular(&metadata) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn staged_bytes(path: &Path, stage_root: bool) -> Result<(u64, u64)> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((0, 0)),
        Err(error) => return Err(error.into()),
    };
    let mut size = 0_u64;
    let mut partial_size = 0_u64;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let partial = stage_root && resume::is_partial_file(&name);
        // Numbered roots contain verified files. Keep the partial files'
        // size separate from those and ignore abandoned temporary files.
        if stage_root && !partial && !name.as_encoded_bytes().iter().all(u8::is_ascii_digit) {
            continue;
        }
        let path = entry.path();
        let Some(metadata) = metadata_if_present(&path)? else {
            continue;
        };
        if partial {
            if is_regular(&metadata) {
                partial_size = partial_size.saturating_add(metadata.len());
            }
        } else if is_directory(&metadata) {
            size = size.saturating_add(staged_bytes(&path, false)?.0);
        } else if is_regular(&metadata) {
            size = size.saturating_add(metadata.len());
        }
    }
    Ok((size, partial_size))
}

fn checked_path(root: &Path, mapped: &[String]) -> Result<PathBuf> {
    let mut path = root.to_path_buf();
    for component in mapped {
        require_directory(&path)?;
        validate_local_component(component)?;
        path.push(component);
    }
    Ok(path)
}

fn reserve_name(parent: &Path, original: &str, folder: bool) -> Result<String> {
    let base = compatible_name(original);
    for copy in 1..=MAX_ENTRIES {
        let name = numbered_name(&base, folder, copy);
        let candidate = parent.join(&name);
        match create_item(&candidate, folder) {
            Ok(()) => return Ok(name),
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    || path_exists(&candidate).unwrap_or(false) =>
            {
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Cannot stage {}", display_path(&candidate)));
            }
        }
    }
    bail!("Too many items share the name {}", display_name(original))
}

fn create_item(path: &Path, folder: bool) -> io::Result<()> {
    if folder {
        fs::create_dir(path)
    } else {
        File::create_new(path).map(drop)
    }
}

fn wire_path(root: &str, relative: &[String]) -> String {
    display_name(&raw_wire_path(root, relative))
}

fn display_path(path: &Path) -> String {
    display_name(&path.to_string_lossy())
}

pub(crate) fn display_name(value: &str) -> String {
    use std::fmt::Write as _;
    let mut result = String::new();
    for ch in value.chars() {
        if ch.is_control() || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
            let _ = write!(result, "{}", ch.escape_default());
        } else {
            result.push(ch);
        }
    }
    result
}

fn raw_wire_path(root: &str, relative: &[String]) -> String {
    std::iter::once(root)
        .chain(relative.iter().map(String::as_str))
        .collect::<Vec<_>>()
        .join("/")
}

fn validate_component(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 1024
            && name != "."
            && name != ".."
            && !name.contains(['/', '\0']),
        "Invalid path component"
    );
    Ok(())
}

fn validate_local_component(name: &str) -> Result<()> {
    validate_component(name)?;
    let mut components = Path::new(name).components();
    ensure!(
        matches!(components.next(), Some(Component::Normal(_)))
            && components.next().is_none()
            && compatible_name(name) == name,
        "Invalid local filename"
    );
    Ok(())
}

fn compatible_name(name: &str) -> String {
    #[cfg(windows)]
    {
        let name: String = name
            .chars()
            .map(|ch| {
                if ch.is_control() || "<>:\"/\\|?*".contains(ch) {
                    '_'
                } else {
                    ch
                }
            })
            .collect();
        let name = name.trim_matches(' ').trim_end_matches(['.', ' ']);
        let Some(stem) = name.split('.').next() else {
            return "_".into();
        };
        let upper = stem.trim_end_matches(' ').to_ascii_uppercase();
        let numbered_device = upper
            .strip_prefix("COM")
            .or_else(|| upper.strip_prefix("LPT"))
            .is_some_and(|number| {
                ["1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³"].contains(&number)
            });
        if numbered_device
            || ["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"].contains(&upper.as_str())
        {
            return format!("_{name}");
        }
        if name.is_empty() {
            return "_".into();
        }
        name.to_string()
    }
    #[cfg(not(windows))]
    name.to_string()
}

fn numbered_name(name: &str, folder: bool, copy: usize) -> String {
    let (stem, extension) = if folder {
        (name, "")
    } else {
        match name.rsplit_once('.') {
            Some((stem, extension)) if !stem.is_empty() && !extension.is_empty() => {
                (stem, &name[name.len() - extension.len() - 1..])
            }
            _ => (name, ""),
        }
    };
    let suffix = if copy == 1 {
        String::new()
    } else {
        format!(" ({copy})")
    };
    if measured_name_length(name) + suffix.len() <= 255 {
        return format!("{stem}{suffix}{extension}");
    }
    let extension = truncate_component(extension, 255 - suffix.len() - 1);
    let available = 255 - suffix.len() - measured_name_length(extension);
    let stem = truncate_component(stem, available);
    let shortened = format!("{stem}{suffix}{extension}");
    #[cfg(windows)]
    return compatible_name(&shortened);
    #[cfg(not(windows))]
    shortened
}

fn measured_name_length(name: &str) -> usize {
    if cfg!(windows) {
        name.encode_utf16().count()
    } else {
        name.len()
    }
}

fn truncate_component(name: &str, max: usize) -> &str {
    let mut end = name.len();
    while measured_name_length(&name[..end]) > max {
        end -= name[..end]
            .chars()
            .next_back()
            .expect("nonempty name")
            .len_utf8();
    }
    &name[..end]
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hex_digest(digest: digest::Digest) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = digest.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

fn copy_exact(
    source: &mut impl Read,
    destination: &mut impl Write,
    bytes: u64,
    buffer: &mut [u8],
    copied: impl FnMut(u64) -> Result<()>,
) -> Result<String> {
    let mut hasher = digest::Context::new(&digest::SHA256);
    copy_hashed(source, destination, bytes, buffer, &mut hasher, copied)?;
    Ok(hex_digest(hasher.finish()))
}

fn copy_hashed(
    source: &mut impl Read,
    destination: &mut impl Write,
    mut bytes: u64,
    buffer: &mut [u8],
    hasher: &mut digest::Context,
    mut copied: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    while bytes > 0 {
        let count = bytes.min(buffer.len() as u64) as usize;
        let chunk = &mut buffer[..count];
        source.read_exact(chunk)?;
        hasher.update(chunk);
        destination.write_all(chunk)?;
        bytes -= count as u64;
        copied(count as u64)?;
    }
    Ok(())
}

fn copy_payload(
    source: &mut impl Read,
    destination: &mut impl Write,
    bytes: u64,
    buffer: &mut [u8],
    hasher: &mut digest::Context,
    copied: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    copy_and_hash_payload(
        bytes,
        buffer,
        hasher,
        |chunk| {
            source.read_exact(chunk)?;
            destination.write_all(chunk)
        },
        copied,
    )
}

fn copy_and_hash_payload(
    mut bytes: u64,
    buffer: &mut [u8],
    hasher: &mut digest::Context,
    mut transfer_chunk: impl FnMut(&mut [u8]) -> io::Result<()>,
    mut copied: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    use std::{sync::mpsc, thread};

    if bytes < PARALLEL_CHECKSUM_MIN_BYTES
        || !thread::available_parallelism().is_ok_and(|cores| cores.get() > 1)
    {
        while bytes > 0 {
            let count = bytes.min(buffer.len() as u64) as usize;
            let chunk = &mut buffer[..count];
            transfer_chunk(chunk)?;
            hasher.update(chunk);
            bytes -= count as u64;
            copied(count as u64)?;
        }
        return Ok(());
    }

    // Hash each filled buffer in order while the transfer uses the other one.
    let mut spare = vec![0; buffer.len()];
    thread::scope(|scope| {
        let (recycle, available) = mpsc::sync_channel(2);
        let (submit, submitted) = mpsc::sync_channel::<(&mut [u8], usize)>(1);
        recycle.send(buffer).expect("both checksum buffers fit");
        recycle
            .send(spare.as_mut_slice())
            .expect("both checksum buffers fit");
        let worker = thread::Builder::new().spawn_scoped(scope, move || {
            while let Ok((buffer, count)) = submitted.recv() {
                hasher.update(&buffer[..count]);
                if recycle.send(buffer).is_err() {
                    break;
                }
            }
        })?;

        let result = (|| -> Result<()> {
            while bytes > 0 {
                let buffer = available.recv().context("Checksum worker stopped")?;
                let count = bytes.min(buffer.len() as u64) as usize;
                transfer_chunk(&mut buffer[..count])?;
                submit
                    .send((buffer, count))
                    .map_err(|_| anyhow::anyhow!("Checksum worker stopped"))?;
                bytes -= count as u64;
                copied(count as u64)?;
            }
            Ok(())
        })();

        // Keep the recycle queue alive while the worker finishes submitted data.
        drop(submit);
        worker
            .join()
            .map_err(|_| anyhow::anyhow!("Checksum worker stopped"))?;
        result
    })
}

fn matches_file(
    path: &Path,
    bytes: u64,
    sha256: &str,
    buffer: &mut [u8],
    mut tick: impl FnMut() -> Result<()>,
) -> Result<bool> {
    if !path_exists(path)? {
        return Ok(false);
    }
    let mut file = open_regular(path)?;
    if file.metadata()?.len() != bytes {
        return Ok(false);
    }
    let actual_digest = copy_exact(&mut file, &mut io::sink(), bytes, buffer, |_| tick())?;
    Ok(actual_digest == sha256 && file.metadata()?.len() == bytes)
}

fn preparing_pulse(
    stream: &mut impl Write,
    next_pulse: &mut Instant,
    cancelled: &AtomicBool,
) -> Result<()> {
    ensure!(!cancelled.load(Ordering::Relaxed), "Download canceled");
    preparation_pulse(stream, next_pulse)
}

fn preparation_pulse(stream: &mut impl Write, next_pulse: &mut Instant) -> Result<()> {
    if Instant::now() >= *next_pulse {
        write_integer(stream, PREPARING)?;
        stream.flush()?;
        *next_pulse = Instant::now() + Duration::from_secs(1);
    }
    Ok(())
}

fn read_prepared(stream: &mut impl Read) -> Result<usize> {
    loop {
        let value = read_integer(stream)?;
        if value != PREPARING {
            return Ok(value);
        }
    }
}

fn read_integer(stream: &mut impl Read) -> Result<usize> {
    let mut number = [0; 4];
    stream.read_exact(&mut number)?;
    Ok(u32::from_be_bytes(number) as usize)
}

fn write_integer(stream: &mut impl Write, number: usize) -> Result<()> {
    stream.write_all(&u32::try_from(number)?.to_be_bytes())?;
    Ok(())
}

fn path_exists(path: &Path) -> io::Result<bool> {
    Ok(metadata_if_present(path)?.is_some())
}

fn metadata_if_present(path: &Path) -> io::Result<Option<Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn directory_exists(path: &Path) -> Result<bool> {
    if path_exists(path)? {
        require_directory(path)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn require_directory(path: &Path) -> Result<()> {
    ensure!(
        is_directory(&fs::symlink_metadata(path).with_context(|| display_path(path).to_string())?),
        "Expected a regular directory at {}",
        display_path(path)
    );
    Ok(())
}

fn open_regular(path: &Path) -> Result<File> {
    ensure!(
        is_regular(&fs::symlink_metadata(path)?),
        "Expected a regular file at {}",
        display_path(path)
    );
    let mut options = OpenOptions::new();
    options.read(true);
    no_follow(&mut options);
    let file = options.open(path)?;
    ensure!(
        is_regular(&file.metadata()?),
        "Expected a regular file at {}",
        display_path(path)
    );
    Ok(file)
}

fn is_regular(metadata: &Metadata) -> bool {
    metadata.is_file() && !is_link(metadata)
}

fn is_directory(metadata: &Metadata) -> bool {
    metadata.is_dir() && !is_link(metadata)
}

fn is_link(metadata: &Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

fn no_follow(options: &mut OpenOptions) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let directory = File::open(path)?;
        directory
            .sync_all()
            .with_context(|| format!("Cannot flush {}", display_path(path)))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let from = CString::new(from.as_os_str().as_bytes())?;
    let to = CString::new(to.as_os_str().as_bytes())?;
    #[cfg(target_os = "macos")]
    let status = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    #[cfg(target_os = "linux")]
    let status = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            from.as_ptr(),
            libc::AT_FDCWD,
            to.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if status == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{MOVEFILE_WRITE_THROUGH, MoveFileExW};

    let from: Vec<_> = from.as_os_str().encode_wide().chain([0]).collect();
    let to: Vec<_> = to.as_os_str().encode_wide().chain([0]).collect();
    let status = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_WRITE_THROUGH) };
    if status != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
fn rename_noreplace(_from: &Path, _to: &Path) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Atomic publication is unavailable on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn prepare(paths: &[PathBuf]) -> Result<Prepared> {
        super::prepare(paths, &AtomicBool::new(false))
    }

    #[test]
    fn file_changed_after_its_preview_is_rejected() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("archive.bin");
        fs::write(&path, b"before")?;
        let result = prepare_with_preview(
            std::slice::from_ref(&path),
            &AtomicBool::new(false),
            |preview| {
                assert_eq!(preview.display_names()?, ["archive.bin"]);
                assert_eq!(preview.bytes, 6);
                fs::write(&path, b"changed afterwards")?;
                Ok(())
            },
        );
        let Err(error) = result else {
            bail!("Prepared a file that changed after its offer")
        };
        let error = format!("{error:#}");
        assert!(error.contains("File changed while preparing"), "{error}");
        Ok(())
    }

    fn receive(
        stream: &mut impl ReadWrite,
        manifest: &Manifest,
        to: &Path,
        progress: impl FnMut(u64, u64),
    ) -> Result<Summary> {
        super::receive(
            stream,
            manifest,
            &Destination::open(to)?,
            &AtomicBool::new(false),
            ResumeVerification::silent(false),
            progress,
        )
    }

    fn confirm_receipt(to: &Path, manifest: &Manifest) -> Result<()> {
        super::confirm_receipt(&Destination::open(to)?, manifest)
    }

    #[derive(Default)]
    struct Exchange {
        incoming: Cursor<Vec<u8>>,
        outgoing: Vec<u8>,
    }

    impl Exchange {
        fn new(incoming: &[u8]) -> Self {
            Self {
                incoming: Cursor::new(incoming.to_vec()),
                outgoing: Vec::new(),
            }
        }

        fn requests(&self) -> Vec<u32> {
            self.outgoing
                .chunks_exact(4)
                .map(|chunk| {
                    let mut index = [0; 4];
                    index.copy_from_slice(chunk);
                    u32::from_be_bytes(index)
                })
                .filter(|index| *index != u32::MAX)
                .collect()
        }
    }

    impl Read for Exchange {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.incoming.read(buffer)
        }
    }

    impl Write for Exchange {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.outgoing.write(buffer)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn retries_only_unfinished_file_and_keeps_existing_folder() -> Result<()> {
        let source = tempfile::tempdir()?;
        let folder = source.path().join("Photos");
        fs::create_dir(&folder)?;
        fs::write(folder.join("a.txt"), "first")?;
        fs::write(folder.join("b.txt"), "second")?;
        fs::create_dir(folder.join("empty"))?;
        let prepared = prepare(&[folder])?;
        let to = tempfile::tempdir()?;
        let existing = to.path().join("Photos");
        fs::create_dir(&existing)?;
        fs::write(existing.join("keep.txt"), "existing")?;

        let mut interrupted = Exchange::new(b"firstsec");
        assert!(receive(&mut interrupted, &prepared.manifest, to.path(), |_, _| {}).is_err());
        assert_eq!(interrupted.requests(), [2, 1, 2]);
        assert!(!to.path().join("Photos (2)").exists());
        let saved = pending(to.path())?.pop().unwrap();
        let stage = to.path().join(STAGING_DIRECTORY).join(saved.id);
        fs::write(stage.join(".partial-interrupted"), b"unverified bytes")?;
        fs::write(stage.join(".state-interrupted"), b"unfinished journal")?;
        assert_eq!(pending(to.path())?[0].bytes, 5);

        let mut retry = Exchange::new(b"second");
        let mut progress = Vec::new();
        let summary = receive(&mut retry, &prepared.manifest, to.path(), |done, _| {
            progress.push(done);
        })?;
        assert_eq!(retry.requests(), [1, 2, PUBLISHED as u32]);
        assert_eq!(progress, [5, 11]);
        assert_eq!(summary.renamed, ["Photos -> Photos (2)"]);
        assert_eq!(fs::read(existing.join("keep.txt"))?, b"existing");
        let published = to.path().join("Photos (2)");
        assert_eq!(fs::read(published.join("a.txt"))?, b"first");
        assert_eq!(fs::read(published.join("b.txt"))?, b"second");
        assert!(published.join("empty").is_dir());
        assert_eq!(pending(to.path())?[0].bytes, 0);

        let mut lost_receipt = Exchange::default();
        let replayed_summary =
            receive(&mut lost_receipt, &prepared.manifest, to.path(), |_, _| {})?;
        assert_eq!(lost_receipt.requests(), [0, PUBLISHED as u32]);
        assert_eq!(replayed_summary.renamed, summary.renamed);
        assert!(!to.path().join("Photos (3)").exists());

        confirm_receipt(to.path(), &prepared.manifest)?;
        assert!(pending(to.path())?.is_empty());
        Ok(())
    }

    #[test]
    fn resumed_publication_preserves_already_published_item() -> Result<()> {
        let source = tempfile::tempdir()?;
        let first = source.path().join("a.txt");
        let second = source.path().join("b.txt");
        fs::write(&first, "a")?;
        fs::write(&second, "b")?;
        let prepared = prepare(&[first, second])?;
        let to = tempfile::tempdir()?;
        let (directory, base) = staging_base(to.path(), true)?;
        let stage = base.join(prepared.manifest.id()?);
        let mut state = initialize_stage(&base, &stage, &prepared.manifest, || Ok(()))?;
        fs::write(stage_item(&stage, 0), "a")?;
        state.published = vec![Some("a.txt".into()), Some("b.txt".into())];
        write_state(&stage, &state)?;
        let interrupted = publish_group(
            &directory,
            &stage,
            &mut state,
            &prepared.manifest.roots,
            &[0, 1],
            || {
                ensure!(!directory.join("a.txt").exists(), "Download canceled");
                Ok(())
            },
        );
        assert_eq!(interrupted.unwrap_err().to_string(), "Download canceled");
        assert!(stage_item(&stage, 1).exists());

        fs::write(directory.join("b.txt"), "existing")?;
        let mut exchange = Exchange::new(b"b");
        receive(&mut exchange, &prepared.manifest, to.path(), |_, _| {})?;
        assert_eq!(exchange.requests(), [1, 1, PUBLISHED as u32]);
        assert_eq!(fs::read(to.path().join("a.txt"))?, b"a");
        assert_eq!(fs::read(to.path().join("b.txt"))?, b"existing");
        assert_eq!(fs::read(to.path().join("b (2).txt"))?, b"b");
        assert!(!to.path().join("a (2).txt").exists());
        confirm_receipt(to.path(), &prepared.manifest)?;
        Ok(())
    }

    #[test]
    fn updates_the_journal_if_a_planned_name_is_taken() -> Result<()> {
        let source = tempfile::tempdir()?;
        let file = source.path().join("file.txt");
        fs::write(&file, "received")?;
        let prepared = prepare(&[file])?;
        let to = tempfile::tempdir()?;
        let (directory, base) = staging_base(to.path(), true)?;
        let stage = base.join(prepared.manifest.id()?);
        let mut state = initialize_stage(&base, &stage, &prepared.manifest, || Ok(()))?;
        fs::write(stage_item(&stage, 0), "received")?;
        state.published[0] = Some("file.txt".into());
        write_state(&stage, &state)?;
        fs::write(directory.join("file.txt"), "existing")?;

        publish_group(
            &directory,
            &stage,
            &mut state,
            &prepared.manifest.roots,
            &[0],
            || Ok(()),
        )?;
        assert_eq!(fs::read(directory.join("file.txt"))?, b"existing");
        assert_eq!(fs::read(directory.join("file (2).txt"))?, b"received");

        let mut retry = Exchange::default();
        let summary = receive(&mut retry, &prepared.manifest, to.path(), |_, _| {})?;
        assert_eq!(retry.requests(), [0, PUBLISHED as u32]);
        assert_eq!(summary.renamed, ["file.txt -> file (2).txt"]);
        assert!(!directory.join("file (3).txt").exists());
        confirm_receipt(to.path(), &prepared.manifest)
    }

    #[test]
    fn publishes_top_level_files_with_matching_names() -> Result<()> {
        let source = tempfile::tempdir()?;
        let mut files = Vec::new();
        for (index, name) in ["photo.jpg", "photo.jpg", "Photo.jpg"]
            .into_iter()
            .enumerate()
        {
            let folder = source.path().join(index.to_string());
            fs::create_dir(&folder)?;
            let file = folder.join(name);
            fs::write(&file, [b'1' + index as u8])?;
            files.push(file);
        }
        let prepared = prepare(&files)?;
        let to = tempfile::tempdir()?;
        fs::write(to.path().join("photo.jpg"), b"existing")?;
        let case_insensitive = to.path().join("Photo.jpg").exists();
        let mut exchange = Exchange::new(b"123");
        receive(&mut exchange, &prepared.manifest, to.path(), |_, _| {})?;
        let third = if case_insensitive {
            "Photo (4).jpg"
        } else {
            "Photo.jpg"
        };
        assert_eq!(fs::read(to.path().join("photo.jpg"))?, b"existing");
        assert_eq!(fs::read(to.path().join("photo (2).jpg"))?, b"1");
        assert_eq!(fs::read(to.path().join("photo (3).jpg"))?, b"2");
        assert_eq!(fs::read(to.path().join(third))?, b"3");
        confirm_receipt(to.path(), &prepared.manifest)
    }

    #[test]
    fn reuses_completed_small_files_left_before_group_sync() -> Result<()> {
        let source = tempfile::tempdir()?;
        let folder = source.path().join("batch");
        fs::create_dir(&folder)?;
        fs::write(folder.join("a.txt"), b"first")?;
        fs::write(folder.join("b.txt"), b"second")?;
        let prepared = prepare(&[folder])?;
        let to = tempfile::tempdir()?;
        let (_, base) = staging_base(to.path(), true)?;
        let stage = base.join(prepared.manifest.id()?);
        initialize_stage(&base, &stage, &prepared.manifest, || Ok(()))?;
        fs::write(resume::path(&stage, 1), b"first")?;
        fs::write(resume::path(&stage, 2), b"wrong!")?;

        let mut exchange = Exchange::new(b"second");
        let mut initial = None;
        receive(&mut exchange, &prepared.manifest, to.path(), |done, _| {
            initial.get_or_insert(done);
        })?;
        assert_eq!(initial, Some(5));
        assert_eq!(exchange.requests(), [1, 2, PUBLISHED as u32]);
        assert_eq!(fs::read(to.path().join("batch/a.txt"))?, b"first");
        assert_eq!(fs::read(to.path().join("batch/b.txt"))?, b"second");
        confirm_receipt(to.path(), &prepared.manifest)
    }

    #[test]
    fn rejects_a_corrupt_checkpoint_and_reuses_the_valid_prefix() -> Result<()> {
        let source = tempfile::tempdir()?;
        let original = source.path().join("large.bin");
        let interval = 16 * 1024 * 1024;
        let bytes = 2 * interval + 2;
        File::create(&original)?.set_len(bytes)?;
        let prepared = prepare(&[original])?;
        let to = tempfile::tempdir()?;
        let (_, base) = staging_base(to.path(), true)?;
        let stage = base.join(prepared.manifest.id()?);
        initialize_stage(&base, &stage, &prepared.manifest, || Ok(()))?;
        let mut partial = resume::open(&resume::path(&stage, 0))?;
        partial.set_len(2 * interval + 1)?;
        partial.seek(SeekFrom::Start(interval))?;
        partial.write_all(b"changed bytes")?;
        drop(partial);

        let mut remaining = Exchange::new(&vec![0; (bytes - interval) as usize]);
        let mut initial = None;
        receive(&mut remaining, &prepared.manifest, to.path(), |done, _| {
            initial.get_or_insert(done);
        })?;
        assert_eq!(initial, Some(interval));
        assert_eq!(
            remaining.requests(),
            [
                resume::REQUESTS_WITH_CHECKPOINTS as u32,
                1,
                0,
                1,
                PUBLISHED as u32
            ]
        );
        let saved = to.path().join("large.bin");
        assert_eq!(fs::metadata(&saved)?.len(), bytes);
        let mut file = File::open(&saved)?;
        file.seek(SeekFrom::Start(interval))?;
        let mut recovered = [0xff; 13];
        file.read_exact(&mut recovered)?;
        assert_eq!(recovered, [0; 13]);
        confirm_receipt(to.path(), &prepared.manifest)
    }

    #[test]
    fn malformed_request_and_traversal_paths_are_rejected() -> Result<()> {
        let files = tempfile::tempdir()?;
        let file = files.path().join("file.txt");
        fs::write(&file, "contents")?;
        let mut prepared = prepare(&[file])?;
        let mut transfer = Exchange::new(&[0, 0, 0, 1, 0, 0, 0, 32]);
        assert!(
            send(
                &mut transfer,
                &prepared,
                ResumeVerification::silent(false),
                |_, _| {}
            )
            .is_err()
        );
        assert!(transfer.outgoing.is_empty());
        prepared.manifest.roots[0].name = "..".to_string();
        assert!(describe(&prepared.manifest).is_err());
        Ok(())
    }

    #[test]
    fn preserves_unicode_and_handles_destination_case_collisions() -> Result<()> {
        let source = tempfile::tempdir()?;
        let folder = source.path().join("album");
        fs::create_dir(&folder)?;
        fs::write(folder.join("first"), "top")?;
        fs::write(folder.join("second"), "bottom")?;
        let mut prepared = prepare(&[folder])?;
        prepared.manifest.roots[0].entries[1].relative = vec!["Photo-猫.jpg".into()];
        prepared.manifest.roots[0].entries[2].relative = vec!["photo-猫.jpg".into()];

        let to = tempfile::tempdir()?;
        fs::write(to.path().join("CaseProbe"), b"")?;
        let case_insensitive = to.path().join("caseprobe").exists();
        let mut exchange = Exchange::new(b"topbottom");
        let summary = receive(&mut exchange, &prepared.manifest, to.path(), |_, _| {})?;
        let published = to.path().join("album");
        let second = if case_insensitive {
            assert_eq!(
                summary.renamed,
                ["album/photo-猫.jpg -> album/photo-猫 (2).jpg"]
            );
            "photo-猫 (2).jpg"
        } else {
            assert!(summary.renamed.is_empty());
            "photo-猫.jpg"
        };
        assert_eq!(fs::read(published.join("Photo-猫.jpg"))?, b"top");
        assert_eq!(fs::read(published.join(second))?, b"bottom");
        confirm_receipt(to.path(), &prepared.manifest)?;
        Ok(())
    }

    #[test]
    fn streamed_files_check_their_hash_before_publication() -> Result<()> {
        let source = tempfile::tempdir()?;
        let file = source.path().join("example.txt");
        fs::write(&file, b"payload")?;
        let paths = [file];
        let mut batch = scan_batch(&paths, &AtomicBool::new(false), |_, _| {})?;
        batch.enable_streaming(&paths)?;
        let destination = tempfile::tempdir()?;

        let mut incoming = vec![0; 4];
        incoming.extend_from_slice(b"payload");
        incoming.extend_from_slice(&[0; 32]);
        let error = receive(
            &mut Exchange::new(&incoming),
            &batch.manifest,
            destination.path(),
            |_, _| {},
        )
        .unwrap_err();
        assert!(error.to_string().contains("Checksum mismatch"), "{error:#}");
        assert!(!destination.path().join("example.txt").exists());
        Ok(())
    }

    #[test]
    fn bad_checksum_is_not_published_and_staged_batch_can_be_discarded() -> Result<()> {
        let files = tempfile::tempdir()?;
        let file = files.path().join("file.txt");
        fs::write(&file, "content")?;
        let prepared = prepare(&[file])?;
        let to = tempfile::tempdir()?;
        let mut exchange = Exchange::new(b"corrupt");
        let error = receive(&mut exchange, &prepared.manifest, to.path(), |_, _| {})
            .expect_err("corrupted payload must fail checksum verification");
        assert!(error.to_string().contains("Checksum mismatch"));
        assert!(!to.path().join("file.txt").exists());
        let pending = pending(to.path())?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].bytes, 0);
        discard(to.path(), &pending[0].id)?;
        Ok(())
    }

    #[test]
    fn preserves_sha256_digests_written_by_previous_versions() -> Result<()> {
        let mut input = &b"abc"[..];
        let mut buffer = [0; 2];
        let actual = copy_exact(&mut input, &mut io::sink(), 3, &mut buffer, |_| Ok(()))?;
        assert_eq!(
            actual,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        Ok(())
    }

    #[test]
    fn storage_full_does_not_report_progress_for_unwritten_bytes() {
        struct FullDestination;

        impl Write for FullDestination {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::StorageFull.into())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut input = &b"data"[..];
        let mut buffer = [0; 4];
        let result = copy_exact(&mut input, &mut FullDestination, 4, &mut buffer, |_| {
            panic!("unwritten bytes must not count as transferred");
        });
        let error = result.unwrap_err().downcast::<io::Error>().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::StorageFull);
    }

    #[cfg(unix)]
    #[test]
    fn selected_links_and_invalid_terminal_text_are_safe() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let folder = temp.path().join("collection");
        fs::create_dir(&folder)?;
        fs::write(folder.join("hello.txt"), "file")?;
        let link = folder.join("\u{1b}[test");
        std::os::unix::fs::symlink("/etc/passwd", link)?;
        let prepared = prepare(&[folder])?;
        let to = tempfile::tempdir()?;
        let mut exchange = Exchange::new(b"file");
        let summary = receive(&mut exchange, &prepared.manifest, to.path(), |_, _| {})?;
        assert_eq!(summary.skipped.len(), 1);
        assert!(summary.skipped[0].contains(r"\u{1b}"));
        assert!(!summary.skipped[0].contains('\u{1b}'));
        assert_eq!(fs::read_dir(to.path().join("collection"))?.count(), 1);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn skipped_directory_link_stays_skipped_with_trailing_slash() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let target = temp.path().join("private");
        fs::create_dir(&target)?;
        fs::write(target.join("data.txt"), "contents")?;
        let link = temp.path().join("shortcut");
        std::os::unix::fs::symlink(target, &link)?;

        let mut trailing_slash = link.as_os_str().to_os_string();
        trailing_slash.push("/");
        for path in [PathBuf::from(trailing_slash), link.join(".")] {
            let error = prepare(&[path]).err().expect("directory link was sent");
            assert!(error.to_string().contains("shortcut (link)"));
        }
        Ok(())
    }

    #[test]
    fn reserved_staging_folder_is_not_repurposed() -> Result<()> {
        let destination = tempfile::tempdir()?;
        let reserved = destination.path().join(STAGING_DIRECTORY);
        fs::create_dir(&reserved)?;
        fs::write(reserved.join("personal"), "keep this")?;
        assert!(staging_base(destination.path(), true).is_err());
        assert_eq!(fs::read(reserved.join("personal"))?, b"keep this");
        Ok(())
    }
}
