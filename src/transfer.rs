use anyhow::{Context, Result, bail, ensure};
use filetime::FileTime;
use ring::digest;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{self, File, Metadata, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const STAGING_DIRECTORY: &str = ".usb-direct-transfer";
const MAX_ENTRIES: usize = 100_000;
const MAX_DEPTH: usize = 128;
const CHUNK: usize = 1024 * 1024;
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
    sources: Vec<Option<PathBuf>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub renamed: Vec<String>,
    pub skipped: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Pending {
    pub id: String,
    pub bytes: u64,
}

impl Manifest {
    fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.roots.iter().flat_map(|root| root.entries.iter())
    }

    // shortcut: retry reuses an unchanged manifest; changed batches stage independently.
    fn id(&self) -> Result<String> {
        let encoded = serde_json::to_vec(self)?;
        Ok(hex_digest(digest::digest(&digest::SHA256, &encoded)))
    }
}

pub fn prepare(paths: &[PathBuf], cancelled: &AtomicBool) -> Result<Prepared> {
    let mut manifest = Manifest {
        roots: Vec::new(),
        skipped: Vec::new(),
    };
    let mut sources = Vec::new();
    let mut buffer = vec![0; CHUNK];
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
        scan(
            path,
            Vec::new(),
            &mut root,
            &mut sources,
            &mut manifest.skipped,
            &mut buffer,
            cancelled,
        )
        .with_context(|| format!("Cannot read {}", display_path(path)))?;
        if !root.entries.is_empty() {
            manifest.roots.push(root);
        }
    }
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
    describe(&manifest)?;
    Ok(Prepared { manifest, sources })
}

fn scan(
    path: &Path,
    relative: Vec<String>,
    root: &mut Root,
    sources: &mut Vec<Option<PathBuf>>,
    skipped: &mut Vec<String>,
    buffer: &mut [u8],
    cancelled: &AtomicBool,
) -> Result<()> {
    ensure!(
        !cancelled.load(Ordering::Relaxed),
        "Transfer preparation canceled"
    );
    ensure!(relative.len() <= MAX_DEPTH, "Folder is nested too deeply");
    ensure!(
        sources.len() + skipped.len() < MAX_ENTRIES,
        "Too many items"
    );
    let metadata = fs::symlink_metadata(path)?;
    let modified = Modified::from(&metadata);
    if is_link(&metadata) {
        skipped.push(format!("{} (link)", raw_wire_path(&root.name, &relative)));
    } else if metadata.is_dir() {
        if path
            .file_name()
            .is_some_and(|name| name == STAGING_DIRECTORY)
            && verify_marker(path).is_ok()
        {
            skipped.push(format!(
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
        sources.push(None);
        let available = MAX_ENTRIES - sources.len() - skipped.len();
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
                    sources.len() + skipped.len() < MAX_ENTRIES,
                    "Too many items"
                );
                skipped.push(format!(
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
                skipped,
                buffer,
                cancelled,
            )?;
        }
    } else if metadata.is_file() {
        let mut file = open_regular(path)?;
        let bytes = metadata.len();
        let sha256 = copy_exact(&mut file, &mut io::sink(), bytes, buffer, |_| {
            ensure!(
                !cancelled.load(Ordering::Relaxed),
                "Transfer preparation canceled"
            );
            Ok(())
        })?;
        let after = file.metadata()?;
        ensure!(
            after.len() == bytes && Modified::from(&after) == modified,
            "File changed while preparing the transfer"
        );
        root.entries.push(Entry {
            relative,
            kind: Kind::File { bytes, sha256 },
            modified,
        });
        sources.push(Some(path.to_path_buf()));
    } else {
        skipped.push(format!(
            "{} (special file)",
            raw_wire_path(&root.name, &relative)
        ));
    }
    Ok(())
}

pub fn describe(manifest: &Manifest) -> Result<(Vec<String>, u64)> {
    ensure!(!manifest.roots.is_empty(), "Empty transfer");
    ensure!(
        manifest.entries().count() + manifest.skipped.len() <= MAX_ENTRIES,
        "Too many items"
    );
    let mut total = 0_u64;
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
                ensure!(is_digest(sha256), "Invalid file hash");
                total = total
                    .checked_add(*bytes)
                    .context("Transfer size overflow")?;
            }
        }
    }
    Ok((
        manifest
            .roots
            .iter()
            .map(|root| display_name(&root.name))
            .collect(),
        total,
    ))
}

pub fn send(
    stream: &mut impl ReadWrite,
    prepared: &Prepared,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let (_, total) = describe(&prepared.manifest)?;
    let entries: Vec<_> = prepared.manifest.entries().collect();
    let count = read_prepared(stream)?;
    ensure!(count <= entries.len(), "Invalid requested file count");
    let mut requests = Vec::with_capacity(count);
    let mut requested_bytes = 0_u64;
    for _ in 0..count {
        let index = read_integer(stream)?;
        ensure!(
            requests.last().is_none_or(|last| *last < index),
            "Invalid file order"
        );
        let Some(Entry {
            kind: Kind::File { bytes, .. },
            ..
        }) = entries.get(index)
        else {
            bail!("Receiver requested an unknown file");
        };
        requested_bytes += bytes;
        requests.push(index);
    }
    let mut completed = total - requested_bytes;
    progress(completed, total);
    let mut buffer = vec![0; CHUNK];
    for index in requests {
        let entry = entries[index];
        let Kind::File { bytes, sha256 } = &entry.kind else {
            unreachable!("Requested file type was checked");
        };
        let path = prepared.sources[index]
            .as_ref()
            .context("Missing source file")?;
        let mut source = open_regular(path)?;
        let metadata = source.metadata()?;
        ensure!(
            metadata.len() == *bytes && Modified::from(&metadata) == entry.modified,
            "Source changed since the offer: {}",
            display_path(path)
        );
        let digest = copy_exact(&mut source, stream, *bytes, &mut buffer, |size| {
            completed += size;
            progress(completed, total);
            Ok(())
        })?;
        ensure!(
            digest == *sha256,
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

pub fn receive(
    stream: &mut impl ReadWrite,
    manifest: &Manifest,
    to: &Path,
    mut progress: impl FnMut(u64, u64),
) -> Result<Summary> {
    let (_, total) = describe(manifest)?;
    let (directory, base) = staging_base(to, true)?;
    let _lock = lock_staging(&base)?;
    let stage = base.join(manifest.id()?);
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    let mut tick = || preparing_pulse(stream, &mut next_pulse);
    let mut state = if directory_exists(&stage)? {
        read_state(&stage)?
    } else {
        initialize_stage(&base, &stage, manifest, &mut tick)?
    };
    validate_state(&state, manifest)?;
    remove_temporary_entries(&stage, ".partial-")?;
    remove_temporary_entries(&stage, ".state-")?;
    let mut summary = Summary {
        skipped: manifest
            .skipped
            .iter()
            .map(|line| display_name(line))
            .collect(),
        ..Summary::default()
    };
    let mut requests = Vec::new();
    let mut staged_roots = Vec::new();
    let mut remaining = 0;
    let mut next_index = 0;
    let mut buffer = vec![0; CHUNK];
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
        for (entry, mapped) in root.entries.iter().zip(&state.paths[index]) {
            tick()?;
            let saved_path = checked_path(&saved_root, mapped)?;
            if let Kind::File { bytes, sha256 } = &entry.kind {
                if !matches_file(&saved_path, *bytes, sha256, &mut buffer, &mut tick)? {
                    ensure!(
                        is_staged,
                        "Previously published data changed: {}",
                        display_path(&saved_path)
                    );
                    requests.push((next_index, saved_path, *bytes, sha256));
                    remaining += bytes;
                }
            } else {
                require_directory(&saved_path)?;
            }
            next_index += 1;
        }
    }

    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, requests.len())?;
        for (index, ..) in &requests {
            write_integer(&mut writer, *index)?;
        }
        writer.flush()?;
    }
    let mut completed = total - remaining;
    progress(completed, total);
    for (_, path, bytes, expected_hash) in requests {
        let mut part = tempfile::Builder::new()
            .prefix(".partial-")
            .tempfile_in(&stage)?;
        let digest = copy_exact(stream, part.as_file_mut(), bytes, &mut buffer, |size| {
            completed += size;
            progress(completed, total);
            Ok(())
        })?;
        ensure!(
            digest == *expected_hash,
            "Checksum mismatch for {}",
            display_path(&path)
        );
        part.as_file().sync_all()?;
        part.persist(&path).map_err(|error| error.error)?;
        sync_directory(path.parent().context("Missing staging parent")?)?;
    }

    let mut tick = || preparing_pulse(stream, &mut next_pulse);
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
            &directory,
            root,
            choices,
            |candidate| reserved.contains(candidate),
            &mut tick,
        )?;
        reserved.insert(name.clone());
        state.published[index] = Some(name);
    }
    if !staged_roots.is_empty() {
        // The journal records every destination before any staged item moves.
        // The source's presence distinguishes planned items from published ones.
        write_state(&stage, &state)?;
    }

    for index in staged_roots {
        publish(
            &directory,
            &stage,
            &mut state,
            index,
            &manifest.roots[index],
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
    write_integer(stream, PUBLISHED)?;
    stream.flush()?;
    // Keep the publication journal until the sender confirms receipt. A dropped
    // receipt can then be retried without creating a second copy of any item.
    Ok(summary)
}

pub(crate) fn confirm_receipt(to: &Path, manifest: &Manifest) -> Result<()> {
    let (_, base) = staging_base(to, false)?;
    verify_marker(&base)?;
    let _lock = lock_staging(&base)?;
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
    sync_directory(&base)
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
            items.push(Pending {
                id,
                bytes: staged_bytes(&path, true)?,
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

fn publish(
    directory: &Path,
    stage: &Path,
    state: &mut State,
    index: usize,
    root: &Root,
    mut tick: impl FnMut() -> Result<()>,
) -> Result<()> {
    let source = stage_item(stage, index);
    let mut choices = publication_names(
        compatible_name(&root.name),
        matches!(root.entries[0].kind, Kind::Directory),
    );
    loop {
        tick()?;
        let destination = directory.join(
            state.published[index]
                .as_deref()
                .context("Missing destination in the publication journal")?,
        );
        match rename_noreplace(&source, &destination) {
            Ok(()) => {
                sync_directory(directory)?;
                sync_directory(stage)?;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                // The filesystem also detects case-equivalent names and races
                // with other programs. Record each changed name before moving.
                let name = unclaimed_name(directory, root, &mut choices, |_| false, &mut tick)?;
                state.published[index] = Some(name);
                write_state(stage, state)?;
            }
            Err(error) => return Err(error).context("Cannot safely publish the received item"),
        }
    }
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

fn staged_bytes(path: &Path, stage_root: bool) -> Result<u64> {
    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut size = 0_u64;
    for entry in entries {
        let entry = entry?;
        // Only numbered roots hold files that have passed verification.
        if stage_root
            && !entry
                .file_name()
                .as_encoded_bytes()
                .iter()
                .all(u8::is_ascii_digit)
        {
            continue;
        }
        let path = entry.path();
        let Some(metadata) = metadata_if_present(&path)? else {
            continue;
        };
        if is_directory(&metadata) {
            size += staged_bytes(&path, false)?;
        } else if is_regular(&metadata) {
            size += metadata.len();
        }
    }
    Ok(size)
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
        if path_exists(&candidate)? {
            continue;
        }
        match create_item(&candidate, folder) {
            Ok(()) => return Ok(name),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
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
    mut bytes: u64,
    buffer: &mut [u8],
    mut copied: impl FnMut(u64) -> Result<()>,
) -> Result<String> {
    let mut hasher = digest::Context::new(&digest::SHA256);
    while bytes > 0 {
        let count = bytes.min(buffer.len() as u64) as usize;
        let chunk = &mut buffer[..count];
        source.read_exact(chunk)?;
        hasher.update(chunk);
        destination.write_all(chunk)?;
        bytes -= count as u64;
        copied(count as u64)?;
    }
    Ok(hex_digest(hasher.finish()))
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

fn preparing_pulse(stream: &mut impl Write, next_pulse: &mut Instant) -> Result<()> {
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
        publish(
            &directory,
            &stage,
            &mut state,
            0,
            &prepared.manifest.roots[0],
            || Ok(()),
        )?;

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

        publish(
            &directory,
            &stage,
            &mut state,
            0,
            &prepared.manifest.roots[0],
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
    fn malformed_request_and_traversal_paths_are_rejected() -> Result<()> {
        let files = tempfile::tempdir()?;
        let file = files.path().join("file.txt");
        fs::write(&file, "contents")?;
        let mut prepared = prepare(&[file])?;
        let mut transfer = Exchange::new(&[0, 0, 0, 1, 0, 0, 0, 32]);
        assert!(send(&mut transfer, &prepared, |_, _| {}).is_err());
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
