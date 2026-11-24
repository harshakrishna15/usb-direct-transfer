use super::{
    CHUNK, Entry, FileIdentity, Kind, Modified, PUBLISHED, Prepared, ReadWrite, ResumeVerification,
    Source, StagedFile, copy_and_hash_payload, copy_hashed, copy_payload, display_path, is_regular,
    metadata_if_present, open_regular, preparation_pulse, preparing_pulse, read_integer,
    read_prepared, resume, staging_batch, sync_directory, write_integer,
};
use anyhow::{Context, Result, bail, ensure};
use ring::digest;
use std::collections::BTreeMap;
use std::fs::{self, File, Metadata};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use std::{sync::mpsc, thread};

// The receiver offers checkpoints. The sender replies with the longest verified
// prefix for each file, then sends the remaining bytes and file hashes.
// With parallel verification, offsets arrive first and digests are framed so
// the receiver can send keepalives while it reads each checkpoint.
const CHECKPOINT_READY: usize = 0;

struct Prefix {
    bytes: u64,
    hasher: digest::Context,
}

struct ProposedFile {
    index: usize,
    checkpoints: Vec<(u64, Option<[u8; 32]>)>,
}

fn read_proposals(
    stream: &mut impl Read,
    entries: &[&Entry],
    parallel_resume: bool,
) -> Result<Vec<ProposedFile>> {
    let count = read_prepared(stream)?;
    ensure!(count <= entries.len(), "Invalid saved prefix count");
    let mut offered: Vec<ProposedFile> = Vec::with_capacity(count);
    let mut budget = resume::MAX_CHECKPOINTS;
    for _ in 0..count {
        let index = read_integer(stream)?;
        ensure!(
            offered.last().is_none_or(|previous| previous.index < index),
            "Invalid saved file order"
        );
        let Some(Kind::File { bytes, .. }) = entries.get(index).map(|entry| &entry.kind) else {
            anyhow::bail!("Unknown file in resume request");
        };
        let count = read_integer(stream)?;
        ensure!(
            count > 0 && count - 1 <= budget,
            "Invalid saved checkpoint count"
        );
        budget -= count - 1;
        let mut checkpoints = Vec::with_capacity(count);
        let mut previous = 0;
        for _ in 0..count {
            let offset = u64::from_be_bytes(read_array(stream)?);
            ensure!(
                (previous < offset) && offset <= *bytes,
                "Invalid saved file length"
            );
            let expected = if parallel_resume {
                None
            } else {
                Some(read_array::<32>(stream)?)
            };
            checkpoints.push((offset, expected));
            previous = offset;
        }
        offered.push(ProposedFile { index, checkpoints });
    }
    Ok(offered)
}

fn verify_proposals(
    batch: &Prepared,
    entries: &[&Entry],
    offered: &[ProposedFile],
    buffer: &mut [u8],
    mut hash: impl FnMut(Option<[u8; 32]>) -> Result<[u8; 32]>,
    mut checked: impl FnMut(u64) -> Result<()>,
) -> Result<BTreeMap<usize, Prefix>> {
    let mut accepted = BTreeMap::new();
    for ProposedFile { index, checkpoints } in offered {
        let index = *index;
        let source = batch.sources[index]
            .as_ref()
            .context("Missing source file")?;
        let entry = entries[index];
        let Kind::File { bytes, .. } = entry.kind else {
            unreachable!("Resume requests were validated");
        };
        let mut file = open_regular(&source.path)?;
        check_source(&file, source, bytes, entry.modified)?;
        let mut hasher = digest::Context::new(&digest::SHA256);
        let mut previous = 0;
        let mut verified = None;
        let mut matching = true;
        for &(offset, supplied) in checkpoints {
            if matching {
                copy_hashed(
                    &mut file,
                    &mut io::sink(),
                    offset - previous,
                    buffer,
                    &mut hasher,
                    &mut checked,
                )?;
            }
            let expected = hash(supplied)?;
            if matching && hasher.clone().finish().as_ref() == expected {
                verified = Some(Prefix {
                    bytes: offset,
                    hasher: hasher.clone(),
                });
            } else {
                matching = false;
            }
            previous = offset;
        }
        check_source(&file, source, bytes, entry.modified)?;
        if let Some(prefix) = verified {
            accepted.insert(index, prefix);
        }
    }
    Ok(accepted)
}

fn verify_in_parallel(
    stream: &mut impl ReadWrite,
    batch: &Prepared,
    entries: &[&Entry],
    offered: &[ProposedFile],
    buffer: &mut [u8],
    mut checked: impl FnMut(u64) + Send,
) -> Result<BTreeMap<usize, Prefix>> {
    let stopped = AtomicBool::new(false);
    thread::scope(|scope| {
        let (submit, hashes) = mpsc::channel();
        let (finish, outcome) = mpsc::channel();
        let cancelled = &stopped;
        // The plan bounds the hash queue. Drain the receiver's results while a
        // worker checks the source so neither disk blocks the network stream.
        thread::Builder::new().spawn_scoped(scope, move || {
            let verified = verify_proposals(
                batch,
                entries,
                offered,
                buffer,
                |_| {
                    hashes
                        .recv()
                        .context("Received saved checkpoints ended early")
                },
                |count| {
                    ensure!(!cancelled.load(Ordering::Relaxed), "Transfer canceled");
                    checked(count);
                    Ok(())
                },
            );
            let _ = finish.send(verified);
        })?;

        let result = (|| {
            let count: usize = offered.iter().map(|file| file.checkpoints.len()).sum();
            for _ in 0..count {
                ensure!(
                    read_prepared(stream)? == CHECKPOINT_READY,
                    "Invalid saved checkpoint marker"
                );
                if submit.send(read_array::<32>(stream)?).is_err() {
                    break;
                }
            }
            drop(submit);
            let mut next_pulse = Instant::now() + Duration::from_secs(1);
            loop {
                match outcome.recv_timeout(Duration::from_secs(1)) {
                    Ok(result) => return result,
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        preparation_pulse(stream, &mut next_pulse)?;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        bail!("Source verification stopped");
                    }
                }
            }
        })();
        stopped.store(true, Ordering::Relaxed);
        result
    })
}

pub(super) fn send(
    stream: &mut impl ReadWrite,
    batch: &Prepared,
    mut resume: ResumeVerification<impl FnMut(u64, u64) + Send>,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let entries: Vec<_> = batch.manifest.entries().collect();
    let offered = read_proposals(stream, &entries, resume.parallel)?;
    let planned = offered
        .iter()
        .filter_map(|file| file.checkpoints.last().map(|(bytes, _)| bytes))
        .sum();
    let mut bytes_checked = 0;
    let mut checked = |count| {
        bytes_checked += count;
        (resume.checked)(bytes_checked, planned);
    };
    if planned > 0 {
        checked(0);
    }
    let mut buffer = vec![0; CHUNK];
    let mut accepted = if resume.parallel && !offered.is_empty() {
        verify_in_parallel(stream, batch, &entries, &offered, &mut buffer, checked)?
    } else {
        let mut next_pulse = Instant::now() + Duration::from_secs(1);
        verify_proposals(
            batch,
            &entries,
            &offered,
            &mut buffer,
            |expected| expected.context("Missing saved checksum"),
            |count| {
                checked(count);
                preparation_pulse(stream, &mut next_pulse)
            },
        )?
    };
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, accepted.len())?;
        for (&index, prefix) in &accepted {
            write_integer(&mut writer, index)?;
            writer.write_all(&prefix.bytes.to_be_bytes())?;
        }
        writer.flush()?;
    }

    let total = batch.bytes;
    let mut complete = accepted.values().map(|prefix| prefix.bytes).sum();
    progress(complete, total);
    let mut payload = vec![0; CHUNK];
    for (index, entry) in entries.into_iter().enumerate() {
        let Kind::File { bytes, .. } = entry.kind else {
            continue;
        };
        let prefix = accepted.remove(&index);
        if prefix.as_ref().is_some_and(|prefix| prefix.bytes == bytes) {
            continue;
        }
        let source = batch.sources[index]
            .as_ref()
            .context("Missing source file")?;
        let mut file = open_regular(&source.path)?;
        check_source(&file, source, bytes, entry.modified)?;
        let (offset, mut hasher) = if let Some(prefix) = prefix {
            file.seek(SeekFrom::Start(prefix.bytes))?;
            (prefix.bytes, prefix.hasher)
        } else {
            (0, digest::Context::new(&digest::SHA256))
        };
        copy_and_hash_payload(
            bytes - offset,
            &mut buffer,
            &mut hasher,
            |reference| send_verified_chunk(&mut file, stream, reference, &mut payload),
            |count| {
                complete += count;
                progress(complete, total);
                Ok(())
            },
        )
        .with_context(|| format!("Cannot copy {}", display_path(&source.path)))?;
        check_source(&file, source, bytes, entry.modified)?;
        stream.write_all(hasher.finish().as_ref())?;
    }
    stream.flush()?;
    ensure!(
        read_prepared(stream)? == PUBLISHED,
        "Missing publication confirmation"
    );
    Ok(())
}

fn send_verified_chunk(
    source: &mut (impl Read + Seek),
    stream: &mut impl Write,
    reference: &mut [u8],
    payload: &mut [u8],
) -> io::Result<()> {
    // Keep the reference bytes for the file hash and send the second read.
    source.read_exact(reference)?;
    // Read this range again while it is still likely to be in the page cache.
    source.seek_relative(-(reference.len() as i64))?;
    let payload = &mut payload[..reference.len()];
    source.read_exact(payload)?;
    if payload != reference {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Source changed between reads",
        ));
    }
    stream.write_all(payload)
}

fn check_source(file: &File, source: &Source, bytes: u64, modified: Modified) -> Result<()> {
    let metadata = file.metadata()?;
    ensure!(
        metadata.len() == bytes
            && Modified::from(&metadata) == modified
            && FileIdentity::from(&metadata) == source.identity,
        "Source changed since the offer: {}",
        display_path(&source.path)
    );
    Ok(())
}

struct SavedPrefix {
    prefix: Prefix,
    metadata: Metadata,
    at_target: bool,
}

struct SavedFile {
    checkpoints: Vec<Prefix>,
    metadata: Metadata,
    at_target: bool,
}

struct SavedPlan {
    path: PathBuf,
    metadata: Metadata,
    at_target: bool,
    bytes: u64,
    interval: u64,
}

impl SavedPlan {
    fn offsets(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        let count = self.bytes.div_ceil(self.interval) as usize;
        (0..count).map(|index| {
            self.bytes
                .min(self.interval.saturating_mul(index as u64 + 1))
        })
    }
}

fn verify_saved_file(before: &Metadata, file: &File) -> Result<()> {
    let after = file.metadata()?;
    ensure!(
        after.len() == before.len()
            && Modified::from(&after) == Modified::from(before)
            && FileIdentity::from(&after) == FileIdentity::from(before),
        "Staged data changed during verification"
    );
    Ok(())
}

pub(super) fn receive(
    stream: &mut impl ReadWrite,
    stage: &Path,
    files: Vec<StagedFile<'_>>,
    total: u64,
    cancelled: &AtomicBool,
    mut resume: Option<ResumeVerification<impl FnMut(u64, u64)>>,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let mut buffer = vec![0; CHUNK];
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    let parallel_resume = resume.as_ref().is_some_and(|resume| resume.parallel);
    let mut plans = Vec::new();
    if resume.is_some() {
        let mut budget = resume::MAX_CHECKPOINTS;
        for file in &files {
            preparing_pulse(stream, &mut next_pulse, cancelled)?;
            if let Some(plan) = saved_plan(stage, file, budget)? {
                budget -= plan.offsets().len() - 1;
                plans.push((file.index, plan));
            }
        }
    }
    let planned: u64 = plans.iter().map(|(_, plan)| plan.bytes).sum();
    let mut bytes_checked = 0;
    let mut checked = |count| {
        bytes_checked += count;
        if let Some(resume) = &mut resume {
            (resume.checked)(bytes_checked, planned);
        }
    };
    if planned > 0 {
        checked(0);
    }
    let mut offered = BTreeMap::new();
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        if parallel_resume {
            write_integer(&mut writer, plans.len())?;
            for (index, plan) in &plans {
                let offsets = plan.offsets();
                write_integer(&mut writer, *index)?;
                write_integer(&mut writer, offsets.len())?;
                for offset in offsets {
                    writer.write_all(&offset.to_be_bytes())?;
                }
            }
            writer.flush()?;
        }
        for (index, plan) in plans {
            let saved = hash_saved(plan, &mut buffer, |count, checkpoint| {
                preparing_pulse(&mut writer, &mut next_pulse, cancelled)?;
                if count > 0 {
                    checked(count);
                }
                if parallel_resume && let Some(checkpoint) = checkpoint {
                    write_integer(&mut writer, CHECKPOINT_READY)?;
                    writer.write_all(checkpoint.hasher.clone().finish().as_ref())?;
                    if checkpoint.bytes >= resume::MIN_INTERVAL {
                        writer.flush()?;
                    }
                }
                Ok(())
            })?;
            offered.insert(index, saved);
        }
        if !parallel_resume {
            write_integer(&mut writer, offered.len())?;
            for (&index, saved) in &offered {
                write_integer(&mut writer, index)?;
                write_integer(&mut writer, saved.checkpoints.len())?;
                for checkpoint in &saved.checkpoints {
                    writer.write_all(&checkpoint.bytes.to_be_bytes())?;
                    writer.write_all(checkpoint.hasher.clone().finish().as_ref())?;
                }
            }
        }
        writer.flush()?;
    }
    let count = read_prepared(stream)?;
    ensure!(count <= offered.len(), "Invalid accepted prefix count");
    let mut accepted = BTreeMap::new();
    for _ in 0..count {
        let index = read_integer(stream)?;
        let bytes = u64::from_be_bytes(read_array(stream)?);
        let saved = offered
            .remove(&index)
            .context("Unexpected saved file index")?;
        let prefix = saved
            .checkpoints
            .into_iter()
            .find(|checkpoint| checkpoint.bytes == bytes)
            .context("Unexpected saved checkpoint")?;
        accepted.insert(
            index,
            SavedPrefix {
                prefix,
                metadata: saved.metadata,
                at_target: saved.at_target,
            },
        );
    }
    for file in &files {
        ensure!(
            file.is_staged || file.bytes == 0 || accepted.contains_key(&file.index),
            "Previously published data changed: {}",
            display_path(&file.path)
        );
    }

    let mut complete = accepted.values().map(|saved| saved.prefix.bytes).sum();
    progress(complete, total);
    let mut small_files = staging_batch::Batch::default();
    let received = (|| -> Result<()> {
        for file in files {
            ensure!(!cancelled.load(Ordering::Relaxed), "Download canceled");
            let saved = accepted.remove(&file.index);
            receive_file(
                stream,
                stage,
                file,
                saved,
                &mut buffer,
                &mut small_files,
                |count| {
                    complete += count;
                    progress(complete, total);
                    Ok(())
                },
            )?;
        }
        Ok(())
    })();
    received.and(small_files.flush(stage))
}

fn saved_plan(stage: &Path, file: &StagedFile<'_>, budget: usize) -> Result<Option<SavedPlan>> {
    let saved = metadata_if_present(&file.path)?;
    if !file.is_staged {
        ensure!(
            saved
                .as_ref()
                .is_some_and(|metadata| is_regular(metadata) && metadata.len() == file.bytes),
            "Previously published data changed: {}",
            display_path(&file.path)
        );
    }
    if file.bytes == 0 {
        return Ok(None);
    }
    let (path, metadata, at_target) = match saved {
        Some(metadata) if is_regular(&metadata) && metadata.len() == file.bytes => {
            (file.path.clone(), metadata, true)
        }
        _ => {
            let part = resume::path(stage, file.index);
            let Some(metadata) = metadata_if_present(&part)? else {
                return Ok(None);
            };
            ensure!(is_regular(&metadata), "Expected a regular saved file");
            (part, metadata, false)
        }
    };
    let present = metadata.len().min(file.bytes);
    let bytes = if present == file.bytes {
        present
    } else {
        present / resume::MIN_INTERVAL * resume::MIN_INTERVAL
    };
    if bytes == 0 {
        return Ok(None);
    }
    let interval = if at_target {
        bytes
    } else {
        resume::interval(bytes, budget)
    };
    Ok(Some(SavedPlan {
        path,
        metadata,
        at_target,
        bytes,
        interval,
    }))
}

fn hash_saved(
    plan: SavedPlan,
    buffer: &mut [u8],
    mut report: impl FnMut(u64, Option<&Prefix>) -> Result<()>,
) -> Result<SavedFile> {
    let mut input = open_regular(&plan.path)?;
    verify_saved_file(&plan.metadata, &input)?;
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut checkpoints = Vec::new();
    let mut previous = 0;
    for offset in plan.offsets() {
        copy_hashed(
            &mut input,
            &mut io::sink(),
            offset - previous,
            buffer,
            &mut hasher,
            |count| report(count, None),
        )?;
        let prefix = Prefix {
            bytes: offset,
            hasher: hasher.clone(),
        };
        report(0, Some(&prefix))?;
        checkpoints.push(prefix);
        previous = offset;
    }
    verify_saved_file(&plan.metadata, &input)?;
    Ok(SavedFile {
        checkpoints,
        metadata: plan.metadata,
        at_target: plan.at_target,
    })
}

fn receive_file(
    stream: &mut impl Read,
    stage: &Path,
    file: StagedFile<'_>,
    saved: Option<SavedPrefix>,
    buffer: &mut [u8],
    small_files: &mut staging_batch::Batch,
    copied: impl FnMut(u64) -> Result<()>,
) -> Result<()> {
    let empty_hash = || digest::Context::new(&digest::SHA256);
    let verify = |stream: &mut _, hasher: digest::Context| -> Result<()> {
        ensure!(
            hasher.finish().as_ref() == read_array::<32>(stream)?,
            "Checksum mismatch for {}",
            display_path(&file.path)
        );
        Ok(())
    };
    let saved = match saved {
        Some(saved) if saved.at_target => {
            return verify_saved_file(&saved.metadata, &open_regular(&file.path)?);
        }
        other => other,
    };
    if !file.is_staged {
        // A previously published empty file has no prefix to propose.
        let metadata = open_regular(&file.path)?.metadata()?;
        ensure!(metadata.len() == 0, "Previously published data changed");
        return verify(stream, empty_hash());
    }
    if file.bytes <= staging_batch::MAX_SMALL_FILE && saved.is_none() {
        let mut part = tempfile::Builder::new()
            .prefix(".partial-")
            .tempfile_in(stage)?;
        let mut hasher = empty_hash();
        copy_payload(
            stream,
            part.as_file_mut(),
            file.bytes,
            buffer,
            &mut hasher,
            copied,
        )?;
        verify(stream, hasher)?;
        return small_files.add(stage, file.index, file.path, part);
    }

    small_files.flush(stage)?;
    let part = resume::path(stage, file.index);
    let mut output = resume::open(&part)?;
    let (offset, mut hasher) = if let Some(saved) = saved {
        verify_saved_file(&saved.metadata, &output)?;
        (saved.prefix.bytes, saved.prefix.hasher)
    } else {
        (0, empty_hash())
    };
    output.set_len(offset)?;
    output.seek(SeekFrom::Start(offset))?;
    if offset != file.bytes {
        copy_payload(
            stream,
            &mut output,
            file.bytes - offset,
            buffer,
            &mut hasher,
            copied,
        )?;
        verify(stream, hasher)?;
    }
    output.sync_all()?;
    drop(output);
    fs::rename(part, &file.path)?;
    sync_directory(file.path.parent().context("Missing staging parent")?)
}

fn read_array<const N: usize>(stream: &mut impl Read) -> io::Result<[u8; N]> {
    let mut value = [0; N];
    stream.read_exact(&mut value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    struct ChangedOnRewind(Cursor<Vec<u8>>);

    impl Read for ChangedOnRewind {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.0.read(buffer)
        }
    }

    impl Seek for ChangedOnRewind {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            if matches!(position, SeekFrom::Current(offset) if offset < 0) {
                self.0.get_mut()[0] ^= 1;
            }
            self.0.seek(position)
        }
    }

    #[test]
    fn changing_bytes_between_reads_are_never_sent() {
        let mut reference = [0; 4];
        let mut payload = [0; 4];
        let mut copied = Vec::new();
        let mut input = ChangedOnRewind(Cursor::new(b"data".to_vec()));
        let result = copy_and_hash_payload(
            4,
            &mut reference,
            &mut digest::Context::new(&digest::SHA256),
            |reference| send_verified_chunk(&mut input, &mut copied, reference, &mut payload),
            |_| Ok(()),
        );
        assert!(
            result
                .is_err_and(|error| { error.to_string().contains("Source changed between reads") })
        );
        assert!(copied.is_empty());
    }
}
