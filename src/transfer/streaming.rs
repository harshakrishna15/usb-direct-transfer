use super::{
    CHUNK, Entry, FileIdentity, Kind, Modified, PUBLISHED, Prepared, ReadWrite, Source, StagedFile,
    copy_and_hash_payload, copy_hashed, copy_payload, display_path, is_regular,
    metadata_if_present, open_regular, preparation_pulse, preparing_pulse, read_integer,
    read_prepared, resume, staging_batch, sync_directory, write_integer,
};
use anyhow::{Context, Result, ensure};
use ring::digest;
use std::collections::BTreeMap;
use std::fs::{self, File, Metadata};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// The receiver offers checkpoint hashes. The sender replies with the longest
// verified prefix for each file, then sends remaining bytes and file hashes.
struct Prefix {
    bytes: u64,
    hasher: digest::Context,
}

struct ProposedFile {
    index: usize,
    checkpoints: Vec<(u64, [u8; 32])>,
}

fn read_proposals(stream: &mut impl Read, entries: &[&Entry]) -> Result<Vec<ProposedFile>> {
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
            checkpoints.push((offset, read_array::<32>(stream)?));
            previous = offset;
        }
        offered.push(ProposedFile { index, checkpoints });
    }
    Ok(offered)
}

pub(super) fn send(
    stream: &mut impl ReadWrite,
    batch: &Prepared,
    total: u64,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let entries: Vec<_> = batch.manifest.entries().collect();
    let offered = read_proposals(stream, &entries)?;

    let mut accepted = BTreeMap::new();
    let mut buffer = vec![0; CHUNK];
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    for ProposedFile { index, checkpoints } in offered {
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
        for (offset, expected) in checkpoints {
            copy_hashed(
                &mut file,
                &mut io::sink(),
                offset - previous,
                &mut buffer,
                &mut hasher,
                |_| preparation_pulse(stream, &mut next_pulse),
            )?;
            if hasher.clone().finish().as_ref() != expected {
                break;
            }
            previous = offset;
            verified = Some(Prefix {
                bytes: offset,
                hasher: hasher.clone(),
            });
        }
        check_source(&file, source, bytes, entry.modified)?;
        if let Some(prefix) = verified {
            accepted.insert(index, prefix);
        }
    }
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, accepted.len())?;
        for (&index, prefix) in &accepted {
            write_integer(&mut writer, index)?;
            writer.write_all(&prefix.bytes.to_be_bytes())?;
        }
        writer.flush()?;
    }

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
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let mut offered = BTreeMap::new();
    let mut buffer = vec![0; CHUNK];
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    let mut tick = || preparing_pulse(stream, &mut next_pulse, cancelled);
    let mut budget = resume::MAX_CHECKPOINTS;
    for file in &files {
        tick()?;
        if let Some(saved) = saved_checkpoints(stage, file, budget, &mut buffer, &mut tick)? {
            budget -= saved.checkpoints.len() - 1;
            offered.insert(file.index, saved);
        }
    }
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, offered.len())?;
        for (&index, saved) in &offered {
            write_integer(&mut writer, index)?;
            write_integer(&mut writer, saved.checkpoints.len())?;
            for checkpoint in &saved.checkpoints {
                writer.write_all(&checkpoint.bytes.to_be_bytes())?;
                writer.write_all(checkpoint.hasher.clone().finish().as_ref())?;
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

fn saved_checkpoints(
    stage: &Path,
    file: &StagedFile<'_>,
    budget: usize,
    buffer: &mut [u8],
    mut tick: impl FnMut() -> Result<()>,
) -> Result<Option<SavedFile>> {
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
    let part = resume::path(stage, file.index);
    let at_target =
        saved.is_some_and(|metadata| is_regular(&metadata) && metadata.len() == file.bytes);
    let path = if at_target { &file.path } else { &part };
    if !at_target && metadata_if_present(path)?.is_none() {
        return Ok(None);
    }
    let mut input = open_regular(path)?;
    let metadata = input.metadata()?;
    ensure!(
        !at_target || metadata.len() == file.bytes,
        "Staged file changed before verification"
    );
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
    let mut hasher = digest::Context::new(&digest::SHA256);
    let mut checkpoints = Vec::new();
    let mut offset = 0;
    while offset < bytes {
        let length = (bytes - offset).min(interval);
        copy_hashed(
            &mut input,
            &mut io::sink(),
            length,
            buffer,
            &mut hasher,
            |_| tick(),
        )?;
        offset += length;
        checkpoints.push(Prefix {
            bytes: offset,
            hasher: hasher.clone(),
        });
    }
    verify_saved_file(&metadata, &input)?;
    Ok(Some(SavedFile {
        checkpoints,
        metadata,
        at_target,
    }))
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
