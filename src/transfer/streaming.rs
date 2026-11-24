use super::{
    CHUNK, FileIdentity, Kind, Modified, PUBLISHED, Prepared, ReadWrite, Source, StagedFile,
    copy_hashed, copy_payload, display_path, is_regular, metadata_if_present, open_regular,
    preparation_pulse, preparing_pulse, read_integer, read_prepared, resume, staging_batch,
    sync_directory, write_integer,
};
use anyhow::{Context, Result, ensure};
use ring::digest;
use std::collections::BTreeMap;
use std::fs::{self, File, Metadata};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// The receiver offers hashes of saved prefixes. The sender replies with the
// indices it verified, then sends the remaining file bytes and each file hash.
struct Prefix {
    bytes: u64,
    hasher: digest::Context,
}

pub(super) fn send(
    stream: &mut impl ReadWrite,
    batch: &Prepared,
    total: u64,
    mut progress: impl FnMut(u64, u64),
) -> Result<()> {
    let entries: Vec<_> = batch.manifest.entries().collect();
    let count = read_prepared(stream)?;
    ensure!(count <= entries.len(), "Invalid saved prefix count");
    let mut offered = Vec::with_capacity(count);
    for _ in 0..count {
        let index = read_integer(stream)?;
        ensure!(
            offered
                .last()
                .is_none_or(|(previous, _, _)| *previous < index),
            "Invalid saved file order"
        );
        let Some(Kind::File { bytes, .. }) = entries.get(index).map(|entry| &entry.kind) else {
            anyhow::bail!("Unknown file in resume request");
        };
        let offset = u64::from_be_bytes(read_array(stream)?);
        ensure!((1..=*bytes).contains(&offset), "Invalid saved file length");
        let expected = read_array::<32>(stream)?;
        offered.push((index, offset, expected));
    }

    let mut accepted = BTreeMap::new();
    let mut buffer = vec![0; CHUNK];
    let mut next_pulse = Instant::now() + Duration::from_secs(1);
    for (index, offset, expected) in offered {
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
        copy_hashed(
            &mut file,
            &mut io::sink(),
            offset,
            &mut buffer,
            &mut hasher,
            |_| preparation_pulse(stream, &mut next_pulse),
        )?;
        check_source(&file, source, bytes, entry.modified)?;
        if hasher.clone().finish().as_ref() == expected {
            accepted.insert(
                index,
                Prefix {
                    bytes: offset,
                    hasher,
                },
            );
        }
    }
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, accepted.len())?;
        for &index in accepted.keys() {
            write_integer(&mut writer, index)?;
        }
        writer.flush()?;
    }

    let mut complete = accepted.values().map(|prefix| prefix.bytes).sum();
    progress(complete, total);
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
        copy_payload(
            &mut file,
            stream,
            bytes - offset,
            &mut buffer,
            &mut hasher,
            |count| {
                complete += count;
                progress(complete, total);
                Ok(())
            },
        )?;
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

impl SavedPrefix {
    fn verify_file(&self, file: &File) -> Result<()> {
        let metadata = file.metadata()?;
        ensure!(
            metadata.len() == self.metadata.len()
                && Modified::from(&metadata) == Modified::from(&self.metadata)
                && FileIdentity::from(&metadata) == FileIdentity::from(&self.metadata),
            "Staged data changed during verification"
        );
        Ok(())
    }
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
    for file in &files {
        tick()?;
        if let Some(prefix) = saved_prefix(stage, file, &mut buffer, &mut tick)? {
            offered.insert(file.index, prefix);
        }
    }
    {
        let mut writer = io::BufWriter::new(&mut *stream);
        write_integer(&mut writer, offered.len())?;
        for (&index, saved) in &offered {
            write_integer(&mut writer, index)?;
            writer.write_all(&saved.prefix.bytes.to_be_bytes())?;
            writer.write_all(saved.prefix.hasher.clone().finish().as_ref())?;
        }
        writer.flush()?;
    }
    let count = read_prepared(stream)?;
    ensure!(count <= offered.len(), "Invalid accepted prefix count");
    let mut accepted = BTreeMap::new();
    for _ in 0..count {
        let index = read_integer(stream)?;
        let prefix = offered
            .remove(&index)
            .context("Unexpected saved file index")?;
        accepted.insert(index, prefix);
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

fn saved_prefix(
    stage: &Path,
    file: &StagedFile<'_>,
    buffer: &mut [u8],
    mut tick: impl FnMut() -> Result<()>,
) -> Result<Option<SavedPrefix>> {
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
    let mut hasher = digest::Context::new(&digest::SHA256);
    copy_hashed(
        &mut input,
        &mut io::sink(),
        bytes,
        buffer,
        &mut hasher,
        |_| tick(),
    )?;
    let saved = SavedPrefix {
        prefix: Prefix { bytes, hasher },
        metadata,
        at_target,
    };
    saved.verify_file(&input)?;
    Ok(Some(saved))
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
            return saved.verify_file(&open_regular(&file.path)?);
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
        saved.verify_file(&output)?;
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
