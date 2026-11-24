use super::{Modified, copy_exact, is_regular, metadata_if_present, resume, sync_directory};
use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::thread;
use tempfile::NamedTempFile;

pub(super) const MAX_SMALL_FILE: u64 = 64 * 1024;
const GROUP_SIZE: usize = 16;
const SYNC_WORKERS: usize = 4;

struct VerifiedFile {
    file: File,
    part: PathBuf,
    target: PathBuf,
}

#[derive(Default)]
pub(super) struct Batch(Vec<VerifiedFile>);

impl Batch {
    pub(super) fn add(
        &mut self,
        stage: &Path,
        index: usize,
        target: PathBuf,
        temp: NamedTempFile,
    ) -> Result<()> {
        // Give completed data a stable name so it can be checked and reused
        // even if the process stops before this group is flushed.
        let part = resume::path(stage, index);
        let file = temp.persist(&part).map_err(|error| error.error)?;
        self.0.push(VerifiedFile { file, part, target });
        if self.0.len() == GROUP_SIZE {
            self.flush(stage)?;
        }
        Ok(())
    }

    pub(super) fn flush(&mut self, stage: &Path) -> Result<()> {
        let files = std::mem::take(&mut self.0);
        if files.is_empty() {
            return Ok(());
        }
        if files.len() == 1 {
            files[0].file.sync_all()?;
        } else {
            thread::scope(|scope| -> Result<()> {
                let mut jobs = Vec::new();
                for group in files.chunks(files.len().div_ceil(SYNC_WORKERS)) {
                    jobs.push(thread::Builder::new().spawn_scoped(
                        scope,
                        move || -> io::Result<()> {
                            for item in group {
                                item.file.sync_all()?;
                            }
                            Ok(())
                        },
                    )?);
                }
                for job in jobs {
                    job.join()
                        .map_err(|_| anyhow::anyhow!("File sync worker stopped"))??;
                }
                Ok(())
            })?;
        }

        let mut parents = HashSet::new();
        let moved = (|| -> Result<()> {
            for VerifiedFile { file, part, target } in files {
                drop(file);
                let parent = target.parent().context("Missing staging parent")?;
                fs::rename(part, &target)?;
                parents.insert(parent.to_path_buf());
            }
            Ok(())
        })();
        let synced = (|| -> Result<()> {
            let sync_stage = !parents.is_empty() && !parents.contains(stage);
            for parent in parents {
                sync_directory(&parent)?;
            }
            if sync_stage {
                sync_directory(stage)?;
            }
            Ok(())
        })();
        moved.and(synced)
    }
}

pub(super) fn recover(
    part: &Path,
    target: &Path,
    bytes: u64,
    sha256: &str,
    buffer: &mut [u8],
    mut tick: impl FnMut() -> Result<()>,
) -> Result<bool> {
    let Some(metadata) = metadata_if_present(part)? else {
        return Ok(false);
    };
    if !is_regular(&metadata) || metadata.len() != bytes {
        return Ok(false);
    }
    let mut file = resume::open(part)?;
    let original = file.metadata()?;
    if original.len() != bytes
        || copy_exact(&mut file, &mut io::sink(), bytes, buffer, |_| tick())? != sha256
    {
        return Ok(false);
    }
    let after = file.metadata()?;
    if after.len() != bytes || Modified::from(&after) != Modified::from(&original) {
        return Ok(false);
    }
    file.sync_all()?;
    drop(file);
    fs::rename(part, target)?;
    sync_directory(target.parent().context("Missing staging parent")?)?;
    Ok(true)
}
