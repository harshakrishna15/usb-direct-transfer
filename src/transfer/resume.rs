use super::{
    CHUNK, Modified, copy_hashed, display_path, hex_digest, is_digest, is_regular,
    metadata_if_present, no_follow, open_regular,
};
use anyhow::{Result, ensure};
use ring::digest;
use serde::{Deserialize, Serialize};
use std::fs::{File, Metadata, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

pub(super) const REQUESTS_WITH_CHECKPOINTS: usize = u32::MAX as usize - 1;
pub(super) const MAX_CHECKPOINTS: usize = 8192;
const MAX_PER_FILE: usize = 4096;
const MIN_INTERVAL: u64 = 16 * 1024 * 1024;
const FILE_PREFIX: &str = ".resume-";

pub(super) fn interval(bytes: u64, budget: usize) -> u64 {
    if budget == 0 || bytes <= MIN_INTERVAL {
        return bytes.max(1);
    }
    let checkpoints = budget.min(MAX_PER_FILE);
    let minimum = bytes.div_ceil(checkpoints as u64 + 1).max(MIN_INTERVAL);
    minimum.div_ceil(CHUNK as u64) * CHUNK as u64
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct Checkpoints {
    pub interval: u64,
    pub digests: Vec<String>,
}

impl Checkpoints {
    pub(super) fn validate(&self, bytes: u64, budget: &mut usize) -> Result<()> {
        ensure!(
            self.interval >= MIN_INTERVAL && !self.digests.is_empty(),
            "Invalid transfer checkpoints"
        );
        ensure!(
            self.digests.len() <= (*budget).min(MAX_PER_FILE)
                && self.digests.len() as u64 == bytes.saturating_sub(1) / self.interval
                && self.digests.iter().all(|digest| is_digest(digest)),
            "Invalid transfer checkpoints"
        );
        *budget -= self.digests.len();
        Ok(())
    }

    pub(super) fn offset(&self, checkpoint: usize) -> Result<u64> {
        ensure!(
            checkpoint <= self.digests.len(),
            "Receiver requested an unknown checkpoint"
        );
        Ok(checkpoint as u64 * self.interval)
    }

    pub(super) fn verify_saved(
        &self,
        path: &Path,
        buffer: &mut [u8],
        mut tick: impl FnMut() -> Result<()>,
    ) -> Result<Option<VerifiedPrefix>> {
        let Some(metadata) = metadata_if_present(path)? else {
            return Ok(None);
        };
        ensure!(
            is_regular(&metadata),
            "Expected a regular file at {}",
            display_path(path)
        );
        if metadata.len() < self.interval {
            return Ok(None);
        }
        let mut file = open_regular(path)?;
        let original = file.metadata()?;
        let mut verified = 0;
        let mut hasher = digest::Context::new(&digest::SHA256);
        for expected in self
            .digests
            .iter()
            .take(usize::try_from(original.len() / self.interval).unwrap_or(usize::MAX))
        {
            let mut candidate = hasher.clone();
            copy_hashed(
                &mut file,
                &mut io::sink(),
                self.interval,
                buffer,
                &mut candidate,
                |_| tick(),
            )?;
            if hex_digest(candidate.clone().finish()) != *expected {
                break;
            }
            hasher = candidate;
            verified += 1;
        }
        let prefix = VerifiedPrefix {
            checkpoint: verified,
            hasher,
            length: original.len(),
            modified: Modified::from(&original),
        };
        prefix.require_unchanged(&file.metadata()?)?;
        Ok((verified > 0).then_some(prefix))
    }
}

pub(super) struct VerifiedPrefix {
    pub checkpoint: usize,
    pub hasher: digest::Context,
    length: u64,
    modified: Modified,
}

impl VerifiedPrefix {
    pub(super) fn require_unchanged(&self, metadata: &Metadata) -> Result<()> {
        ensure!(
            metadata.len() == self.length && Modified::from(metadata) == self.modified,
            "Partial file changed after verification. Retry the transfer."
        );
        Ok(())
    }
}

pub(super) fn path(stage: &Path, index: usize) -> PathBuf {
    stage.join(format!("{FILE_PREFIX}{index}"))
}

pub(super) fn is_partial_file(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .and_then(|name| name.strip_prefix(FILE_PREFIX))
        .is_some_and(|index| index.parse::<usize>().is_ok())
}

pub(super) fn open(path: &Path) -> Result<File> {
    if let Some(metadata) = metadata_if_present(path)? {
        ensure!(
            is_regular(&metadata),
            "Expected a regular file at {}",
            display_path(path)
        );
    }
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    no_follow(&mut options);
    let file = options.open(path)?;
    ensure!(
        is_regular(&file.metadata()?),
        "Expected a regular file at {}",
        display_path(path)
    );
    Ok(file)
}
