use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::domain::{PrintJob, PrintJobId};
use crate::{PrintError, Result};

#[derive(Debug, Clone)]
pub struct SpoolDirectory {
    root: PathBuf,
}

impl SpoolDirectory {
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        for name in ["receiving", "ready", "failed"] {
            let directory = root.join(name);
            tokio::fs::create_dir_all(&directory).await?;
            set_private_directory_permissions(&directory).await?;
        }
        Ok(Self { root })
    }

    pub async fn begin(&self, job: &PrintJob) -> Result<SpoolWriter> {
        validate_job_id(&job.id)?;
        let receiving_path = self.root.join("receiving").join(format!("{}.part", job.id));
        let ready_path = self.root.join("ready").join(format!("{}.document", job.id));
        if tokio::fs::try_exists(&ready_path).await? {
            return Err(PrintError::Conflict(format!(
                "document for job {} is already ready",
                job.id
            )));
        }
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&receiving_path)
            .await?;
        set_private_file_permissions(&receiving_path).await?;
        Ok(SpoolWriter {
            file: Some(file),
            receiving_path,
            ready_path,
            expected_size: job.document.size_bytes,
            expected_sha256: job.document.sha256,
            written: 0,
            hasher: Sha256::new(),
        })
    }

    pub fn ready_path(&self, job_id: &PrintJobId) -> Result<PathBuf> {
        validate_job_id(job_id)?;
        Ok(self.root.join("ready").join(format!("{job_id}.document")))
    }

    pub async fn delete_ready(&self, job_id: &PrintJobId) -> Result<()> {
        let path = self.ready_path(job_id)?;
        match tokio::fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    /// Removes incomplete files left by a process crash. Ready documents are
    /// intentionally preserved so recovery can reconcile them with job state.
    pub async fn clear_interrupted_receives(&self) -> Result<usize> {
        let directory = self.root.join("receiving");
        let mut entries = tokio::fs::read_dir(directory).await?;
        let mut removed = 0;
        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_file() {
                tokio::fs::remove_file(entry.path()).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

pub struct SpoolWriter {
    file: Option<tokio::fs::File>,
    receiving_path: PathBuf,
    ready_path: PathBuf,
    expected_size: u64,
    expected_sha256: [u8; 32],
    written: u64,
    hasher: Sha256,
}

impl Drop for SpoolWriter {
    fn drop(&mut self) {
        // Best-effort synchronous cleanup prevents malformed or disconnected
        // uploads from accumulating until the next application restart.
        // Close the file first because Windows does not permit unlinking an
        // open file.
        self.file.take();
        let _ = std::fs::remove_file(&self.receiving_path);
    }
}

impl SpoolWriter {
    pub async fn write_chunk(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Err(PrintError::Invalid("empty print document chunk".into()));
        }
        if offset != self.written {
            return Err(PrintError::Invalid(format!(
                "unexpected print document offset {offset}; expected {}",
                self.written
            )));
        }
        let next = self
            .written
            .checked_add(data.len() as u64)
            .ok_or_else(|| PrintError::Invalid("print document size overflow".into()))?;
        if next > self.expected_size {
            return Err(PrintError::Invalid(
                "print document exceeds declared size".into(),
            ));
        }
        self.file
            .as_mut()
            .ok_or_else(|| PrintError::InvalidState("spool writer is closed".into()))?
            .write_all(data)
            .await?;
        self.hasher.update(data);
        self.written = next;
        Ok(())
    }

    pub async fn finish(mut self) -> Result<PathBuf> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| PrintError::InvalidState("spool writer is closed".into()))?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        if self.written != self.expected_size {
            self.discard().await;
            return Err(PrintError::Invalid(format!(
                "received {} bytes; expected {}",
                self.written, self.expected_size
            )));
        }
        let actual: [u8; 32] = self.hasher.clone().finalize().into();
        if actual != self.expected_sha256 {
            self.discard().await;
            return Err(PrintError::Invalid(
                "print document digest does not match".into(),
            ));
        }
        tokio::fs::rename(&self.receiving_path, &self.ready_path).await?;
        Ok(self.ready_path.clone())
    }

    async fn discard(&self) {
        let _ = tokio::fs::remove_file(&self.receiving_path).await;
    }
}

fn validate_job_id(id: &PrintJobId) -> Result<()> {
    uuid::Uuid::parse_str(id.as_str())
        .map(|_| ())
        .map_err(|_| PrintError::Invalid("print job id is not a UUID".into()))
}

#[cfg(unix)]
async fn set_private_directory_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).await?;
    Ok(())
}

#[cfg(target_os = "windows")]
async fn set_private_directory_permissions(path: &Path) -> Result<()> {
    harden_windows_path(path, true).await
}

#[cfg(not(any(unix, target_os = "windows")))]
async fn set_private_directory_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
async fn set_private_file_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await?;
    Ok(())
}

#[cfg(target_os = "windows")]
async fn set_private_file_permissions(path: &Path) -> Result<()> {
    harden_windows_path(path, false).await
}

#[cfg(not(any(unix, target_os = "windows")))]
async fn set_private_file_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(target_os = "windows")]
async fn harden_windows_path(path: &Path, directory: bool) -> Result<()> {
    let username = std::env::var("USERNAME")
        .map_err(|_| PrintError::Backend("Windows USERNAME is unavailable".into()))?;
    let user_grant = if directory {
        format!("{username}:(OI)(CI)F")
    } else {
        format!("{username}:F")
    };
    let system_grant = if directory {
        "SYSTEM:(OI)(CI)F"
    } else {
        "SYSTEM:F"
    };
    let mut command = tokio::process::Command::new("icacls.exe");
    command
        .arg(path)
        .args(["/inheritance:r", "/grant:r", &user_grant, system_grant])
        .creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    let output = command.output().await?;
    if !output.status.success() {
        return Err(PrintError::Backend(format!(
            "could not secure Windows print spool path: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        ClientJobId, ColorMode, DeviceId, DuplexMode, Orientation, PrintDocument, PrintJob,
        PrintOptions, PrinterShareId,
    };

    fn job(bytes: &[u8]) -> PrintJob {
        PrintJob::offer(
            ClientJobId::parse("client-job").unwrap(),
            DeviceId::parse("device").unwrap(),
            PrinterShareId::parse("share").unwrap(),
            PrintDocument {
                name: "test.pdf".into(),
                media_type: "application/pdf".into(),
                size_bytes: bytes.len() as u64,
                sha256: Sha256::digest(bytes).into(),
            },
            PrintOptions {
                copies: 1,
                color_mode: ColorMode::Monochrome,
                duplex_mode: DuplexMode::OneSided,
                orientation: Orientation::Portrait,
                media_name: "iso_a4_210x297mm".into(),
                page_ranges: vec![],
                collate: false,
            },
            1,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn chunks_are_ordered_verified_and_atomically_finalized() {
        let temporary = tempfile::tempdir().unwrap();
        let spool = SpoolDirectory::open(temporary.path()).await.unwrap();
        let bytes = b"%PDF-test";
        let job = job(bytes);
        let mut writer = spool.begin(&job).await.unwrap();
        writer.write_chunk(0, &bytes[..4]).await.unwrap();
        assert!(writer.write_chunk(0, &bytes[4..]).await.is_err());
        writer.write_chunk(4, &bytes[4..]).await.unwrap();
        let ready = writer.finish().await.unwrap();
        assert_eq!(tokio::fs::read(ready).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn digest_mismatch_removes_staging_file() {
        let temporary = tempfile::tempdir().unwrap();
        let spool = SpoolDirectory::open(temporary.path()).await.unwrap();
        let mut job = job(b"original");
        job.document.sha256 = [0; 32];
        let mut writer = spool.begin(&job).await.unwrap();
        writer.write_chunk(0, b"original").await.unwrap();
        assert!(writer.finish().await.is_err());
        assert_eq!(spool.clear_interrupted_receives().await.unwrap(), 0);
    }
}
