use serde::{Deserialize, Serialize};

use super::{
    ClientJobId, ColorMode, DeviceId, DuplexMode, NativeJobId, Orientation, PrintJobId,
    PrinterShareId, TimestampMs,
};
use crate::{PrintError, Result};

pub const MAX_DOCUMENT_BYTES: u64 = 500 * 1024 * 1024;
pub const MAX_COPIES: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrintDocument {
    pub name: String,
    pub media_type: String,
    pub size_bytes: u64,
    pub sha256: [u8; 32],
}

impl PrintDocument {
    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() || self.name.len() > 512 {
            return Err(PrintError::Invalid("invalid document name".into()));
        }
        if !matches!(
            self.media_type.as_str(),
            "application/pdf" | "image/pwg-raster"
        ) {
            return Err(PrintError::Invalid(format!(
                "unsupported print document type: {}",
                self.media_type
            )));
        }
        if self.size_bytes == 0 || self.size_bytes > MAX_DOCUMENT_BYTES {
            return Err(PrintError::Invalid("invalid print document size".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageRange {
    pub first: u32,
    pub last: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrintOptions {
    pub copies: u32,
    pub color_mode: ColorMode,
    pub duplex_mode: DuplexMode,
    pub orientation: Orientation,
    pub media_name: String,
    pub page_ranges: Vec<PageRange>,
    pub collate: bool,
}

impl PrintOptions {
    pub fn validate(&self) -> Result<()> {
        if self.copies == 0 || self.copies > MAX_COPIES {
            return Err(PrintError::Invalid("invalid number of copies".into()));
        }
        if self.media_name.trim().is_empty() {
            return Err(PrintError::Invalid("print media name is empty".into()));
        }
        if self
            .page_ranges
            .iter()
            .any(|range| range.first == 0 || range.first > range.last)
        {
            return Err(PrintError::Invalid("invalid page range".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum PrintJobState {
    Offered,
    Receiving,
    Validating,
    Ready,
    Submitting,
    Queued,
    Printing,
    Completed,
    Held,
    Cancelled,
    Failed,
    Ambiguous,
}

impl PrintJobState {
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrintJob {
    pub id: PrintJobId,
    pub client_job_id: ClientJobId,
    pub source_device_id: DeviceId,
    pub printer_share_id: PrinterShareId,
    pub document: PrintDocument,
    pub options: PrintOptions,
    pub state: PrintJobState,
    pub native_job_id: Option<NativeJobId>,
    pub failure_message: Option<String>,
    pub created_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
}

impl PrintJob {
    pub fn offer(
        client_job_id: ClientJobId,
        source_device_id: DeviceId,
        printer_share_id: PrinterShareId,
        document: PrintDocument,
        options: PrintOptions,
        now_ms: TimestampMs,
    ) -> Result<Self> {
        document.validate()?;
        options.validate()?;
        Ok(Self {
            id: PrintJobId::random(),
            client_job_id,
            source_device_id,
            printer_share_id,
            document,
            options,
            state: PrintJobState::Offered,
            native_job_id: None,
            failure_message: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }

    pub fn begin_receiving(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Receiving, now_ms)
    }

    pub fn document_received(&mut self, digest: [u8; 32], now_ms: TimestampMs) -> Result<()> {
        if self.state != PrintJobState::Receiving {
            return Err(PrintError::InvalidState(
                "document is not being received".into(),
            ));
        }
        if digest != self.document.sha256 {
            return Err(PrintError::Invalid("document digest does not match".into()));
        }
        self.transition(PrintJobState::Validating, now_ms)
    }

    pub fn validated(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Ready, now_ms)
    }

    pub fn queued(&mut self, native_job_id: NativeJobId, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Queued, now_ms)?;
        self.native_job_id = Some(native_job_id);
        self.failure_message = None;
        Ok(())
    }

    pub fn begin_submitting(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Submitting, now_ms)
    }

    pub fn submission_ambiguous(
        &mut self,
        native_job_id: Option<NativeJobId>,
        message: String,
        now_ms: TimestampMs,
    ) -> Result<()> {
        self.transition(PrintJobState::Ambiguous, now_ms)?;
        if native_job_id.is_some() {
            self.native_job_id = native_job_id;
        }
        self.failure_message = Some(message);
        Ok(())
    }

    pub fn printing(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Printing, now_ms)?;
        self.failure_message = None;
        Ok(())
    }

    pub fn complete(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Completed, now_ms)?;
        self.failure_message = None;
        Ok(())
    }

    pub fn hold(&mut self, message: String, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Held, now_ms)?;
        self.failure_message = Some(message);
        Ok(())
    }

    pub fn cancel(&mut self, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Cancelled, now_ms)
    }

    pub fn fail(&mut self, message: String, now_ms: TimestampMs) -> Result<()> {
        self.transition(PrintJobState::Failed, now_ms)?;
        self.failure_message = Some(message);
        Ok(())
    }

    fn transition(&mut self, next: PrintJobState, now_ms: TimestampMs) -> Result<()> {
        use PrintJobState::*;
        let allowed = matches!(
            (self.state, next),
            (Offered, Receiving)
                | (Offered, Cancelled)
                | (Offered, Failed)
                | (Receiving, Validating)
                | (Receiving, Cancelled)
                | (Receiving, Failed)
                | (Validating, Ready)
                | (Validating, Cancelled)
                | (Validating, Failed)
                | (Ready, Submitting)
                | (Ready, Cancelled)
                | (Ready, Failed)
                | (Submitting, Queued)
                | (Submitting, Ambiguous)
                | (Submitting, Cancelled)
                | (Submitting, Failed)
                | (Queued, Ambiguous)
                | (Queued, Printing)
                | (Queued, Completed)
                | (Queued, Held)
                | (Queued, Cancelled)
                | (Queued, Failed)
                | (Printing, Completed)
                | (Printing, Held)
                | (Printing, Cancelled)
                | (Printing, Failed)
                | (Held, Queued)
                | (Held, Printing)
                | (Held, Completed)
                | (Held, Cancelled)
                | (Held, Failed)
                | (Ambiguous, Queued)
                | (Ambiguous, Printing)
                | (Ambiguous, Completed)
                | (Ambiguous, Held)
                | (Ambiguous, Cancelled)
                | (Ambiguous, Failed)
        );
        if self.state == next {
            return Ok(());
        }
        if !allowed || self.state.terminal() {
            return Err(PrintError::InvalidState(format!(
                "cannot transition {:?} to {:?}",
                self.state, next
            )));
        }
        self.state = next;
        self.updated_at_ms = now_ms;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job() -> PrintJob {
        PrintJob::offer(
            ClientJobId::parse("client-job").unwrap(),
            DeviceId::parse("device").unwrap(),
            PrinterShareId::parse("share").unwrap(),
            PrintDocument {
                name: "document.pdf".into(),
                media_type: "application/pdf".into(),
                size_bytes: 10,
                sha256: [7; 32],
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

    #[test]
    fn job_cannot_skip_validation_or_reprint_after_completion() {
        let mut job = job();
        assert!(job
            .queued(NativeJobId::parse("native").unwrap(), 2)
            .is_err());
        job.begin_receiving(2).unwrap();
        job.document_received([7; 32], 3).unwrap();
        job.validated(4).unwrap();
        job.begin_submitting(5).unwrap();
        job.queued(NativeJobId::parse("native").unwrap(), 5)
            .unwrap();
        job.complete(6).unwrap();
        assert!(job.queued(NativeJobId::parse("again").unwrap(), 7).is_err());
    }

    #[test]
    fn digest_mismatch_does_not_advance_job() {
        let mut job = job();
        job.begin_receiving(2).unwrap();
        assert!(job.document_received([8; 32], 3).is_err());
        assert_eq!(job.state, PrintJobState::Receiving);
    }

    #[test]
    fn ambiguous_submission_cannot_return_to_ready_or_submit_again() {
        let mut job = job();
        job.begin_receiving(2).unwrap();
        job.document_received([7; 32], 3).unwrap();
        job.validated(4).unwrap();
        job.begin_submitting(5).unwrap();
        job.submission_ambiguous(None, "inspect system queue".into(), 6)
            .unwrap();

        assert_eq!(job.state, PrintJobState::Ambiguous);
        assert!(job.begin_submitting(7).is_err());
        assert!(job.validated(7).is_err());
    }
}
