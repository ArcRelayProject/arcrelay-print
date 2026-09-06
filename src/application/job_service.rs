use std::path::Path;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::application::{
    Clock, NativeJobStatus, NativePrintSpooler, PrintJobRepository, PrinterShareRepository,
};
use crate::domain::{
    ClientJobId, DeviceId, PrintDocument, PrintDomainEvent, PrintJob, PrintJobId, PrintOptions,
    PrinterShareId, ShareState,
};
use crate::{PrintError, Result};

const MAX_ACTIVE_JOBS_PER_DEVICE: usize = 8;
const ACTIVE_JOB_STALE_AFTER_MS: i64 = 30 * 60 * 1_000;

pub struct PrintJobService {
    shares: Arc<dyn PrinterShareRepository>,
    jobs: Arc<dyn PrintJobRepository>,
    spooler: Arc<dyn NativePrintSpooler>,
    clock: Arc<dyn Clock>,
    events: broadcast::Sender<PrintDomainEvent>,
}

impl PrintJobService {
    pub fn new(
        shares: Arc<dyn PrinterShareRepository>,
        jobs: Arc<dyn PrintJobRepository>,
        spooler: Arc<dyn NativePrintSpooler>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            shares,
            jobs,
            spooler,
            clock,
            events,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<PrintDomainEvent> {
        self.events.subscribe()
    }

    pub async fn offer(
        &self,
        client_job_id: ClientJobId,
        source_device_id: DeviceId,
        printer_share_id: PrinterShareId,
        document: PrintDocument,
        options: PrintOptions,
    ) -> Result<PrintJob> {
        if let Some(existing) = self
            .jobs
            .find_by_client_job(&source_device_id, &client_job_id)
            .await?
        {
            if existing.printer_share_id == printer_share_id
                && existing.document == document
                && existing.options == options
            {
                return Ok(existing);
            }
            return Err(PrintError::Conflict(
                "client print job id was reused with different content".into(),
            ));
        }
        let now = self.clock.now_ms();
        let active_for_source = self
            .jobs
            .list_active()
            .await?
            .into_iter()
            .filter(|job| {
                job.source_device_id == source_device_id
                    && now.saturating_sub(job.updated_at_ms) < ACTIVE_JOB_STALE_AFTER_MS
            })
            .count();
        if active_for_source >= MAX_ACTIVE_JOBS_PER_DEVICE {
            return Err(PrintError::Conflict(format!(
                "device already has {MAX_ACTIVE_JOBS_PER_DEVICE} active print jobs"
            )));
        }
        let share = self
            .shares
            .find(&printer_share_id)
            .await?
            .ok_or_else(|| PrintError::NotFound(printer_share_id.to_string()))?;
        if share.state != ShareState::Published {
            return Err(PrintError::InvalidState(
                "printer is not publicly shared".into(),
            ));
        }
        if !share.capabilities.color_modes.contains(&options.color_mode)
            || !share
                .capabilities
                .duplex_modes
                .contains(&options.duplex_mode)
            || !share
                .capabilities
                .media_sizes
                .iter()
                .any(|media| media.name == options.media_name)
            || options.copies > share.capabilities.max_copies
            || (!options.page_ranges.is_empty() && !share.capabilities.supports_page_ranges)
            || (options.collate && !share.capabilities.supports_collation)
            || !share
                .capabilities
                .accepted_document_types
                .contains(&document.media_type)
        {
            return Err(PrintError::Invalid(
                "print options are not supported by this printer".into(),
            ));
        }
        let job = PrintJob::offer(
            client_job_id,
            source_device_id,
            printer_share_id,
            document,
            options,
            now,
        )?;
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn begin_receiving(&self, id: &PrintJobId) -> Result<PrintJob> {
        let mut job = self.require_job(id).await?;
        job.begin_receiving(self.clock.now_ms())?;
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn document_received(&self, id: &PrintJobId, digest: [u8; 32]) -> Result<PrintJob> {
        let mut job = self.require_job(id).await?;
        job.document_received(digest, self.clock.now_ms())?;
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn mark_valid(&self, id: &PrintJobId) -> Result<PrintJob> {
        let mut job = self.require_job(id).await?;
        job.validated(self.clock.now_ms())?;
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn submit(&self, id: &PrintJobId, document_path: &Path) -> Result<PrintJob> {
        let mut job = self.require_job(id).await?;
        let share = self
            .shares
            .find(&job.printer_share_id)
            .await?
            .ok_or_else(|| PrintError::NotFound(job.printer_share_id.to_string()))?;
        if share.state != ShareState::Published {
            return Err(PrintError::InvalidState(
                "printer share was stopped before submission".into(),
            ));
        }
        job.begin_submitting(self.clock.now_ms())?;
        self.persist(&job).await?;

        let native_id = match self
            .spooler
            .submit(&share.local_printer_id, document_path, &job)
            .await
        {
            Ok(native_id) => native_id,
            Err(error) => {
                let message = format!(
                    "native spooler submission result is uncertain; inspect the system queue before retrying: {error}"
                );
                job.submission_ambiguous(None, message.clone(), self.clock.now_ms())?;
                self.persist(&job).await?;
                return Err(PrintError::Backend(message));
            }
        };
        job.queued(native_id.clone(), self.clock.now_ms())?;
        if let Err(save_error) = self.persist(&job).await {
            let message = format!(
                "native job {native_id} was submitted but its queued state could not be saved: {save_error}"
            );
            job.submission_ambiguous(Some(native_id), message.clone(), self.clock.now_ms())?;
            if let Err(ambiguous_save_error) = self.persist(&job).await {
                return Err(PrintError::Persistence(format!(
                    "{message}; ambiguous state also could not be saved: {ambiguous_save_error}"
                )));
            }
            return Err(PrintError::Backend(message));
        }
        Ok(job)
    }

    pub async fn cancel(&self, id: &PrintJobId) -> Result<PrintJob> {
        let mut job = self.require_job(id).await?;
        if let Some(native_id) = &job.native_job_id {
            self.spooler.cancel(native_id).await?;
        }
        job.cancel(self.clock.now_ms())?;
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn get_owned(&self, id: &PrintJobId, source: &DeviceId) -> Result<PrintJob> {
        let job = self.require_job(id).await?;
        if &job.source_device_id != source {
            return Err(PrintError::NotFound(id.to_string()));
        }
        Ok(job)
    }

    pub async fn refresh_owned(&self, id: &PrintJobId, source: &DeviceId) -> Result<PrintJob> {
        let mut job = self.get_owned(id, source).await?;
        use crate::domain::PrintJobState;
        if !matches!(
            job.state,
            PrintJobState::Queued
                | PrintJobState::Printing
                | PrintJobState::Held
                | PrintJobState::Ambiguous
        ) {
            return Ok(job);
        }
        let Some(native_id) = job.native_job_id.clone() else {
            if job.state == PrintJobState::Ambiguous {
                return Ok(job);
            }
            job.hold(
                "native print job id is unavailable".into(),
                self.clock.now_ms(),
            )?;
            self.persist(&job).await?;
            return Ok(job);
        };
        match self.spooler.status(&native_id).await? {
            NativeJobStatus::Queued => {
                if matches!(job.state, PrintJobState::Held | PrintJobState::Ambiguous) {
                    job.queued(native_id, self.clock.now_ms())?;
                }
            }
            NativeJobStatus::Printing => job.printing(self.clock.now_ms())?,
            NativeJobStatus::Completed => job.complete(self.clock.now_ms())?,
            NativeJobStatus::Held | NativeJobStatus::Unknown => job.hold(
                "native print status requires inspection".into(),
                self.clock.now_ms(),
            )?,
            NativeJobStatus::Cancelled => job.cancel(self.clock.now_ms())?,
            NativeJobStatus::Failed => {
                job.fail("native print job failed".into(), self.clock.now_ms())?
            }
        }
        self.persist(&job).await?;
        Ok(job)
    }

    pub async fn cancel_owned(&self, id: &PrintJobId, source: &DeviceId) -> Result<PrintJob> {
        self.get_owned(id, source).await?;
        self.cancel(id).await
    }

    pub async fn list_recent(&self, limit: usize, refresh_active: bool) -> Result<Vec<PrintJob>> {
        let mut jobs = self.jobs.list_recent(limit).await?;
        if refresh_active {
            for job in &mut jobs {
                use crate::domain::PrintJobState;
                if matches!(
                    job.state,
                    PrintJobState::Queued
                        | PrintJobState::Printing
                        | PrintJobState::Held
                        | PrintJobState::Ambiguous
                ) {
                    if let Ok(refreshed) = self.refresh_owned(&job.id, &job.source_device_id).await
                    {
                        *job = refreshed;
                    }
                }
            }
        }
        Ok(jobs)
    }

    /// Reconciles jobs after startup without ever resubmitting a document.
    /// Ambiguous native states are held for user inspection instead of risking
    /// duplicate physical output.
    pub async fn reconcile_active(&self) -> Result<Vec<PrintJob>> {
        let mut reconciled = Vec::new();
        for mut job in self.jobs.list_active().await? {
            use crate::domain::PrintJobState;
            match job.state {
                PrintJobState::Receiving | PrintJobState::Validating => {
                    job.fail(
                        "ArcRelay restarted before the document was ready".into(),
                        self.clock.now_ms(),
                    )?;
                }
                PrintJobState::Submitting => {
                    job.submission_ambiguous(
                        None,
                        "ArcRelay restarted while submitting; inspect the system queue before retrying"
                            .into(),
                        self.clock.now_ms(),
                    )?;
                }
                PrintJobState::Queued
                | PrintJobState::Printing
                | PrintJobState::Held
                | PrintJobState::Ambiguous => {
                    let Some(native_id) = &job.native_job_id else {
                        if job.state != PrintJobState::Ambiguous {
                            job.hold(
                                "native print job id is unavailable".into(),
                                self.clock.now_ms(),
                            )?;
                        }
                        self.persist(&job).await?;
                        reconciled.push(job);
                        continue;
                    };
                    match self.spooler.status(native_id).await? {
                        NativeJobStatus::Queued => {
                            if job.state == PrintJobState::Ambiguous {
                                job.queued(native_id.clone(), self.clock.now_ms())?;
                            }
                        }
                        NativeJobStatus::Printing => job.printing(self.clock.now_ms())?,
                        NativeJobStatus::Completed => job.complete(self.clock.now_ms())?,
                        NativeJobStatus::Held | NativeJobStatus::Unknown => job.hold(
                            "native print status requires inspection".into(),
                            self.clock.now_ms(),
                        )?,
                        NativeJobStatus::Cancelled => job.cancel(self.clock.now_ms())?,
                        NativeJobStatus::Failed => {
                            job.fail("native print job failed".into(), self.clock.now_ms())?
                        }
                    }
                }
                PrintJobState::Offered | PrintJobState::Ready => {}
                PrintJobState::Completed | PrintJobState::Cancelled | PrintJobState::Failed => {
                    continue
                }
            }
            self.persist(&job).await?;
            reconciled.push(job);
        }
        Ok(reconciled)
    }

    async fn require_job(&self, id: &PrintJobId) -> Result<PrintJob> {
        self.jobs
            .find(id)
            .await?
            .ok_or_else(|| PrintError::NotFound(id.to_string()))
    }

    async fn persist(&self, job: &PrintJob) -> Result<()> {
        self.jobs.save(job).await?;
        let _ = self.events.send(PrintDomainEvent::JobStateChanged {
            job_id: job.id.clone(),
            state: job.state,
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::*;
    use crate::application::{NativeJobStatus, PrintJobRepository, PrinterShareRepository};
    use crate::domain::{
        ColorMode, DuplexMode, LocalPrinter, LocalPrinterId, MediaSize, NativeJobId, Orientation,
        PrintJobState, PrintResolution, PrinterCapabilities, PrinterShare, PrinterStatus,
    };

    #[derive(Default)]
    struct TestStore {
        shares: Mutex<Vec<PrinterShare>>,
        jobs: Mutex<HashMap<String, PrintJob>>,
        fail_next_queued_save: AtomicBool,
    }

    #[async_trait]
    impl PrinterShareRepository for TestStore {
        async fn find(&self, id: &PrinterShareId) -> Result<Option<PrinterShare>> {
            Ok(self
                .shares
                .lock()
                .unwrap()
                .iter()
                .find(|share| &share.id == id)
                .cloned())
        }

        async fn find_by_local_printer(&self, id: &LocalPrinterId) -> Result<Option<PrinterShare>> {
            Ok(self
                .shares
                .lock()
                .unwrap()
                .iter()
                .find(|share| &share.local_printer_id == id)
                .cloned())
        }

        async fn list(&self) -> Result<Vec<PrinterShare>> {
            Ok(self.shares.lock().unwrap().clone())
        }

        async fn save(&self, share: &PrinterShare) -> Result<()> {
            let mut shares = self.shares.lock().unwrap();
            if let Some(existing) = shares.iter_mut().find(|existing| existing.id == share.id) {
                *existing = share.clone();
            } else {
                shares.push(share.clone());
            }
            Ok(())
        }
    }

    #[async_trait]
    impl PrintJobRepository for TestStore {
        async fn find(&self, id: &PrintJobId) -> Result<Option<PrintJob>> {
            Ok(self.jobs.lock().unwrap().get(id.as_str()).cloned())
        }

        async fn find_by_client_job(
            &self,
            source_device_id: &DeviceId,
            id: &ClientJobId,
        ) -> Result<Option<PrintJob>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .values()
                .find(|job| &job.source_device_id == source_device_id && &job.client_job_id == id)
                .cloned())
        }

        async fn list_active(&self) -> Result<Vec<PrintJob>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .values()
                .filter(|job| !job.state.terminal())
                .cloned()
                .collect())
        }

        async fn list_recent(&self, limit: usize) -> Result<Vec<PrintJob>> {
            Ok(self
                .jobs
                .lock()
                .unwrap()
                .values()
                .take(limit)
                .cloned()
                .collect())
        }

        async fn save(&self, job: &PrintJob) -> Result<()> {
            if job.state == PrintJobState::Queued
                && self.fail_next_queued_save.swap(false, Ordering::SeqCst)
            {
                return Err(PrintError::Persistence(
                    "injected queued save failure".into(),
                ));
            }
            self.jobs
                .lock()
                .unwrap()
                .insert(job.id.to_string(), job.clone());
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestSpooler {
        submissions: AtomicUsize,
    }

    #[async_trait]
    impl NativePrintSpooler for TestSpooler {
        async fn submit(&self, _: &LocalPrinterId, _: &Path, _: &PrintJob) -> Result<NativeJobId> {
            self.submissions.fetch_add(1, Ordering::SeqCst);
            NativeJobId::parse("native-42")
        }

        async fn status(&self, _: &NativeJobId) -> Result<NativeJobStatus> {
            Ok(NativeJobStatus::Queued)
        }

        async fn cancel(&self, _: &NativeJobId) -> Result<()> {
            Ok(())
        }
    }

    struct TestClock;

    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            10
        }
    }

    fn capabilities() -> PrinterCapabilities {
        PrinterCapabilities {
            media_sizes: vec![MediaSize::new("iso_a4_210x297mm", 210_000, 297_000).unwrap()],
            color_modes: vec![ColorMode::Monochrome],
            duplex_modes: vec![DuplexMode::OneSided],
            resolutions: vec![PrintResolution {
                horizontal_dpi: 300,
                vertical_dpi: 300,
            }],
            max_copies: 1,
            supports_page_ranges: true,
            supports_collation: false,
            accepted_document_types: vec!["application/pdf".into()],
        }
    }

    #[tokio::test]
    async fn successful_native_submit_with_failed_queued_save_is_never_resubmitted() {
        let store = Arc::new(TestStore::default());
        let spooler = Arc::new(TestSpooler::default());
        let printer = LocalPrinter {
            id: LocalPrinterId::parse("printer").unwrap(),
            display_name: "Test Printer".into(),
            status: PrinterStatus::Ready,
            connection_kind: "test".into(),
            is_default: true,
            capabilities: capabilities(),
        };
        store
            .shares
            .lock()
            .unwrap()
            .push(PrinterShare::publish(printer, 1).unwrap());
        let share_id = store.shares.lock().unwrap()[0].id.clone();
        let service = PrintJobService::new(
            store.clone(),
            store.clone(),
            spooler.clone(),
            Arc::new(TestClock),
        );
        let mut events = service.subscribe();
        let client_job_id = ClientJobId::parse("client-job").unwrap();
        let source_device_id = DeviceId::parse("source-device").unwrap();
        let document = PrintDocument {
            name: "document.pdf".into(),
            media_type: "application/pdf".into(),
            size_bytes: 4,
            sha256: [7; 32],
        };
        let options = PrintOptions {
            copies: 1,
            color_mode: ColorMode::Monochrome,
            duplex_mode: DuplexMode::OneSided,
            orientation: Orientation::Portrait,
            media_name: "iso_a4_210x297mm".into(),
            page_ranges: vec![],
            collate: false,
        };
        let job = service
            .offer(
                client_job_id.clone(),
                source_device_id,
                share_id,
                document,
                options,
            )
            .await
            .unwrap();
        assert_eq!(
            events.recv().await.unwrap(),
            PrintDomainEvent::JobStateChanged {
                job_id: job.id.clone(),
                state: PrintJobState::Offered,
            }
        );
        service.begin_receiving(&job.id).await.unwrap();
        service.document_received(&job.id, [7; 32]).await.unwrap();
        service.mark_valid(&job.id).await.unwrap();
        store.fail_next_queued_save.store(true, Ordering::SeqCst);

        assert!(service
            .submit(&job.id, Path::new("ignored.pdf"))
            .await
            .is_err());
        let persisted = PrintJobRepository::find(store.as_ref(), &job.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.state, PrintJobState::Ambiguous);
        assert_eq!(
            persisted.native_job_id.as_ref().unwrap().as_str(),
            "native-42"
        );
        assert_eq!(spooler.submissions.load(Ordering::SeqCst), 1);

        assert!(service
            .submit(&job.id, Path::new("ignored.pdf"))
            .await
            .is_err());
        assert_eq!(spooler.submissions.load(Ordering::SeqCst), 1);
    }
}
