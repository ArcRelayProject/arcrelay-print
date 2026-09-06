use std::path::Path;

use async_trait::async_trait;

use crate::domain::{
    ClientJobId, LocalPrinter, LocalPrinterId, NativeJobId, PrintJob, PrintJobId, PrinterShare,
    PrinterShareId, PublishedPrinter, QueueBindingId, RemoteQueueBinding, SystemQueueId,
    TimestampMs,
};
use crate::Result;

#[async_trait]
pub trait PrinterShareRepository: Send + Sync {
    async fn find(&self, id: &PrinterShareId) -> Result<Option<PrinterShare>>;
    async fn find_by_local_printer(&self, id: &LocalPrinterId) -> Result<Option<PrinterShare>>;
    async fn list(&self) -> Result<Vec<PrinterShare>>;
    async fn save(&self, share: &PrinterShare) -> Result<()>;
}

#[async_trait]
pub trait QueueBindingRepository: Send + Sync {
    async fn find(&self, id: &QueueBindingId) -> Result<Option<RemoteQueueBinding>>;
    async fn find_remote(
        &self,
        remote_device_id: &crate::domain::DeviceId,
        printer_share_id: &PrinterShareId,
    ) -> Result<Option<RemoteQueueBinding>>;
    async fn list(&self) -> Result<Vec<RemoteQueueBinding>>;
    async fn save(&self, binding: &RemoteQueueBinding) -> Result<()>;
    async fn delete(&self, id: &QueueBindingId) -> Result<()>;
}

#[async_trait]
pub trait PrintJobRepository: Send + Sync {
    async fn find(&self, id: &PrintJobId) -> Result<Option<PrintJob>>;
    async fn find_by_client_job(
        &self,
        source_device_id: &crate::domain::DeviceId,
        id: &ClientJobId,
    ) -> Result<Option<PrintJob>>;
    async fn list_active(&self) -> Result<Vec<PrintJob>>;
    async fn list_recent(&self, limit: usize) -> Result<Vec<PrintJob>>;
    async fn save(&self, job: &PrintJob) -> Result<()>;
}

#[async_trait]
pub trait LocalPrinterProvider: Send + Sync {
    async fn list(&self) -> Result<Vec<LocalPrinter>>;
    async fn find(&self, id: &LocalPrinterId) -> Result<Option<LocalPrinter>>;
}

#[derive(Debug, Clone)]
pub struct SystemQueueSpec {
    pub queue_name: String,
    pub ipp_uri: String,
    pub printer: PublishedPrinter,
}

#[async_trait]
pub trait SystemQueueRegistrar: Send + Sync {
    async fn install(&self, spec: &SystemQueueSpec) -> Result<SystemQueueId>;
    async fn remove(&self, id: &SystemQueueId) -> Result<()>;
    async fn exists(&self, id: &SystemQueueId) -> Result<bool>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeJobStatus {
    Queued,
    Printing,
    Completed,
    Held,
    Cancelled,
    Failed,
    Unknown,
}

#[async_trait]
pub trait NativePrintSpooler: Send + Sync {
    async fn submit(
        &self,
        printer: &LocalPrinterId,
        document: &Path,
        job: &PrintJob,
    ) -> Result<NativeJobId>;
    async fn status(&self, id: &NativeJobId) -> Result<NativeJobStatus>;
    async fn cancel(&self, id: &NativeJobId) -> Result<()>;
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> TimestampMs;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> TimestampMs {
        use std::time::{Duration, SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as i64
    }
}
