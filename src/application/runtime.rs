use std::path::PathBuf;
use std::sync::Arc;

use crate::application::{
    Clock, LocalPrinterProvider, NativePrintSpooler, PrintJobService, PrinterShareService,
    SystemClock, SystemQueueRegistrar, SystemQueueService,
};
use crate::infrastructure::persistence::SqlitePrintStore;
use crate::infrastructure::spool::SpoolDirectory;
use crate::Result;

#[derive(Debug, Clone)]
pub struct PrintRuntimeConfig {
    pub database_url: String,
    pub spool_directory: PathBuf,
}

pub struct PrintRuntimeAdapters {
    pub printers: Arc<dyn LocalPrinterProvider>,
    pub queue_registrar: Arc<dyn SystemQueueRegistrar>,
    pub native_spooler: Arc<dyn NativePrintSpooler>,
    pub clock: Arc<dyn Clock>,
    pub hosting_supported: bool,
}

impl PrintRuntimeAdapters {
    pub fn with_system_clock(
        printers: Arc<dyn LocalPrinterProvider>,
        queue_registrar: Arc<dyn SystemQueueRegistrar>,
        native_spooler: Arc<dyn NativePrintSpooler>,
    ) -> Self {
        Self {
            printers,
            queue_registrar,
            native_spooler,
            clock: Arc::new(SystemClock),
            hosting_supported:
                crate::infrastructure::platform::SystemPrintBackend::hosting_supported(),
        }
    }
}

/// Long-lived Rust service composition root. Tauri owns this handle but the
/// runtime does not depend on any window or frontend lifecycle.
pub struct PrintRuntime {
    pub shares: Arc<PrinterShareService>,
    pub queues: Arc<SystemQueueService>,
    pub jobs: Arc<PrintJobService>,
    pub spool: Arc<SpoolDirectory>,
    store: Arc<SqlitePrintStore>,
}

impl PrintRuntime {
    pub async fn open(config: PrintRuntimeConfig, adapters: PrintRuntimeAdapters) -> Result<Self> {
        let store = Arc::new(SqlitePrintStore::connect(config.database_url).await?);
        let spool = Arc::new(SpoolDirectory::open(config.spool_directory).await?);
        let shares = Arc::new(PrinterShareService::new(
            adapters.printers,
            store.clone(),
            store.clone(),
            adapters.clock.clone(),
            adapters.hosting_supported,
        ));
        let queues = Arc::new(SystemQueueService::new(
            store.clone(),
            adapters.queue_registrar,
            adapters.clock.clone(),
        ));
        let jobs = Arc::new(PrintJobService::new(
            store.clone(),
            store.clone(),
            adapters.native_spooler,
            adapters.clock,
        ));
        Ok(Self {
            shares,
            queues,
            jobs,
            spool,
            store,
        })
    }

    /// Startup reconciliation is intentionally conservative: interrupted
    /// uploads are removed and native jobs are inspected, never resubmitted.
    pub async fn recover(&self) -> Result<RecoverySummary> {
        let removed_partial_documents = self.spool.clear_interrupted_receives().await?;
        let reconciled_jobs = self.jobs.reconcile_active().await?.len();
        Ok(RecoverySummary {
            removed_partial_documents,
            reconciled_jobs,
        })
    }

    pub fn store(&self) -> &SqlitePrintStore {
        &self.store
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoverySummary {
    pub removed_partial_documents: usize,
    pub reconciled_jobs: usize,
}
