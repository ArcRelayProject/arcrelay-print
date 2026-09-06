use std::collections::HashSet;
use std::sync::Arc;

use crate::application::{
    Clock, LocalPrinterProvider, PrinterShareRepository, QueueBindingRepository,
};
use crate::domain::{
    is_managed_remote_queue_name, DeviceId, LocalPrinter, LocalPrinterId, PrinterShare,
    PrinterShareId, PrinterStatus, PublishedPrinter, ShareState,
};
use crate::{PrintError, Result};

pub struct PrinterShareService {
    printers: Arc<dyn LocalPrinterProvider>,
    shares: Arc<dyn PrinterShareRepository>,
    remote_queues: Arc<dyn QueueBindingRepository>,
    clock: Arc<dyn Clock>,
    hosting_supported: bool,
}

impl PrinterShareService {
    pub fn new(
        printers: Arc<dyn LocalPrinterProvider>,
        shares: Arc<dyn PrinterShareRepository>,
        remote_queues: Arc<dyn QueueBindingRepository>,
        clock: Arc<dyn Clock>,
        hosting_supported: bool,
    ) -> Self {
        Self {
            printers,
            shares,
            remote_queues,
            clock,
            hosting_supported,
        }
    }

    #[must_use]
    pub fn hosting_supported(&self) -> bool {
        self.hosting_supported
    }

    pub async fn publish(&self, local_printer_id: &LocalPrinterId) -> Result<PrinterShare> {
        if !self.hosting_supported {
            return Err(PrintError::Backend(
                "sharing local printers is not supported on this operating system yet".into(),
            ));
        }
        let remote_queue_ids = self.remote_queue_ids().await?;
        if is_remote_queue(local_printer_id, &remote_queue_ids) {
            return Err(PrintError::Invalid(
                "ArcRelay-installed remote printers cannot be shared again".into(),
            ));
        }
        let now = self.clock.now_ms();
        let printer = self
            .printers
            .find(local_printer_id)
            .await?
            .ok_or_else(|| PrintError::NotFound(local_printer_id.to_string()))?;
        let mut share = match self.shares.find_by_local_printer(local_printer_id).await? {
            Some(mut share) => {
                share.resume(now)?;
                share.update_capabilities(printer.display_name, printer.capabilities, now)?;
                share
            }
            None => PrinterShare::publish(printer, now)?,
        };
        // The public-LAN product policy is explicit in the aggregate. Keeping
        // this assignment here makes a future private mode an application choice.
        share.scope = crate::domain::ShareScope::PairedDevices;
        self.shares.save(&share).await?;
        Ok(share)
    }

    pub async fn suspend(&self, id: &PrinterShareId) -> Result<PrinterShare> {
        let mut share = self
            .shares
            .find(id)
            .await?
            .ok_or_else(|| PrintError::NotFound(id.to_string()))?;
        share.suspend(self.clock.now_ms())?;
        self.shares.save(&share).await?;
        Ok(share)
    }

    pub async fn list(&self) -> Result<Vec<PrinterShare>> {
        let remote_queue_ids = self.remote_queue_ids().await?;
        Ok(self
            .shares
            .list()
            .await?
            .into_iter()
            .filter(|share| !is_remote_queue(&share.local_printer_id, &remote_queue_ids))
            .collect())
    }

    pub async fn list_local_printers(&self) -> Result<Vec<LocalPrinter>> {
        let remote_queue_ids = self.remote_queue_ids().await?;
        Ok(self
            .printers
            .list()
            .await?
            .into_iter()
            .filter(|printer| !is_remote_queue(&printer.id, &remote_queue_ids))
            .collect())
    }

    pub async fn list_published(
        &self,
        source_device_id: &DeviceId,
        source_device_name: &str,
    ) -> Result<Vec<PublishedPrinter>> {
        if !self.hosting_supported {
            return Ok(Vec::new());
        }
        let remote_queue_ids = self.remote_queue_ids().await?;
        let local = self
            .printers
            .list()
            .await?
            .into_iter()
            .filter(|printer| !is_remote_queue(&printer.id, &remote_queue_ids))
            .map(|printer| (printer.id, printer.status))
            .collect::<std::collections::HashMap<_, _>>();
        Ok(self
            .shares
            .list()
            .await?
            .into_iter()
            .filter(|share| {
                share.state == ShareState::Published
                    && !is_remote_queue(&share.local_printer_id, &remote_queue_ids)
            })
            .map(|share| PublishedPrinter {
                source_device_id: source_device_id.clone(),
                source_device_name: source_device_name.to_owned(),
                status: local
                    .get(&share.local_printer_id)
                    .copied()
                    .unwrap_or(PrinterStatus::Offline),
                share_id: share.id,
                display_name: share.display_name,
                capabilities: share.capabilities,
                capability_revision: share.capability_revision,
            })
            .collect())
    }

    async fn remote_queue_ids(&self) -> Result<HashSet<String>> {
        let mut ids = HashSet::new();
        for binding in self.remote_queues.list().await? {
            ids.insert(binding.system_queue_name);
            if let Some(system_id) = binding.system_queue_id {
                ids.insert(system_id.to_string());
            }
        }
        Ok(ids)
    }
}

fn is_remote_queue(id: &LocalPrinterId, remote_queue_ids: &HashSet<String>) -> bool {
    is_managed_remote_queue_name(id.as_str()) || remote_queue_ids.contains(id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_queue_is_recognized_by_reserved_name_or_persisted_system_id() {
        let remote_queue_ids = HashSet::from(["system-printer-id".to_string()]);

        assert!(is_remote_queue(
            &LocalPrinterId::parse("ArcRelay_Office_ab12cd34").unwrap(),
            &HashSet::new()
        ));
        assert!(is_remote_queue(
            &LocalPrinterId::parse("system-printer-id").unwrap(),
            &remote_queue_ids
        ));
        assert!(!is_remote_queue(
            &LocalPrinterId::parse("local-office-printer").unwrap(),
            &remote_queue_ids
        ));
    }
}
