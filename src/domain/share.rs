use serde::{Deserialize, Serialize};

use super::{LocalPrinter, LocalPrinterId, PrinterCapabilities, PrinterShareId, TimestampMs};
use crate::{PrintError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum ShareScope {
    #[serde(alias = "lanPublic")]
    PairedDevices,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum ShareState {
    Published,
    Suspended,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct PrinterShare {
    pub id: PrinterShareId,
    pub local_printer_id: LocalPrinterId,
    pub display_name: String,
    pub scope: ShareScope,
    pub state: ShareState,
    pub capabilities: PrinterCapabilities,
    pub capability_revision: u64,
    pub created_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
}

impl PrinterShare {
    pub fn publish(printer: LocalPrinter, now_ms: TimestampMs) -> Result<Self> {
        printer.validate()?;
        Ok(Self {
            id: PrinterShareId::random(),
            local_printer_id: printer.id,
            display_name: printer.display_name,
            scope: ShareScope::PairedDevices,
            state: ShareState::Published,
            capabilities: printer.capabilities,
            capability_revision: 1,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }

    pub fn resume(&mut self, now_ms: TimestampMs) -> Result<()> {
        match self.state {
            ShareState::Suspended => {
                self.state = ShareState::Published;
                self.updated_at_ms = now_ms;
                Ok(())
            }
            ShareState::Published => Ok(()),
            ShareState::Removed => Err(PrintError::InvalidState(
                "a removed printer share cannot be resumed".into(),
            )),
        }
    }

    pub fn suspend(&mut self, now_ms: TimestampMs) -> Result<()> {
        match self.state {
            ShareState::Published => {
                self.state = ShareState::Suspended;
                self.updated_at_ms = now_ms;
                Ok(())
            }
            ShareState::Suspended => Ok(()),
            ShareState::Removed => Err(PrintError::InvalidState(
                "a removed printer share cannot be suspended".into(),
            )),
        }
    }

    pub fn remove(&mut self, now_ms: TimestampMs) {
        self.state = ShareState::Removed;
        self.updated_at_ms = now_ms;
    }

    pub fn update_capabilities(
        &mut self,
        display_name: String,
        capabilities: PrinterCapabilities,
        now_ms: TimestampMs,
    ) -> Result<bool> {
        capabilities.validate()?;
        if display_name.trim().is_empty() {
            return Err(PrintError::Invalid("printer name is empty".into()));
        }
        let changed = self.display_name != display_name || self.capabilities != capabilities;
        if changed {
            self.display_name = display_name;
            self.capabilities = capabilities;
            self.capability_revision = self.capability_revision.saturating_add(1);
            self.updated_at_ms = now_ms;
        }
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::ShareScope;

    #[test]
    fn legacy_lan_public_scope_migrates_to_paired_devices() {
        let scope: ShareScope = serde_json::from_str(r#""lanPublic""#).unwrap();
        assert_eq!(scope, ShareScope::PairedDevices);
        assert_eq!(serde_json::to_string(&scope).unwrap(), r#""pairedDevices""#);
    }
}
