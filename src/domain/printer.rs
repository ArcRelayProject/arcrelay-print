use serde::{Deserialize, Serialize};

use super::{DeviceId, LocalPrinterId, PrinterCapabilities, PrinterShareId};
use crate::{PrintError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum PrinterStatus {
    Ready,
    Busy,
    Offline,
    Error,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct LocalPrinter {
    pub id: LocalPrinterId,
    pub display_name: String,
    pub status: PrinterStatus,
    pub connection_kind: String,
    pub is_default: bool,
    pub capabilities: PrinterCapabilities,
}

impl LocalPrinter {
    pub fn validate(&self) -> Result<()> {
        if self.display_name.trim().is_empty() {
            return Err(PrintError::Invalid("printer name is empty".into()));
        }
        self.capabilities.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct PublishedPrinter {
    pub source_device_id: DeviceId,
    pub source_device_name: String,
    pub share_id: PrinterShareId,
    pub display_name: String,
    pub status: PrinterStatus,
    pub capabilities: PrinterCapabilities,
    pub capability_revision: u64,
}
