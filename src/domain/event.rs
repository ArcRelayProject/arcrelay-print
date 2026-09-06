use serde::{Deserialize, Serialize};

use super::{PrintJobId, PrintJobState, PrinterShareId, QueueBindingId, ShareState};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type", content = "payload")]
pub enum PrintDomainEvent {
    ShareStateChanged {
        share_id: PrinterShareId,
        state: ShareState,
    },
    QueueBindingChanged {
        binding_id: QueueBindingId,
    },
    JobStateChanged {
        job_id: PrintJobId,
        state: PrintJobState,
    },
}
