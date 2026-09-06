use serde::{Deserialize, Serialize};

use super::{DeviceId, PrinterShareId, QueueBindingId, SystemQueueId, TimestampMs};
use crate::{PrintError, Result};

pub const REMOTE_QUEUE_PREFIX: &str = "ArcRelay_";

#[must_use]
pub fn is_managed_remote_queue_name(value: &str) -> bool {
    value.starts_with(REMOTE_QUEUE_PREFIX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub enum QueueBindingState {
    Installing,
    Ready,
    SourceOffline,
    Removing,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
pub struct RemoteQueueBinding {
    pub id: QueueBindingId,
    pub remote_device_id: DeviceId,
    pub printer_share_id: PrinterShareId,
    pub system_queue_name: String,
    pub system_queue_id: Option<SystemQueueId>,
    pub state: QueueBindingState,
    pub auto_reconnect: bool,
    pub failure_message: Option<String>,
    pub created_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
}

impl RemoteQueueBinding {
    pub fn installing(
        remote_device_id: DeviceId,
        printer_share_id: PrinterShareId,
        system_queue_name: String,
        now_ms: TimestampMs,
    ) -> Result<Self> {
        if system_queue_name.trim().is_empty() {
            return Err(PrintError::Invalid("system queue name is empty".into()));
        }
        Ok(Self {
            id: QueueBindingId::random(),
            remote_device_id,
            printer_share_id,
            system_queue_name,
            system_queue_id: None,
            state: QueueBindingState::Installing,
            auto_reconnect: true,
            failure_message: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }

    pub fn installed(&mut self, queue_id: SystemQueueId, now_ms: TimestampMs) -> Result<()> {
        if self.state != QueueBindingState::Installing {
            return Err(PrintError::InvalidState(
                "only an installing queue can become ready".into(),
            ));
        }
        self.system_queue_id = Some(queue_id);
        self.state = QueueBindingState::Ready;
        self.failure_message = None;
        self.updated_at_ms = now_ms;
        Ok(())
    }

    pub fn retry_installing(&mut self, now_ms: TimestampMs) -> Result<()> {
        if self.state != QueueBindingState::Failed {
            return Err(PrintError::InvalidState(
                "only a failed queue can retry installation".into(),
            ));
        }
        self.system_queue_id = None;
        self.state = QueueBindingState::Installing;
        self.failure_message = None;
        self.updated_at_ms = now_ms;
        Ok(())
    }

    pub fn source_offline(&mut self, now_ms: TimestampMs) {
        if matches!(
            self.state,
            QueueBindingState::Ready | QueueBindingState::SourceOffline
        ) {
            self.state = QueueBindingState::SourceOffline;
            self.updated_at_ms = now_ms;
        }
    }

    pub fn source_online(&mut self, now_ms: TimestampMs) {
        if self.state == QueueBindingState::SourceOffline {
            self.state = QueueBindingState::Ready;
            self.updated_at_ms = now_ms;
        }
    }

    pub fn begin_removal(&mut self, now_ms: TimestampMs) -> Result<()> {
        if self.state == QueueBindingState::Removing {
            return Ok(());
        }
        if self.system_queue_id.is_none() && self.state != QueueBindingState::Failed {
            return Err(PrintError::InvalidState(
                "queue has not been installed".into(),
            ));
        }
        self.state = QueueBindingState::Removing;
        self.updated_at_ms = now_ms;
        Ok(())
    }

    pub fn fail(&mut self, message: String, now_ms: TimestampMs) {
        self.state = QueueBindingState::Failed;
        self.failure_message = Some(message);
        self.updated_at_ms = now_ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_reserved_arcrelay_queue_names() {
        assert!(is_managed_remote_queue_name("ArcRelay_Office_ab12cd34"));
        assert!(!is_managed_remote_queue_name("Office_ArcRelay_Printer"));
        assert!(!is_managed_remote_queue_name("ArcRelay"));
    }

    #[test]
    fn failed_binding_can_retry_installation() {
        let mut binding = RemoteQueueBinding::installing(
            DeviceId::parse("device").unwrap(),
            PrinterShareId::parse("share").unwrap(),
            "ArcRelay_Printer_share".into(),
            1,
        )
        .unwrap();
        binding.fail("already exists".into(), 2);

        binding.retry_installing(3).unwrap();

        assert_eq!(binding.state, QueueBindingState::Installing);
        assert_eq!(binding.failure_message, None);
        assert_eq!(binding.updated_at_ms, 3);
    }
}
