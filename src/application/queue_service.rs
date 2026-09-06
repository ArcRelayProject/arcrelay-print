use std::sync::Arc;

use crate::application::{Clock, QueueBindingRepository, SystemQueueRegistrar, SystemQueueSpec};
use crate::domain::{PublishedPrinter, QueueBindingId, QueueBindingState, RemoteQueueBinding};
use crate::{PrintError, Result};

pub struct SystemQueueService {
    bindings: Arc<dyn QueueBindingRepository>,
    registrar: Arc<dyn SystemQueueRegistrar>,
    clock: Arc<dyn Clock>,
}

impl SystemQueueService {
    pub fn new(
        bindings: Arc<dyn QueueBindingRepository>,
        registrar: Arc<dyn SystemQueueRegistrar>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            bindings,
            registrar,
            clock,
        }
    }

    pub async fn install(
        &self,
        printer: PublishedPrinter,
        queue_name: String,
        ipp_uri: String,
    ) -> Result<RemoteQueueBinding> {
        let existing = self
            .bindings
            .find_remote(&printer.source_device_id, &printer.share_id)
            .await?;
        let mut binding = match existing {
            Some(mut binding) => match binding.state {
                QueueBindingState::Failed => {
                    binding.retry_installing(self.clock.now_ms())?;
                    self.bindings.save(&binding).await?;
                    binding
                }
                QueueBindingState::Removing => {
                    return Err(PrintError::InvalidState(
                        "printer queue is currently being removed".into(),
                    ))
                }
                QueueBindingState::Installing
                | QueueBindingState::Ready
                | QueueBindingState::SourceOffline => return Ok(binding),
            },
            None => {
                let binding = RemoteQueueBinding::installing(
                    printer.source_device_id.clone(),
                    printer.share_id.clone(),
                    queue_name,
                    self.clock.now_ms(),
                )?;
                self.bindings.save(&binding).await?;
                binding
            }
        };
        match self
            .registrar
            .install(&SystemQueueSpec {
                queue_name: binding.system_queue_name.clone(),
                ipp_uri,
                printer,
            })
            .await
        {
            Ok(system_id) => binding.installed(system_id, self.clock.now_ms())?,
            Err(error) => binding.fail(error.to_string(), self.clock.now_ms()),
        }
        self.bindings.save(&binding).await?;
        Ok(binding)
    }

    pub async fn remove(&self, id: &QueueBindingId) -> Result<()> {
        let mut binding = self
            .bindings
            .find(id)
            .await?
            .ok_or_else(|| PrintError::NotFound(id.to_string()))?;
        binding.begin_removal(self.clock.now_ms())?;
        self.bindings.save(&binding).await?;
        if let Some(system_id) = &binding.system_queue_id {
            self.registrar.remove(system_id).await?;
        }
        self.bindings.delete(id).await
    }

    pub async fn list(&self) -> Result<Vec<RemoteQueueBinding>> {
        self.bindings.list().await
    }
}
