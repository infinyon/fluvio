use crate::producer::event::EventHandler;
use std::fmt::Debug;
use crate::{FluvioError};

pub struct ClientStatsEvent {
    batch_event: EventHandler,
}

impl Debug for ClientStatsEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientStatsEvent")
            .field("batch_event", &"{...}")
            .finish()
    }
}

impl Default for ClientStatsEvent {
    fn default() -> Self {
        Self {
            batch_event: EventHandler::new(),
        }
    }
}

impl ClientStatsEvent {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn listen_batch_event(&self) -> Result<(), FluvioError> {
        self.batch_event.listen().await;
        Ok(())
    }

    pub async fn notify_batch_event(&self) -> Result<(), FluvioError> {
        self.batch_event.notify().await;
        Ok(())
    }
}
