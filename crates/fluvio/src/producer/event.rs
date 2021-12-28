use std::sync::Arc;

use async_lock::Mutex;
use event_listener::Event;

/// Handler of events that keep track of the number of occurrences
/// of the event that needs to be handled
pub(crate) struct EventHandler {
    count: Mutex<usize>,
    event: Event,
}

impl EventHandler {
    pub fn new() -> Self {
        Self {
            count: Mutex::new(0),
            event: Event::new(),
        }
    }
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    pub async fn notify(&self) {
        let mut count = self.count.lock().await;
        *count += 1;
        self.event.notify(1);
    }

    async fn try_acquire_notification(&self) -> bool {
        let mut count = self.count.lock().await;
        if *count > 0 {
            *count -= 1;
            true
        } else {
            false
        }
    }

    pub async fn listen(&self) {
        loop {
            let listener = self.event.listen();
            if !self.try_acquire_notification().await {
                listener.await;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::EventHandler;

    #[fluvio_future::test]
    async fn test_event_handler() {
        let event = EventHandler::new();
        let timeout = std::time::Duration::from_millis(150);

        assert!(async_std::future::timeout(timeout, event.listen())
            .await
            .is_err());

        event.notify().await;
        assert!(async_std::future::timeout(timeout, event.listen())
            .await
            .is_ok());
        event.notify().await;
        event.notify().await;
        assert!(async_std::future::timeout(timeout, event.listen())
            .await
            .is_ok());
        assert!(async_std::future::timeout(timeout, event.listen())
            .await
            .is_ok());
        assert!(async_std::future::timeout(timeout, event.listen())
            .await
            .is_err());
    }
}
