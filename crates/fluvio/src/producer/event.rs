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

    async fn is_notified(&self) -> bool {
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
            if !self.is_notified().await {
                listener.await;
            } else {
                break;
            }
        }
    }
}
