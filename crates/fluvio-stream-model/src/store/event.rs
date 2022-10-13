use std::sync::atomic::{AtomicI64, Ordering, AtomicBool};
use std::sync::Arc;

use tracing::trace;
use event_listener::{Event, EventListener};

const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

/// Track publishing of events by using u64 counter
#[derive(Debug, Default)]
pub struct EventPublisher {
    event: Event,
    change: AtomicI64,
}

impl EventPublisher {
    pub fn new() -> Self {
        Self {
            event: Event::new(),
            change: AtomicI64::new(0),
        }
    }

    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }

    fn notify(&self) {
        self.event.notify(usize::MAX);
    }

    #[inline]
    pub fn current_change(&self) -> i64 {
        self.change.load(DEFAULT_EVENT_ORDERING)
    }

    /// stores new value and notifies any listeners
    pub fn store_change(&self, value: i64) {
        self.change.store(value, DEFAULT_EVENT_ORDERING);
        self.notify()
    }

    pub fn listen(&self) -> EventListener {
        self.event.listen()
    }
}

pub struct SimpleEvent {
    flag: AtomicBool,
    event: Event,
}

impl SimpleEvent {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            flag: AtomicBool::new(false),
            event: Event::new(),
        })
    }

    // is flag set
    pub fn is_set(&self) -> bool {
        self.flag.load(DEFAULT_EVENT_ORDERING)
    }

    pub async fn listen(&self) {
        if self.is_set() {
            trace!("before, flag is set");
            return;
        }

        let listener = self.event.listen();

        if self.is_set() {
            trace!("after flag is set");
            return;
        }

        listener.await
    }

    pub fn notify(&self) {
        self.flag.store(true, DEFAULT_EVENT_ORDERING);
        self.event.notify(usize::MAX);
    }
}

/*
#[cfg(test)]
mod test {

    use std::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering::SeqCst;

    use tracing::debug;

    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use super::ChangeListener;
    use super::EventPublisher;
    use super::SimpleEvent;

    struct TestController {
        change: ChangeListener,
        shutdown: Arc<SimpleEvent>,
        last_change: Arc<AtomicI64>,
    }

    impl TestController {
        fn start(change: ChangeListener, shutdown: Arc<SimpleEvent>, last_change: Arc<AtomicI64>) {
            let controller = Self {
                change,
                shutdown,
                last_change,
            };
            spawn(controller.dispatch_loop());
        }

        async fn dispatch_loop(mut self) {
            use tokio::select;

            debug!("entering loop");
            loop {
                self.sync().await;

                select! {
                    _ = self.change.listen() => {
                        debug!("listen occur");
                        continue;
                    },
                    _ = self.shutdown.listen() => {
                        debug!("shutdown");
                        break;
                    }
                }
            }

            debug!("terminated, last change: {}", self.change.last_change());
        }

        /// randomly sleep to simulate some tasks
        async fn sync(&mut self) {
            debug!("sync start");
            self.last_change.fetch_add(1, SeqCst);
            sleep(Duration::from_millis(5)).await;
            self.change.load_last(); // sync to latest
            debug!("sync end: {}", self.change.last_change());
        }
    }

    #[fluvio_future::test]
    async fn test_listener()  {
        let publisher = Arc::new(EventPublisher::new());
        let listener = publisher.change_listener(0);
        let shutdown = SimpleEvent::shared();
        let last_change = Arc::new(AtomicI64::new(0));
        TestController::start(listener, shutdown.clone(), last_change.clone());

        for i in 0..5u16 {
            sleep(Duration::from_millis(2)).await;
            publisher.increment();
            publisher.notify();
            debug!("notification: {}, value: {}", i, publisher.current_change());
        }

        // wait for test controller to finish
        sleep(Duration::from_millis(20)).await;

        // shutdown and wait to finish
        shutdown.notify();

        sleep(Duration::from_millis(5)).await;

        // assert_eq!(last_change.load(SeqCst), 2); // there should be 2 sync happenings

    }
}
*/
