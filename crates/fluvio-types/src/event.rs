use std::sync::atomic::{Ordering, AtomicBool};
use std::sync::Arc;

use tracing::trace;
use event_listener::Event;

const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

#[derive(Debug)]
pub struct StickyEvent {
    flag: AtomicBool,
    event: Event,
}

impl StickyEvent {
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

    pub fn listen_pinned(&self) -> impl std::future::Future<Output = ()> + '_ {
        Box::pin(self.listen())
    }

    pub fn notify(&self) {
        self.flag.store(true, DEFAULT_EVENT_ORDERING);
        self.event.notify(usize::MAX);
    }
}

pub mod offsets {
    use std::fmt;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::{Arc, Weak};

    use tracing::trace;
    use event_listener::{Event, EventListener};

    pub type SharedOffsetPublisher = Arc<OffsetPublisher>;
    pub type WeakSharedOffsetPublisher = Weak<OffsetPublisher>;

    const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

    pub const INIT_OFFSET: i64 = -1;
    pub const TOPIC_DELETED: i64 = -2;

    /// publish current offsets to listeners
    #[derive(Debug)]
    pub struct OffsetPublisher {
        current_value: AtomicI64,
        event: Event,
    }

    impl OffsetPublisher {
        pub fn shared(initial_value: i64) -> Arc<Self> {
            Arc::new(Self::new(initial_value))
        }

        pub fn new(initial_value: i64) -> Self {
            Self {
                current_value: AtomicI64::new(initial_value),
                event: Event::new(),
            }
        }

        // get current value
        pub fn current_value(&self) -> i64 {
            self.current_value.load(DEFAULT_EVENT_ORDERING)
        }

        fn listen(&self) -> EventListener {
            self.event.listen()
        }

        /// update with new value, this will trigger
        pub fn update(&self, new_value: i64) {
            self.current_value.store(new_value, DEFAULT_EVENT_ORDERING);
            self.event.notify(usize::MAX);
        }

        /// increase value by 1 and notify
        pub fn update_increment(&self) {
            self.update(self.current_value() + 1);
        }

        #[deprecated = "Replace by change_listener"]
        pub fn change_listner(self: &Arc<Self>) -> OffsetChangeListener {
            self.change_listener()
        }

        pub fn change_listener(self: &Arc<Self>) -> OffsetChangeListener {
            OffsetChangeListener::new(self.clone())
        }
    }

    pub struct OffsetChangeListener {
        publisher: Arc<OffsetPublisher>,
        last_value: i64,
    }

    impl fmt::Debug for OffsetChangeListener {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "OffsetCL{}", self.last_value)
        }
    }

    impl OffsetChangeListener {
        fn new(publisher: Arc<OffsetPublisher>) -> Self {
            Self {
                publisher,
                last_value: 0,
            }
        }

        #[allow(unused)]
        #[inline]
        fn has_change(&self) -> bool {
            self.current_value() != self.last_value
        }

        /// check if publisher has new value, if so return new value
        #[inline]
        fn has_new_value(&self) -> Option<i64> {
            let current_value = self.current_value();
            if current_value == self.last_value {
                None
            } else {
                Some(current_value)
            }
        }

        #[inline]
        fn current_value(&self) -> i64 {
            self.publisher.current_value()
        }

        #[inline]
        pub fn last_value(&self) -> i64 {
            self.last_value
        }

        // wait for new values from publisher in lock-free fashin
        pub async fn listen(&mut self) -> i64 {
            if let Some(new_value) = self.has_new_value() {
                self.last_value = new_value;
                return new_value;
            }

            let listener = self.publisher.listen();

            if let Some(new_value) = self.has_new_value() {
                self.last_value = new_value;
                return new_value;
            }

            listener.await;

            self.last_value = self.publisher.current_value();

            trace!(current_value = self.last_value);

            self.last_value
        }
    }
}

#[cfg(test)]
mod test {

    use std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    };
    use std::sync::atomic::AtomicBool;

    use tracing::debug;

    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use super::offsets::{OffsetChangeListener, OffsetPublisher};

    const ITER: u16 = 10;

    const CONTROLLER_WAIT: u64 = 500;

    struct TestController {
        listener: OffsetChangeListener,
        status: Arc<AtomicBool>,
    }

    impl TestController {
        fn start(listener: OffsetChangeListener, status: Arc<AtomicBool>) {
            let controller = Self { listener, status };
            spawn(controller.dispatch_loop());
        }

        async fn dispatch_loop(mut self) {
            use tokio::select;

            let mut timer = sleep(Duration::from_millis(300));

            let mut last_value = 0;
            loop {
                debug!("waiting");

                select! {
                    _ = &mut timer => {
                        debug!("timer expired");
                        break;
                    },
                    fetch_last_value = self.listener.listen() => {

                        debug!(fetch_last_value,"fetched last value");

                        // value from listener should be always be incremental and greater than prev value
                        assert!(fetch_last_value > last_value);
                        last_value = fetch_last_value;
                        if last_value >= (ITER-1) as i64 {
                            debug!("end controller");
                            self.status.store(true, Ordering::SeqCst);
                            break;
                        }
                    }
                }
            }
        }
    }

    #[fluvio_future::test]
    async fn test_offset_listener_no_wait() {
        let publisher = OffsetPublisher::shared(0);
        let listener = publisher.change_listener();
        let status = Arc::new(AtomicBool::new(false));

        TestController::start(listener, status.clone());
        // wait util controller to catch
        sleep(Duration::from_millis(1)).await;

        for i in 1..ITER {
            publisher.update(i as i64);
            assert_eq!(publisher.current_value(), i as i64);
            debug!(i, "publishing value");
            // sleep(Duration::from_millis(1)).await;
        }

        // wait for test controller to finish
        sleep(Duration::from_millis(CONTROLLER_WAIT)).await;
        debug!("test finished");

        // don't check in CI which is not reliable
        if std::env::var("CI").is_err() {
            assert!(status.load(Ordering::SeqCst), "status should be set");
        }
    }

    #[fluvio_future::test]
    async fn test_offset_listener_wait() {
        let publisher = OffsetPublisher::shared(0);
        let listener = publisher.change_listener();
        let status = Arc::new(AtomicBool::new(false));

        TestController::start(listener, status.clone());
        // wait util controller to catch
        sleep(Duration::from_millis(1)).await;

        for i in 1..ITER {
            publisher.update(i as i64);
            assert_eq!(publisher.current_value(), i as i64);
            debug!(i, "publishing value");
            sleep(Duration::from_millis(5)).await;
        }

        // wait for test controller to finish
        sleep(Duration::from_millis(CONTROLLER_WAIT)).await;
        debug!("test finished");

        // don't check in CI for now
        if std::env::var("CI").is_err() {
            assert!(status.load(Ordering::SeqCst), "status should be set");
        }
    }
}
