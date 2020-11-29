use std::sync::atomic::{AtomicU64, Ordering, AtomicBool};
use std::sync::Arc;

use event_listener::{Event, EventListener};

const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

pub struct EventPublisher {
    event: Event,
    change: AtomicU64,
}

impl EventPublisher {
    pub fn new() -> Self {
        Self { 
            event: Event::new(),
            change: AtomicU64::new(0),
        }
    }

    pub fn notify(&self)  {
        self.change.fetch_add(1,DEFAULT_EVENT_ORDERING);
        self.event.notify(usize::MAX);
    }

    #[inline]
    pub fn current_change(&self) -> u64 {
        self.change.load(DEFAULT_EVENT_ORDERING)
    }

    pub fn change_listener(self: &Arc<Self>) -> ChangeListener {
        let last_change =  self.current_change();
        ChangeListener {
            publisher: self.clone(),
            last_change
        }
    }

    pub fn listen(&self) -> EventListener {
        self.event.listen()
    }

}

/// listen for changes in the event
pub struct ChangeListener {
    publisher: Arc<EventPublisher>,
    last_change: u64
}

impl ChangeListener {

    /// check if there should be any changes
    /// this should be done before event listener
    /// to ensure no events are missed
    #[inline]
    pub fn has_change(&mut self) -> bool {
        let current_change = self.publisher.current_change();
        if current_change == self.last_change {
            false
        } else {
            self.last_change = current_change;
            true
        }
    }

    /// listen for new change
    pub fn listen(&self) -> EventListener {
        self.publisher.listen()
    }

    pub fn last_change(&self) -> u64 {
        self.last_change
    }

    pub fn current_change(&self) -> u64 {
        self.publisher.current_change()
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

    pub fn listen(&self) -> EventListener {
        self.event.listen()
    }

    pub fn notify(&self) {
        self.flag.store(true, DEFAULT_EVENT_ORDERING);
        self.event.notify(usize::MAX);
    }
}


#[cfg(test)]
mod test {

    use std::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering::SeqCst;

    use tracing::debug;
    use rand::{ thread_rng, Rng};


    use fluvio_future::test_async;
    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use super::ChangeListener;
    use super::EventPublisher;
    use super::SimpleEvent;

    struct TestController {
        change: ChangeListener,
        shutdown: Arc<SimpleEvent>,
        last_change: Arc<AtomicU64>
    }

    impl TestController {

        fn start(change: ChangeListener,shutdown: Arc<SimpleEvent>,last_change: Arc<AtomicU64>)   {
            let controller = Self{
                change,
                shutdown,
                last_change
            };
            spawn(controller.dispatch_loop());
        }

        async fn dispatch_loop(mut self) {

            use tokio::select;

            debug!("entering loop");
            loop {

                self.sync().await;


                if self.shutdown.is_set() {
                    debug!("shutdown exiting");
                    break;
                }

            
                if self.change.has_change() {
                    debug!("has change: {}",self.change.last_change());
                    continue;
                }

                let listener = self.change.listen();

                if self.change.has_change() {
                    debug!("has change: {}",self.change.last_change());
                    continue;
                }

                select! {
                    _ = listener => {
                        debug!("listen occur");
                        continue;
                    },
                    _ = self.shutdown.listen() => {
                        debug!("shutdown");
                        break;
                    }
                }

            }

            debug!("terminated, last change: {}",self.change.last_change());
            self.last_change.fetch_add(self.change.last_change(),SeqCst);
        }

        /// randomly sleep to simulate some tasks
        async fn sync(&mut self) {
            debug!("sync start");
            let delay = thread_rng().gen_range(1,10);
            sleep(Duration::from_millis(delay)).await;
            debug!("sync end");

        }


    }

    #[test_async]
    async fn test_listener() -> Result<(),()> {

        
        let publisher = Arc::new(EventPublisher::new());
        let listener = publisher.change_listener();
        let shutdown = SimpleEvent::shared();
        let last_change = Arc::new(AtomicU64::new(0));
        TestController::start(listener,shutdown.clone(),last_change.clone());

        let mut rng = thread_rng();
        for i in 0..20u16 {
            let delay = rng.gen_range(1,10);
            sleep(Duration::from_millis(delay)).await;
            publisher.notify();
            debug!("notification: {}",i);
        }
        
        // shutdown and wait to finish
        shutdown.notify();

        // wait for test controller to finish
        sleep(Duration::from_millis(10)).await;

        assert_eq!(publisher.current_change(),last_change.load(SeqCst));

        Ok(())

    }

}