use std::sync::atomic::{AtomicI64, Ordering, AtomicBool};
use std::sync::Arc;

use tracing::{debug,trace};
use event_listener::{Event, EventListener};

const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

/// Track publishing of events by using u64 counter
#[derive(Debug)]
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

    pub fn notify(&self)  {
        self.event.notify(usize::MAX);
    }

    #[inline]
    pub fn current_change(&self) -> i64 {
        self.change.load(DEFAULT_EVENT_ORDERING)
    }

    pub fn increment(&self) -> i64 {
        self.change.fetch_add(1,DEFAULT_EVENT_ORDERING)
    }

    /// store new value
    pub fn store_change(&self,value: i64) {
        self.change.store(value, DEFAULT_EVENT_ORDERING);
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

/// listen for changes in the event by comparing against last change
pub struct ChangeListener {
    publisher: Arc<EventPublisher>,
    last_change: i64
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
            debug!("updating last change: {}",current_change);
            self.last_change = current_change;
            true
        }
    }


    #[inline]
    pub fn last_change(&self) -> i64 {
        self.last_change
    }

    pub fn set_last_change(&mut self, last_change: i64) {
        self.last_change = last_change;
    }

    pub fn current_change(&self) -> i64 {
        self.publisher.current_change()
    }

    pub async fn listen(&mut self)  {

        if self.has_change() {
            trace!("before has change: {}",self.last_change());
            return;
        }

        let listener = self.publisher.listen();

        if self.has_change() {
            trace!("after has change: {}",self.last_change());
            return;
        }

        listener.await;

        self.last_change = self.publisher.current_change();

        trace!("new last change: {}",self.last_change());

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

    pub async fn listen(&self)  {

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


#[cfg(test)]
mod test {

    use std::time::Duration;
    use std::sync::Arc;
    use std::sync::atomic::AtomicI64;
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
        last_change: Arc<AtomicI64>
    }

    impl TestController {

        fn start(change: ChangeListener,shutdown: Arc<SimpleEvent>,last_change: Arc<AtomicI64>)   {
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

            debug!("terminated, last change: {}",self.change.last_change());
            self.last_change.fetch_add(self.change.last_change(),SeqCst);
        }

        /// randomly sleep to simulate some tasks
        async fn sync(&mut self) {
            debug!("sync start");
            let delay = thread_rng().gen_range(1,30);
            sleep(Duration::from_millis(delay)).await;
            debug!("sync end");

        }


    }

    #[test_async]
    async fn test_listener() -> Result<(),()> {

        
        let publisher = Arc::new(EventPublisher::new());
        let listener = publisher.change_listener();
        let shutdown = SimpleEvent::shared();
        let last_change = Arc::new(AtomicI64::new(0));
        TestController::start(listener,shutdown.clone(),last_change.clone());

        let mut rng = thread_rng();
        for i in 0..20u16 {
            let delay = rng.gen_range(1,10);
            sleep(Duration::from_millis(delay)).await;
            publisher.increment();
            publisher.notify();
            debug!("notification: {}",i);
        }
       
         // wait for test controller to finish
        sleep(Duration::from_millis(20)).await;

        // shutdown and wait to finish
        shutdown.notify();

        sleep(Duration::from_millis(20)).await;

        assert_eq!(publisher.current_change(),last_change.load(SeqCst));

        Ok(())

    }

}