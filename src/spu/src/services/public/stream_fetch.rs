use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::{debug, trace, error};
use tracing::instrument;
use futures_util::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::broadcast::RecvError;

use fluvio_types::event::SimpleEvent;
use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_future::task::spawn;
use fluvio_socket::{InnerFlvSink, InnerExclusiveFlvSink, FlvSocketError};
use dataplane::api::{RequestMessage, RequestHeader};
use dataplane::{Offset, Isolation, ReplicaKey};
use dataplane::fetch::FilePartitionResponse;
use fluvio_spu_schema::server::stream_fetch::{FileStreamFetchRequest, StreamFetchResponse};

use crate::core::DefaultSharedGlobalContext;

/// Fetch records as stream
pub struct StreamFetchHandler<S> {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    header: RequestHeader,
    sink: InnerExclusiveFlvSink<S>,
    end_event: Arc<SimpleEvent>,
}

impl<S> StreamFetchHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    InnerFlvSink<S>: ZeroCopyWrite,
{
    /// handle fluvio continuous fetch request
    pub fn handle_stream_fetch(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        sink: InnerExclusiveFlvSink<S>,
        end_event: Arc<SimpleEvent>,
    ) {
        // first get receiver to offset update channel to we don't missed events

        let (header, msg) = request.get_header_request();

        let current_offset = msg.fetch_offset;
        let isolation = msg.isolation;
        let replica = ReplicaKey::new(msg.topic, msg.partition);
        let max_bytes = msg.max_bytes as u32;
        debug!(
            "conn: {}, start continuous fetch replica: {} offset: {}, max_bytes: {}",
            sink.id(),
            replica,
            current_offset,
            max_bytes
        );

        let handler = Self {
            ctx,
            isolation,
            replica,
            header,
            max_bytes,
            sink,
            end_event,
        };

        spawn(async move { handler.process(current_offset).await });
    }

    #[instrument(
        skip(self),
        name = "stream fetch",
        fields(
            replica = %self.replica,
            sink = self.sink.id()
        )
    )]
    async fn process(mut self, starting_offset: Offset) {
        if let Err(err) = self.inner_process(starting_offset).await {
            error!("error: {:#?}", err);
            self.end_event.notify();
        }
    }

    async fn inner_process(&mut self, starting_offset: Offset) -> Result<(), FlvSocketError> {
        let mut current_offset =
            if let Some(offset) = self.send_back_records(starting_offset).await? {
                offset
            } else {
                debug!("conn: {}, no records, finishing processing", self.sink.id());
                return Ok(());
            };

        let mut receiver = self.ctx.offset_channel().receiver();

        let mut counter: i32 = 0;
        loop {
            counter += 1;
            debug!(counter, "waiting for event",);

            select! {

                _ = self.end_event.listen() => {
                    debug!("end event has been received, terminating");
                    break;
                },

                offset_event_res = receiver.recv() => {

                    match offset_event_res {
                        Ok(offset_event) => {

                            debug!(leo = offset_event.hw,
                                hw = offset_event.hw,
                                "received offset");
                            if offset_event.replica_id == self.replica {
                                // depends on isolation, we need to keep track different offset
                                let update_offset = match self.isolation {
                                    Isolation::ReadCommitted => offset_event.hw,
                                    Isolation::ReadUncommitted => offset_event.leo
                                };
                                debug!("conn: {}, update offset: {}",self.sink.id(),update_offset);
                                if update_offset != current_offset {
                                    debug!(update_offset,
                                        current_offset,
                                        "updated offsets");
                                    if let Some(offset) = self.send_back_records(current_offset).await? {
                                        debug!(offset, "readed offset");
                                        current_offset = offset;
                                    } else {
                                        debug!("no more replica records can be read");
                                        break;
                                    }
                                } else {
                                    debug!("changed in offset: {} offset: {} ignoring",self.replica,update_offset);
                                }
                            } else {
                                trace!("ignoring event because replica does not match");
                            }


                        },
                        Err(err) => {
                            match err {
                                RecvError::Closed => {
                                    error!("lost connection to leader controller");
                                    return Err(IoError::new(
                                        ErrorKind::Other,
                                        format!("lost connection to leader: {}",self.replica)
                                    ).into())
                                },
                                RecvError::Lagged(lagging) => {
                                    error!(lagging);
                                }
                            }

                        }
                    }
                },
            }
        }

        debug!("done with stream fetch loop exiting");

        Ok(())
    }

    /// send back records
    #[instrument(skip(self))]
    async fn send_back_records(
        &mut self,
        offset: Offset,
    ) -> Result<Option<Offset>, FlvSocketError> {
        let mut partition_response = FilePartitionResponse {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        if let Some((hw, leo)) = self
            .ctx
            .leaders_state()
            .read_records(
                &self.replica,
                offset,
                self.max_bytes,
                self.isolation.clone(),
                &mut partition_response,
            )
            .await
        {
            debug!(
                recods = partition_response.records.len(),
                offset, hw, leo, "retrieved slices",
            );
            let response = StreamFetchResponse {
                topic: self.replica.topic.clone(),
                partition: partition_response,
            };

            let response = RequestMessage::<FileStreamFetchRequest>::response_with_header(
                &self.header,
                response,
            );
            trace!("sending back file fetch response: {:#?}", response);

            let mut inner_sink = self.sink.lock().await;
            inner_sink
                .encode_file_slices(&response, self.header.api_version())
                .await?;

            drop(inner_sink);

            trace!("finish sending fetch response");

            // get next offset
            let next_offset = match self.isolation {
                Isolation::ReadCommitted => hw,
                Isolation::ReadUncommitted => leo,
            };

            Ok(Some(next_offset))
        } else {
            debug!(
                "conn: {} unable to retrieve records from replica: {}, from: {}",
                self.sink.id(),
                self.replica,
                offset
            );
            // in this case, partition is not founded
            Ok(None)
        }
    }
}

mod events {
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    use tracing::trace;
    use event_listener::{Event, EventListener};

    const DEFAULT_EVENT_ORDERING: Ordering = Ordering::SeqCst;

    /// publish current offsets to listeners
    pub struct OffsetPublisher {
        current_value: AtomicI64,
        event: Event,
    }

    impl OffsetPublisher {
        pub fn shared(initial_value: i64) -> Arc<Self> {
            Arc::new(Self {
                current_value: AtomicI64::new(initial_value),
                event: Event::new(),
            })
        }

        // get current value
        pub fn current_value(&self) -> i64 {
            self.current_value.load(DEFAULT_EVENT_ORDERING)
        }

        pub fn listen(&self) -> EventListener {
            self.event.listen()
        }

        /// update with new value, this will trigger
        pub fn update(&self, new_value: i64) {
            self.current_value.store(new_value, DEFAULT_EVENT_ORDERING);
            self.event.notify(usize::MAX);
        }

        pub fn change_listner(self: &Arc<Self>) -> OffsetChangeListener {
            OffsetChangeListener::new(self.clone())
        }
    }

    pub struct OffsetChangeListener {
        publisher: Arc<OffsetPublisher>,
        last_value: i64,
    }

    impl OffsetChangeListener {
        fn new(publisher: Arc<OffsetPublisher>) -> Self {
            Self {
                publisher,
                last_value: 0,
            }
        }

        #[inline]
        pub fn has_change(&self) -> bool {
            self.current_value() != self.last_value
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
        pub async fn listen(&mut self) {
            if self.has_change() {
                self.last_value = self.publisher.current_value();
                return;
            }

            let listener = self.publisher.listen();

            if self.has_change() {
                self.last_value = self.publisher.current_value();
                return;
            }

            listener.await;

            self.last_value = self.publisher.current_value();

            trace!(current_value = self.last_value);
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

    use fluvio_future::test_async;
    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use super::events::{OffsetChangeListener, OffsetPublisher};

    const ITER: u16 = 10;

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

            let mut timer = sleep(Duration::from_millis(50));

            let mut last_value = 0;
            loop {
                debug!("waiting");

                select! {
                    _ = &mut timer => {
                        debug!("timer expired");
                        break;
                    },
                    _ = self.listener.listen() => {
                        debug!("listen occur");
                        let fetch_last_value = self.listener.last_value();
                        debug!(fetch_last_value);

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

    #[test_async]
    async fn test_offset_listener_no_wait() -> Result<(), ()> {
        let publisher = OffsetPublisher::shared(0);
        let listener = publisher.change_listner();
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
        sleep(Duration::from_millis(100)).await;
        debug!("test finished");
        assert!(status.load(Ordering::SeqCst), "status should be set");

        Ok(())
    }

    #[test_async]
    async fn test_offset_listener_wait() -> Result<(), ()> {
        let publisher = OffsetPublisher::shared(0);
        let listener = publisher.change_listner();
        let status = Arc::new(AtomicBool::new(false));

        TestController::start(listener, status.clone());
        // wait util controller to catch
        sleep(Duration::from_millis(1)).await;

        for i in 1..ITER {
            publisher.update(i as i64);
            assert_eq!(publisher.current_value(), i as i64);
            debug!(i, "publishing value");
            sleep(Duration::from_millis(1)).await;
        }

        // wait for test controller to finish
        sleep(Duration::from_millis(100)).await;
        debug!("test finished");
        assert!(status.load(Ordering::SeqCst), "status should be set");

        Ok(())
    }
}
