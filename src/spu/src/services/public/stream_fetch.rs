use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::{debug, trace, error};
use tracing::instrument;
use futures_util::io::{AsyncRead, AsyncWrite};
use tokio::select;
use tokio::sync::broadcast::RecvError;

use fluvio_types::event::{SimpleEvent, offsets::OffsetPublisher};
use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_future::task::spawn;
use fluvio_socket::{InnerFlvSink, InnerExclusiveFlvSink, FlvSocketError};
use dataplane::{ErrorCode, api::{RequestMessage, RequestHeader}};
use dataplane::{Offset, Isolation, ReplicaKey};
use dataplane::fetch::FilePartitionResponse;
use fluvio_spu_schema::server::stream_fetch::{FileStreamFetchRequest, StreamFetchResponse};
use fluvio_types::event::offsets::OffsetChangeListener;

use crate::core::DefaultSharedGlobalContext;
use crate::controllers::leader_replica::SharedFileLeaderState;
use publishers::INIT_OFFSET;

/// Fetch records as stream
pub struct StreamFetchHandler<S> {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    header: RequestHeader,
    sink: InnerExclusiveFlvSink<S>,
    end_event: Arc<SimpleEvent>,
    consumer_offset_listener: OffsetChangeListener,
    leader_state: SharedFileLeaderState,
    stream_id: u32,
}

impl<S> StreamFetchHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    InnerFlvSink<S>: ZeroCopyWrite,
{
    /// handle fluvio continuous fetch request
    pub async fn start(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        sink: InnerExclusiveFlvSink<S>,
        end_event: Arc<SimpleEvent>,
    ) -> Result<(), FlvSocketError> {
        // first get receiver to offset update channel to we don't missed events

        let (header, msg) = request.get_header_request();

        let current_offset = msg.fetch_offset;
        let isolation = msg.isolation;
        let replica = ReplicaKey::new(msg.topic, msg.partition);
        let max_bytes = msg.max_bytes as u32;

        if let Some(leader_state) = ctx.leaders_state().get(&replica) {
            let (stream_id, offset_publisher) =
                ctx.stream_publishers().create_new_publisher().await;
            let offset_listener = offset_publisher.change_listner();

            debug!(

                sink = sink.id(),
                %replica,
                current_offset,
                max_bytes,
                "start stream fetch"
            );

            let handler = Self {
                ctx: ctx.clone(),
                isolation,
                replica,
                header,
                max_bytes,
                sink,
                end_event,
                consumer_offset_listener: offset_listener,
                stream_id,
                leader_state: leader_state.clone(),
            };

            spawn(async move { handler.process(current_offset).await });
        } else {

            let response = StreamFetchResponse {
                topic: replica.topic,
                stream_id: 0,
                partition: FilePartitionResponse {
                    partition_index: replica.partition,
                    error_code: ErrorCode::NotLeaderForPartition,
                    ..Default::default()
                }
            };
        
        
            let response_msg =
                RequestMessage::<FileStreamFetchRequest>::response_with_header(&header, response);
        
            trace!("sending back file fetch response msg: {:#?}", response_msg);
        
            let mut inner_sink = sink.lock().await;
            inner_sink.send_response(&response_msg, header.api_version()).await?;
            debug!("finish sending not valid");
        }

        Ok(())
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
        let mut last_end_offset =
            if let Some(read_offset) = self.send_back_records(starting_offset).await? {
                debug!(read_offset, "initial records offsets read");
                read_offset
            } else {
                debug!("conn: {}, no records, finishing processing", self.sink.id());
                return Ok(());
            };

        
        let mut leader_offset_receiver = self.leader_state.offset_listener(&self.isolation);
       

        let mut counter: i32 = 0;
        let mut consumer_offset: Option<Offset> = if last_end_offset == starting_offset {
            Some(last_end_offset) // no records, we reset to start
        } else {
            None // offset for consumer
        };

        loop {
            counter += 1;

            debug!(counter, ?consumer_offset, last_end_offset, "waiting");

            select! {

                _ = self.end_event.listen() => {
                    debug!("end event has been received, terminating");
                    break;
                },


                changed_consumer_offset = self.consumer_offset_listener.listen() => {


                    if changed_consumer_offset != INIT_OFFSET  {

                        if changed_consumer_offset < last_end_offset {
                            debug!(
                                changed_consumer_offset,
                                last_end_offset,
                                "need send back"
                            );
                            if let Some(offset_read) = self.send_back_records(changed_consumer_offset).await? {
                                debug!(offset_read);
                                last_end_offset = offset_read;
                                consumer_offset = None;
                                debug!(
                                    last_end_offset,
                                    "reset"
                                );
                            } else {
                                debug!("no more replica records can be read");
                                break;
                            }
                        } else {
                            debug!(
                                changed_consumer_offset,
                                last_end_offset,
                                "consume caught up"
                            );
                            consumer_offset = Some(changed_consumer_offset);
                        }
                    } else {
                        debug!(
                            changed_consumer_offset,
                            "ignoring offset update")
                    }

                },

                leader_offset_update = leader_offset_receiver.listen() => {

                    debug!(leader_offset_update);
                    
                    if let Some(last_consumer_offset) = consumer_offset {
                        // we know what consumer offset is
                        if leader_offset_update > last_consumer_offset {
                            debug!(leader_offset_update,
                                consumer_offset = last_consumer_offset,
                                "reading offset event");
                            if let Some(offset_read) = self.send_back_records(last_consumer_offset).await? {
                                debug!(offset_read);
                                // actual read should be end offset since it might been changed since during read
                                last_end_offset = offset_read;
                                consumer_offset = None;
                            } else {
                                debug!("no more replica records can be read");
                                break;
                            }
                        } else {
                            debug!(ignored_update_offset = leader_offset_update);
                            last_end_offset = leader_offset_update;
                        }
                    } else {

                        // we don't know consumer offset, so we delay
                        debug!(delay_consumer_offset = leader_offset_update);
                        last_end_offset = leader_offset_update;

                    }
                     
                },
            }
        }

        debug!("done with stream fetch loop exiting");

        self.ctx
            .stream_publishers()
            .remove_publisher(self.stream_id)
            .await;

        Ok(())
    }

    /// send back records back to consumer
    /// return known current LEO
    #[instrument(skip(self))]
    async fn send_back_records(
        &mut self,
        offset: Offset,
    ) -> Result<Option<Offset>, FlvSocketError> {
        let mut partition_response = FilePartitionResponse {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        let (hw, leo) = self
            .leader_state
            .read_records(
                offset,
                self.max_bytes,
                self.isolation.clone(),
                &mut partition_response,
            )
            .await;

        let response = StreamFetchResponse {
            topic: self.replica.topic.clone(),
            stream_id: self.stream_id,
            partition: partition_response,
        };

        debug!(
            stream_id = response.stream_id,
            len = response.partition.records.len(),
            offset,
            hw,
            leo,
            "start back stream response",
        );

        let response_msg =
            RequestMessage::<FileStreamFetchRequest>::response_with_header(&self.header, response);

        trace!("sending back file fetch response msg: {:#?}", response_msg);

        let mut inner_sink = self.sink.lock().await;
        inner_sink
            .encode_file_slices(&response_msg, self.header.api_version())
            .await?;

        drop(inner_sink);

        debug!("finish sending stream response");

        // get next offset
        let next_offset = match self.isolation {
            Isolation::ReadCommitted => hw,
            Isolation::ReadUncommitted => leo,
        };

        Ok(Some(next_offset))
    }

}


pub mod publishers {

    use std::{
        collections::HashMap,
        sync::{Arc, atomic::AtomicU32},
    };
    use std::sync::atomic::Ordering::SeqCst;
    use std::fmt::Debug;

    use async_mutex::Mutex;
    use tracing::debug;

    use super::OffsetPublisher;

    pub const INIT_OFFSET: i64 = -1;

    pub struct StreamPublishers {
        publishers: Mutex<HashMap<u32, Arc<OffsetPublisher>>>,
        stream_id: AtomicU32,
    }

    impl Debug for StreamPublishers {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stream {}", self.stream_id.load(SeqCst))
        }
    }

    impl StreamPublishers {
        pub fn new() -> Self {
            Self {
                publishers: Mutex::new(HashMap::new()),
                stream_id: AtomicU32::new(0),
            }
        }

        // get next stream id
        fn next_stream_id(&self) -> u32 {
            self.stream_id.fetch_add(1, SeqCst)
        }

        pub async fn create_new_publisher(&self) -> (u32, Arc<OffsetPublisher>) {
            let stream_id = self.next_stream_id();
            let offset_publisher = OffsetPublisher::shared(INIT_OFFSET);
            let mut publisher_lock = self.publishers.lock().await;
            publisher_lock.insert(stream_id, offset_publisher.clone());
            (stream_id, offset_publisher)
        }

        /// get publisher with stream id
        pub async fn get_publisher(&self, stream_id: u32) -> Option<Arc<OffsetPublisher>> {
            let publisher_lock = self.publishers.lock().await;
            publisher_lock.get(&stream_id).cloned()
        }

        pub async fn remove_publisher(&self, stream_id: u32) {
            let mut publisher_lock = self.publishers.lock().await;
            if publisher_lock.remove(&stream_id).is_some() {
                debug!(stream_id, "removed stream publisher");
            } else {
                debug!(stream_id, "no stream publisher founded");
            }
        }
    }
}
