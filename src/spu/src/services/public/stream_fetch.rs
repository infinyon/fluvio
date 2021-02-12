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
use fluvio_types::event::offsets::OffsetChangeListener;

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
    offset_listener: OffsetChangeListener,
}

impl<S> StreamFetchHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    InnerFlvSink<S>: ZeroCopyWrite,
{
    /// handle fluvio continuous fetch request
    pub fn start(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        sink: InnerExclusiveFlvSink<S>,
        end_event: Arc<SimpleEvent>,
        offset_listener: OffsetChangeListener,
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
            offset_listener,
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
        let mut last_end_offset =
            if let Some(read_offset) = self.send_back_records(starting_offset).await? {
                debug!(read_offset, "initial records offsets read");
                read_offset
            } else {
                debug!("conn: {}, no records, finishing processing", self.sink.id());
                return Ok(());
            };

        let mut receiver = self.ctx.offset_channel().receiver();

        let mut counter: i32 = 0;
        let mut consumer_offset: Option<Offset> = None; // offset for consumer

        loop {
            counter += 1;
            debug!(counter, "waiting for event",);

            select! {

                _ = self.end_event.listen() => {
                    debug!("end event has been received, terminating");
                    break;
                },


                changed_consumer_offset = self.offset_listener.listen() => {

                    if changed_consumer_offset < last_end_offset {
                        // there were something to read
                        if let Some(offset_read) = self.send_back_records(changed_consumer_offset).await? {
                            debug!(offset_read);
                            last_end_offset = offset_read;
                            consumer_offset = None;
                        } else {
                            debug!("no more replica records can be read");
                            break;
                        }
                    }

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

                                debug!(update_offset);
                                if let Some(last_consumer_offset) = consumer_offset {
                                    // we know what consumer offset is
                                    if update_offset > last_consumer_offset {
                                        debug!(update_offset,
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
                                        debug!(ignored_update_offset = update_offset);
                                        last_end_offset = update_offset;
                                    }
                                } else {

                                    // we don't know consumer offset, so we delay
                                    debug!(delay_consumer_offset = update_offset);
                                    last_end_offset = update_offset;

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
