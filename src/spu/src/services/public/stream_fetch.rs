use std::sync::Arc;

use tracing::debug;
use tracing::trace;
use tracing::warn;
use tracing::error;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;
use tokio::select;
use event_listener::Event;

use flv_future_aio::sync::broadcast::RecvError;
use flv_future_aio::zero_copy::ZeroCopyWrite;
use flv_future_aio::task::spawn;

use kf_socket::InnerKfSink;
use kf_socket::InnerExclusiveKfSink;
use kf_socket::KfSocketError;
use dataplane_protocol::api::{RequestMessage, RequestHeader};
use dataplane_protocol::{Offset, Isolation, ReplicaKey};
use dataplane_protocol::fetch::FilePartitionResponse;
use fluvio_spu_schema::server::stream_fetch::FileStreamFetchRequest;
use fluvio_spu_schema::server::stream_fetch::StreamFetchResponse;

use crate::core::DefaultSharedGlobalContext;

/// continuous fetch handler
/// while client is active, it continuously send back new records
pub struct StreamFetchHandler<S> {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    header: RequestHeader,
    kf_sink: InnerExclusiveKfSink<S>,
    end_event: Arc<Event>,
}

impl<S> StreamFetchHandler<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    InnerKfSink<S>: ZeroCopyWrite,
{
    /// handle fluvio continuous fetch request
    pub fn handle_stream_fetch(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        kf_sink: InnerExclusiveKfSink<S>,
        end_event: Arc<Event>,
    ) {
        // first get receiver to offset update channel to we don't missed events

        let (header, msg) = request.get_header_request();

        let current_offset = msg.fetch_offset;
        let isolation = msg.isolation;
        let replica = ReplicaKey::new(msg.topic, msg.partition);
        let max_bytes = msg.max_bytes as u32;
        debug!(
            "conn: {}, start continuous fetch replica: {} offset: {}, max_bytes: {}",
            kf_sink.id(),
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
            kf_sink,
            end_event,
        };

        spawn(async move { handler.process(current_offset).await });
    }

    async fn process(mut self, starting_offset: Offset) -> Result<(), KfSocketError> {
        let mut current_offset =
            if let Some(offset) = self.send_back_records(starting_offset).await? {
                offset
            } else {
                debug!(
                    "conn: {}, no records, finishing processing",
                    self.kf_sink.id()
                );
                return Ok(());
            };

        let mut receiver = self.ctx.offset_channel().receiver();
        //pin_mut!(receiver);

        let mut counter: i32 = 0;
        loop {
            counter += 1;
            debug!(
                "conn: {}, waiting event, counter: {}",
                self.kf_sink.id(),
                counter
            );

            select! {

                _ = self.end_event.listen() => {
                    debug!("stream fetch: {}, connection has been terminated, terminating",self.kf_sink.id());
                    break;
                },

                offset_event_res = receiver.recv() => {

                    match offset_event_res {
                        Ok(offset_event) => {

                            debug!("conn: {}, received offset event connection: {:#?}", self.kf_sink.id(),offset_event);
                            if offset_event.replica_id == self.replica {
                                // depends on isolation, we need to keep track different offset
                                let update_offset = match self.isolation {
                                    Isolation::ReadCommitted => offset_event.hw,
                                    Isolation::ReadUncommitted => offset_event.leo
                                };
                                debug!("conn: {}, update offset: {}",self.kf_sink.id(),update_offset);
                                if update_offset != current_offset {
                                    debug!("conn: {}, updated offset replica: {} offset: {} diff from prev: {}",self.kf_sink.id(), self.replica,update_offset,current_offset);
                                    if let Some(offset) = self.send_back_records(current_offset).await? {
                                        debug!("conn: {}, replica: {} read offset: {}",self.kf_sink.id(), self.replica,offset);
                                        current_offset = offset;
                                    } else {
                                        debug!("conn: {}, no more replica: {} records can be read", self.kf_sink.id(),self.replica);
                                        break;
                                    }
                                } else {
                                    debug!("conn: {}, no changed in offset: {} offset: {} ignoring",self.kf_sink.id(), self.replica,update_offset);
                                }
                            } else {
                                debug!("conn: {}, ignoring event because replica does not match",self.kf_sink.id());
                            }


                        },
                        Err(err) => {
                            match err {
                                RecvError::Closed => {
                                    warn!("conn: {}, lost connection to leader controller",self.kf_sink.id());
                                },
                                RecvError::Lagged(lag) => {
                                    error!("conn: {}, lagging: {}",self.kf_sink.id(),lag);
                                }
                            }

                        }
                    }




                },
            }
        }

        debug!(
            "conn: {}, done with stream fetch loop exiting",
            self.kf_sink.id()
        );

        Ok(())
    }

    async fn send_back_records(&mut self, offset: Offset) -> Result<Option<Offset>, KfSocketError> {
        let mut partition_response = FilePartitionResponse::default();
        partition_response.partition_index = self.replica.partition;

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
                "conn: {}, retrieved slice len: {} replica: {}, from: {} to hw: {}, leo: {}",
                partition_response.records.len(),
                self.kf_sink.id(),
                self.replica,
                offset,
                hw,
                leo,
            );
            let response = StreamFetchResponse {
                topic: self.replica.topic.clone(),
                partition: partition_response,
            };

            let response = RequestMessage::<FileStreamFetchRequest>::response_with_header(
                &self.header,
                response,
            );
            trace!(
                "conn: {}, sending back file fetch response: {:#?}",
                self.kf_sink.id(),
                response
            );

            let mut inner_sink = self.kf_sink.lock().await;
            inner_sink
                .encode_file_slices(&response, self.header.api_version())
                .await?;

            trace!("conn: {}, finish sending fetch response", self.kf_sink.id());

            // get next offset
            let next_offset = match self.isolation {
                Isolation::ReadCommitted => hw,
                Isolation::ReadUncommitted => leo,
            };

            Ok(Some(next_offset))
        } else {
            debug!(
                "conn: {} unable to retrieve records from replica: {}, from: {}",
                self.kf_sink.id(),
                self.replica,
                offset
            );
            // in this case, partition is not founded
            Ok(None)
        }
    }
}
