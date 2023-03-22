use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, error, instrument, trace, warn};
use tokio::select;

use fluvio_smartengine::SmartModuleChainInstance;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_types::event::{StickyEvent, offsets::OffsetPublisher};
use fluvio_future::task::spawn;
use fluvio_socket::{ExclusiveFlvSink, SocketError};
use fluvio_protocol::{
    api::{RequestMessage, RequestHeader},
    record::{RecordSet, Offset, RawRecords},
};
use fluvio_protocol::link::{ErrorCode, smartmodule::SmartModuleTransformRuntimeError};
use fluvio_compression::CompressionError;
use fluvio_spu_schema::{
    server::stream_fetch::{
        DefaultStreamFetchRequest, FileStreamFetchRequest, StreamFetchRequest, StreamFetchResponse,
    },
    fetch::{FilePartitionResponse, FetchablePartitionResponse},
    Isolation,
    file::FileRecordSet,
};
use fluvio_types::event::offsets::OffsetChangeListener;
use fluvio_protocol::record::Batch;

use crate::core::{DefaultSharedGlobalContext, metrics::IncreaseValue};
use crate::replication::leader::SharedFileLeaderState;
use crate::services::public::conn_context::ConnectionContext;
use crate::services::public::stream_fetch::publishers::INIT_OFFSET;
use crate::smartengine::context::SmartModuleContext;
use crate::smartengine::batch::process_batch;
use crate::smartengine::file_batch::FileBatchIterator;
use crate::core::metrics::SpuMetrics;
use crate::traffic::TrafficType;

/// Fetch records as stream
pub struct StreamFetchHandler {
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    max_fetch_bytes: u32,
    header: RequestHeader,
    sink: ExclusiveFlvSink,
    end_event: Arc<StickyEvent>,
    consumer_offset_listener: OffsetChangeListener,
    leader_state: SharedFileLeaderState,
    stream_id: u32,
    metrics: Arc<SpuMetrics>,
}

impl StreamFetchHandler {
    /// handle fluvio continuous fetch request
    pub(crate) async fn start(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        conn_ctx: &mut ConnectionContext,
        sink: ExclusiveFlvSink,
        end_event: Arc<StickyEvent>,
    ) -> Result<(), SocketError> {
        let (header, msg) = request.get_header_request();
        let replica = ReplicaKey::new(msg.topic.clone(), msg.partition);

        if let Some(leader_state) = ctx.leaders_state().get(&replica).await {
            let (stream_id, offset_publisher) = conn_ctx
                .stream_publishers_mut()
                .create_new_publisher()
                .await;
            let consumer_offset_listener = offset_publisher.change_listener();

            spawn(async move {
                if let Err(err) = StreamFetchHandler::fetch(
                    ctx,
                    sink,
                    end_event.clone(),
                    leader_state,
                    stream_id,
                    header,
                    replica,
                    consumer_offset_listener,
                    msg,
                )
                .await
                {
                    error!("error starting stream fetch handler: {:#?}", err);
                    end_event.notify();
                }
            });
        } else {
            debug!(topic = %replica.topic," no leader found, returning");
            let response = StreamFetchResponse {
                topic: replica.topic,
                stream_id: 0,
                partition: FilePartitionResponse {
                    partition_index: replica.partition,
                    error_code: ErrorCode::NotLeaderForPartition,
                    ..Default::default()
                },
            };

            let response_msg =
                RequestMessage::<FileStreamFetchRequest>::response_with_header(&header, response);

            trace!("sending back file fetch response msg: {:#?}", response_msg);

            let mut inner_sink = sink.lock().await;
            inner_sink
                .send_response(&response_msg, header.api_version())
                .await?;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(
        skip(ctx,replica,end_event,leader_state,header,msg,consumer_offset_listener),
        fields(
            replica = %replica,
            sink = sink.id()
        ))
    ]
    pub async fn fetch(
        ctx: DefaultSharedGlobalContext,
        sink: ExclusiveFlvSink,
        end_event: Arc<StickyEvent>,
        leader_state: SharedFileLeaderState,
        stream_id: u32,
        header: RequestHeader,
        replica: ReplicaKey,
        consumer_offset_listener: OffsetChangeListener,
        msg: StreamFetchRequest<FileRecordSet>,
    ) -> Result<(), SocketError> {
        debug!("request: {:#?}", msg);
        let version = header.api_version();

        let derivedstream_ctx =
            match SmartModuleContext::try_from(msg.smartmodules, version, &ctx).await {
                Ok(ctx) => ctx,
                Err(error_code) => {
                    warn!("smartmodule context init failed: {:?}", error_code);
                    send_back_error(&sink, &replica, &header, stream_id, error_code).await?;
                    return Ok(());
                }
            };

        let max_bytes = msg.max_bytes as u32;
        // compute max fetch bytes depends on smart stream
        let max_fetch_bytes = if derivedstream_ctx.is_some() {
            u32::MAX
        } else {
            max_bytes
        };

        let starting_offset = msg.fetch_offset;
        let isolation = msg.isolation;

        debug!(
            max_bytes,
            max_fetch_bytes,
            isolation = ?isolation,
            stream_id,
            sink = %sink.id(),
            starting_offset,
            "stream fetch");

        let handler = Self {
            isolation,
            replica: replica.clone(),
            max_bytes,
            sink: sink.clone(),
            end_event,
            header: header.clone(),
            consumer_offset_listener,
            stream_id,
            leader_state,
            max_fetch_bytes,
            metrics: ctx.metrics(),
        };

        if let Err(err) = handler.process(starting_offset, derivedstream_ctx).await {
            match err {
                StreamFetchError::Fetch(error_code) => {
                    send_back_error(&sink, &replica, &header, stream_id, error_code).await?;
                    Ok(())
                }
                StreamFetchError::Socket(err) => Err(err),
                StreamFetchError::Compression(err) => {
                    error!(%err, "compression error");
                    send_back_error(
                        &sink,
                        &replica,
                        &header,
                        stream_id,
                        ErrorCode::CompressionError,
                    )
                    .await?;
                    Ok(())
                }
            }
        } else {
            Ok(())
        }
    }

    async fn process(
        mut self,
        starting_offset: Offset,
        sm_ctx: Option<SmartModuleContext>,
    ) -> Result<(), StreamFetchError> {
        let mut smartmodule_instance = if let Some(ctx) = sm_ctx {
            let SmartModuleContext { chain: st } = ctx;
            Some(st)
        } else {
            None
        };

        let (mut last_partition_offset, consumer_wait) = self
            .send_back_records(starting_offset, smartmodule_instance.as_mut())
            .await?;

        let mut leader_offset_receiver = self.leader_state.offset_listener(&self.isolation);
        let mut counter: i32 = 0;
        // since we don't need to wait for consumer, can move consumer to same offset as last read
        let mut last_known_consumer_offset: Option<Offset> =
            (!consumer_wait).then_some(last_partition_offset);

        loop {
            counter += 1;
            debug!(
                counter,
                ?last_known_consumer_offset,
                last_partition_offset,
                "Stream fetch waiting for update"
            );

            select! {
                _ = self.end_event.listen() => {
                    debug!("end event has been received, terminating");
                    break;
                },


                // Received offset update from consumer, i.e. consumer acknowledged to this offset
                consumer_offset_update = self.consumer_offset_listener.listen() => {
                    if consumer_offset_update == INIT_OFFSET {
                        continue;
                    }

                    // If the consumer offset is not behind, there is no need to send records
                    if consumer_offset_update >= last_partition_offset {
                        debug!(
                            consumer_offset_update,
                            last_partition_offset,
                            "Consumer offset updated and is caught up, no need to send records",
                        );
                        last_known_consumer_offset = Some(consumer_offset_update);
                        continue;
                    }

                    // If the consumer is behind, we need to send everything beyond
                    // the consumer's latest offset
                    debug!(
                        consumer_offset_update,
                        last_partition_offset,
                        "Consumer offset updated and is behind, need to send records",
                    );
                    let (offset, wait) = self.send_back_records(consumer_offset_update, smartmodule_instance.as_mut()).await?;
                    last_partition_offset = offset;
                    if wait {
                        last_known_consumer_offset = None;
                        debug!(
                            last_partition_offset,
                            ?last_known_consumer_offset,
                            "Finished consumer_offset_update, waiting for consumer",
                        );
                    } else {
                        last_known_consumer_offset = Some(last_partition_offset);
                    }
                },

                // Received new partition offset from leader, i.e. a new record was produced
                partition_offset_update = leader_offset_receiver.listen() => {
                    debug!(partition_offset_update, "Received leader update:");

                    let last_consumer_offset = match last_known_consumer_offset {
                        Some(last_consumer_offset) => last_consumer_offset,
                        None => {
                            // If we do not know the last offset the consumer has read to,
                            // we don't know what to send them. In that case, just save
                            // this new leader offset to our local variable.
                            last_partition_offset = partition_offset_update;
                            debug!(last_partition_offset, "No consumer offset, updating last_partition_offset to partition_offset_update");
                            continue;
                        },
                    };

                    // If the leader offset update is behind the last offset read by
                    // the consumer, then we still have nothing new to send the consumer.
                    // Just save the leader offset and continue waiting for events.
                    if partition_offset_update <= last_consumer_offset {
                        debug!(partition_offset_update, "Leader offset update, but the consumer is already ahead");
                        last_partition_offset = partition_offset_update;
                        continue;
                    }

                    // We need to send the consumer all records since the last consumer offset
                    debug!(partition_offset_update, last_consumer_offset, "reading offset event");
                    let (offset, wait) = self.send_back_records(last_consumer_offset, smartmodule_instance.as_mut()).await?;
                    last_partition_offset = offset;
                    if wait {
                        last_known_consumer_offset = None;
                        debug!(
                            last_partition_offset,
                            ?last_known_consumer_offset,
                            "Finished handling partition_offset_update, waiting for consumer",
                        );
                    } else {
                        last_known_consumer_offset = Some(last_partition_offset);
                        debug!(?last_known_consumer_offset, "Finished handling partition_offset_update, not waiting for consumer");
                    }
                },

            }
        }

        debug!("done with stream fetch loop exiting");

        Ok(())
    }

    /// send back records back to consumer
    /// return (next offset, consumer wait)
    //  consumer wait flag tells that there are records send back to consumer
    #[instrument(
        skip(self, sm_chain),
        fields(stream_id = self.stream_id)
    )]
    async fn send_back_records(
        &mut self,
        starting_offset: Offset,
        sm_chain: Option<&mut SmartModuleChainInstance>,
    ) -> Result<(Offset, bool), StreamFetchError> {
        let now = Instant::now();

        let mut file_partition_response = FilePartitionResponse {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        // Read records from the leader starting from `offset`
        // Returns with the HW/LEO of the latest records available in the leader
        // This describes the range of records that can be read in this request
        let read_end_offset = match self
            .leader_state
            .read_records(starting_offset, self.max_fetch_bytes, self.isolation)
            .await
        {
            Ok(slice) => {
                file_partition_response.high_watermark = slice.end.hw;
                file_partition_response.log_start_offset = slice.start;

                if let Some(file_slice) = slice.file_slice {
                    file_partition_response.records = file_slice.into();
                }
                slice.end
            }
            Err(err) => {
                debug!(%err,"error reading records from leader");
                return Err(err.into());
            }
        };

        debug!(
            hw = read_end_offset.hw,
            leo = read_end_offset.leo,
            slice_start = file_partition_response.records.position(),
            slice_len = file_partition_response.records.len(),
            read_records_ms = %now.elapsed().as_millis(),
            "Starting send_back_records",
        );

        let next_offset = read_end_offset.isolation(&self.isolation);

        // We were unable to read any records from this starting offset,
        // therefore the next offset we should try to read is the same starting offset
        if file_partition_response.records.len() == 0 {
            debug!("empty records, skipping");
            return Ok((starting_offset, false));
        }

        let (offset, wait, metrics_update) = match sm_chain {
            Some(chain) => {
                // If a SmartModule is provided, we need to read records from file to memory
                // In-memory records are then processed by SmartModule and returned to consumer

                let records = &file_partition_response.records;
                let mut file_batch_iterator =
                    FileBatchIterator::from_raw_slice(records.raw_slice());

                let (batch, smartmodule_error) = process_batch(
                    chain,
                    &mut file_batch_iterator,
                    self.max_bytes as usize,
                    self.metrics.chain_metrics(),
                )
                .map_err(|err| {
                    StreamFetchError::Fetch(ErrorCode::Other(format!("SmartModule err {err}")))
                })?;
                let metrics_update = IncreaseValue::from(&batch);

                let (offset, wait) = self
                    .send_processed_response(
                        file_partition_response,
                        next_offset,
                        batch,
                        smartmodule_error,
                    )
                    .await?;
                (offset, wait, metrics_update)
            }
            None => {
                // If no SmartModule is provided, respond using raw file records
                debug!("No SmartModule, sending back entire log");
                let metrics_update = IncreaseValue::from(&file_partition_response);

                let response = StreamFetchResponse {
                    topic: self.replica.topic.clone(),
                    stream_id: self.stream_id,
                    partition: file_partition_response,
                };

                let response_msg = RequestMessage::<FileStreamFetchRequest>::response_with_header(
                    &self.header,
                    response,
                );

                trace!("sending back file fetch response msg: {:#?}", response_msg);

                let mut inner_sink = self.sink.lock().await;
                inner_sink
                    .encode_file_slices(&response_msg, self.header.api_version())
                    .await?;

                drop(inner_sink);

                debug!(read_time_ms = %now.elapsed().as_millis(),"finish sending back records");

                (
                    read_end_offset.isolation(&self.isolation),
                    true,
                    metrics_update,
                )
            }
        };
        self.metrics
            .outbound()
            .increase_by_value(self.header.is_connector(), metrics_update);
        Ok((offset, wait))
    }

    #[instrument(skip(self, file_partition_response, batch, smartmodule_error))]
    async fn send_processed_response(
        &self,
        file_partition_response: FilePartitionResponse,
        next_offset: Offset,
        batch: Batch,
        smartmodule_error: Option<SmartModuleTransformRuntimeError>,
    ) -> Result<(Offset, bool), StreamFetchError> {
        type DefaultPartitionResponse = FetchablePartitionResponse<RecordSet<RawRecords>>;

        let error_code = match smartmodule_error {
            Some(error) => ErrorCode::SmartModuleRuntimeError(error),
            None => file_partition_response.error_code,
        };
        trace!(?error_code, "SmartModule error code output:");

        let has_error = !matches!(error_code, ErrorCode::None);
        let has_records = !batch.records().is_empty();

        if !has_records && !has_error {
            debug!(next_offset, "No records to send back, skipping");
            return Ok((next_offset, false));
        }

        let next_filter_offset = if has_records {
            trace!(?batch, "SmartModule batch:");
            batch.get_last_offset() + 1
        } else {
            next_offset
        };

        debug!(
            next_offset,
            records = batch.records().len(),
            "sending back to consumer"
        );

        //trace!("batch: {:#?}",batch);

        let records = RecordSet::default().add(batch);
        let partition_response = DefaultPartitionResponse {
            partition_index: self.replica.partition,
            error_code,
            high_watermark: file_partition_response.high_watermark,
            log_start_offset: file_partition_response.log_start_offset,
            records: records.try_into()?,
            next_filter_offset,
            // we mark last offset in the response that we should sync up
            ..Default::default()
        };

        let stream_response = StreamFetchResponse {
            topic: self.replica.topic.clone(),
            stream_id: self.stream_id,
            partition: partition_response,
        };

        let response_msg = RequestMessage::<DefaultStreamFetchRequest>::response_with_header(
            &self.header,
            stream_response,
        );

        trace!("Sending SmartModule response: {:#?}", response_msg);

        let mut inner_sink = self.sink.lock().await;
        inner_sink
            .send_response(&response_msg, self.header.api_version())
            .await?;

        Ok((next_offset, true))
    }
}

async fn send_back_error(
    sink: &ExclusiveFlvSink,
    replica: &ReplicaKey,
    header: &RequestHeader,
    stream_id: u32,
    error_code: ErrorCode,
) -> Result<(), SocketError> {
    type DefaultPartitionResponse = FetchablePartitionResponse<RecordSet<RawRecords>>;
    let partition_response = DefaultPartitionResponse {
        error_code,
        partition_index: replica.partition,
        ..Default::default()
    };

    let stream_response = StreamFetchResponse {
        topic: replica.topic.clone(),
        stream_id,
        partition: partition_response,
    };

    let response_msg =
        RequestMessage::<DefaultStreamFetchRequest>::response_with_header(header, stream_response);

    {
        let mut inner_sink = sink.lock().await;
        inner_sink
            .send_response(&response_msg, header.api_version())
            .await?;
    }

    Ok(())
}

enum StreamFetchError {
    Compression(CompressionError),
    Socket(SocketError),
    Fetch(ErrorCode),
}

impl From<SocketError> for StreamFetchError {
    fn from(err: SocketError) -> Self {
        StreamFetchError::Socket(err)
    }
}

impl From<ErrorCode> for StreamFetchError {
    fn from(err: ErrorCode) -> Self {
        StreamFetchError::Fetch(err)
    }
}

impl From<CompressionError> for StreamFetchError {
    fn from(err: CompressionError) -> Self {
        Self::Compression(err)
    }
}
pub mod publishers {

    use std::{collections::HashMap, sync::Arc};
    use std::fmt::Debug;
    use std::ops::AddAssign;

    use super::OffsetPublisher;

    pub const INIT_OFFSET: i64 = -1;

    pub struct StreamPublishers {
        publishers: HashMap<u32, Arc<OffsetPublisher>>,
        stream_id_seq: u32,
    }

    impl Debug for StreamPublishers {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "stream {}", self.stream_id_seq)
        }
    }

    impl StreamPublishers {
        pub(crate) fn new() -> Self {
            Self {
                publishers: HashMap::new(),
                stream_id_seq: 0,
            }
        }

        // get next stream id
        fn next_stream_id(&mut self) -> u32 {
            self.stream_id_seq.add_assign(1);
            self.stream_id_seq
        }

        pub async fn create_new_publisher(&mut self) -> (u32, Arc<OffsetPublisher>) {
            let stream_id = self.next_stream_id();
            let offset_publisher = OffsetPublisher::shared(INIT_OFFSET);
            self.publishers.insert(stream_id, offset_publisher.clone());
            (stream_id, offset_publisher)
        }

        /// get publisher with stream id
        pub async fn get_publisher(&self, stream_id: u32) -> Option<Arc<OffsetPublisher>> {
            self.publishers.get(&stream_id).cloned()
        }
    }
}
