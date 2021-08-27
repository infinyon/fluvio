use std::sync::Arc;
use std::time::{Instant};
use std::io::ErrorKind;
use std::io::Error as IoError;

use tracing::{info, error, debug, trace, instrument};
use tokio::select;

use fluvio_types::event::{SimpleEvent, offsets::OffsetPublisher};
use fluvio_future::task::spawn;
use fluvio_socket::{ExclusiveFlvSink, SocketError};
use dataplane::{
    ErrorCode,
    api::{RequestMessage, RequestHeader},
    fetch::FetchablePartitionResponse,
    record::RecordSet,
    SmartStreamError,
};
use dataplane::{Offset, Isolation, ReplicaKey};
use dataplane::fetch::FilePartitionResponse;
use fluvio_spu_schema::server::stream_fetch::{
    FileStreamFetchRequest, DefaultStreamFetchRequest, StreamFetchResponse, SmartStreamKind,
};
use fluvio_types::event::offsets::OffsetChangeListener;

use crate::core::DefaultSharedGlobalContext;
use crate::replication::leader::SharedFileLeaderState;
use publishers::INIT_OFFSET;
use crate::smart_stream::{SmartStreamEngine, SmartStream};
use crate::smart_stream::file_batch::FileBatchIterator;
use dataplane::batch::Batch;
use dataplane::smartstream::SmartStreamRuntimeError;

/// Fetch records as stream
pub struct StreamFetchHandler {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    max_fetch_bytes: u32,
    header: RequestHeader,
    sink: ExclusiveFlvSink,
    end_event: Arc<SimpleEvent>,
    consumer_offset_listener: OffsetChangeListener,
    leader_state: SharedFileLeaderState,
    stream_id: u32,
}

impl StreamFetchHandler {
    /// handle fluvio continuous fetch request
    #[instrument(skip(request, ctx, sink, end_event))]
    pub async fn start(
        request: RequestMessage<FileStreamFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        sink: ExclusiveFlvSink,
        end_event: Arc<SimpleEvent>,
    ) -> Result<(), SocketError> {
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

            let sm_engine = SmartStreamEngine::default();
            debug!("Has WASM payload: {}", msg.wasm_payload.is_some());

            let smartstream = if let Some(payload) = msg.wasm_payload {
                let wasm = &payload.wasm.get_raw()?;
                let module = sm_engine.create_module_from_binary(wasm).map_err(|err| {
                    SocketError::Io(IoError::new(
                        ErrorKind::Other,
                        format!("module loading error {}", err),
                    ))
                })?;

                let smartstream = match payload.kind {
                    SmartStreamKind::Filter => {
                        debug!("Instantiating SmartStreamFilter");
                        let filter = module.create_filter(&sm_engine).map_err(|err| {
                            SocketError::Io(IoError::new(
                                ErrorKind::Other,
                                format!("Failed to instantiate SmartStreamFilter {}", err),
                            ))
                        })?;
                        SmartStream::Filter(filter)
                    }
                    SmartStreamKind::Map => {
                        debug!("Instantiating SmartStreamMap");
                        let map = module.create_map(&sm_engine).map_err(|err| {
                            SocketError::Io(IoError::new(
                                ErrorKind::Other,
                                format!("Failed to instantiate SmartStreamMap {}", err),
                            ))
                        })?;
                        SmartStream::Map(map)
                    }
                    SmartStreamKind::Aggregate { accumulator } => {
                        let aggregator =
                            module
                                .create_aggregate(&sm_engine, accumulator)
                                .map_err(|err| {
                                    SocketError::Io(IoError::new(
                                        ErrorKind::Other,
                                        format!("Failed to instantiate SmartStreamMap {}", err),
                                    ))
                                })?;
                        SmartStream::Aggregate(aggregator)
                    }
                };

                Some(smartstream)
            } else {
                None
            };

            // if we are filtered we should scan all batches instead of just limit to max bytes
            let max_fetch_bytes = if smartstream.is_none() {
                max_bytes
            } else {
                u32::MAX
            };

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
                max_fetch_bytes,
            };

            spawn(async move { handler.process(current_offset, smartstream).await });
            debug!("spawned stream fetch controller");
        } else {
            debug!(topic = %replica.topic," no leader founded, returning");
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

    #[instrument(
        skip(self, smartstream),
        name = "stream fetch",
        fields(
            replica = %self.replica,
            sink = self.sink.id()
        )
    )]
    async fn process(mut self, starting_offset: Offset, smartstream: Option<SmartStream>) {
        if let Err(err) = self.inner_process(starting_offset, smartstream).await {
            error!("error: {:#?}", err);
            self.end_event.notify();
        }
    }

    async fn inner_process(
        &mut self,
        starting_offset: Offset,
        mut smartstream: Option<SmartStream>,
    ) -> Result<(), SocketError> {
        let (mut last_partition_offset, consumer_wait) = self
            .send_back_records(starting_offset, smartstream.as_mut())
            .await?;

        let mut leader_offset_receiver = self.leader_state.offset_listener(&self.isolation);
        let mut counter: i32 = 0;
        // since we don't need to wait for consumer, can move consumer to same offset as last read
        let mut last_known_consumer_offset: Option<Offset> =
            (!consumer_wait).then(|| last_partition_offset);

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
                    if !(consumer_offset_update < last_partition_offset) {
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
                    let (offset, wait) = self.send_back_records(consumer_offset_update, smartstream.as_mut()).await?;
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
                    let (offset, wait) = self.send_back_records(last_consumer_offset, smartstream.as_mut()).await?;
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
        self.ctx
            .stream_publishers()
            .remove_publisher(self.stream_id)
            .await;

        Ok(())
    }

    /// send back records back to consumer
    /// return (next offset, consumer wait)
    //  consumer wait flag tells that there are records send back to consumer
    #[instrument(
        skip(self, smartstream),
        fields(stream_id = self.stream_id)
    )]
    async fn send_back_records(
        &mut self,
        starting_offset: Offset,
        smartstream: Option<&mut SmartStream>,
    ) -> Result<(Offset, bool), SocketError> {
        let now = Instant::now();
        let mut file_partition_response = FilePartitionResponse {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        // Read records from the leader starting from `offset`
        // Returns with the HW/LEO of the latest records available in the leader
        // This describes the range of records that can be read in this request
        let read_end_offset = self
            .leader_state
            .read_records(
                starting_offset,
                self.max_fetch_bytes,
                self.isolation.clone(),
                &mut file_partition_response,
            )
            .await;

        debug!(
            hw = read_end_offset.hw,
            leo = read_end_offset.leo,
            slice_start = file_partition_response.records.position(),
            slice_end = file_partition_response.records.len(),
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

        // If a smartstream module is provided, we need to read records from file to memory
        // In-memory records are then processed by smartstream and returned to consumer
        match smartstream {
            Some(SmartStream::Filter(filter)) => {
                debug!("Handling SmartStreamFilter logic");

                let (batch, smartstream_error) = {
                    let records = &file_partition_response.records;
                    let mut file_batch_iterator =
                        FileBatchIterator::from_raw_slice(records.raw_slice());

                    // Input: FileBatch, Output: MemoryBatch post-filter
                    filter
                        .filter(&mut file_batch_iterator, self.max_bytes as usize)
                        .map_err(|err| {
                            IoError::new(ErrorKind::Other, format!("filter err {}", err))
                        })?
                };

                self.send_processed_response(
                    file_partition_response,
                    next_offset,
                    batch,
                    smartstream_error,
                )
                .await
            }
            Some(SmartStream::Map(map)) => {
                debug!("Handling SmartStreamMap logic");

                let (batch, smartstream_error) = {
                    let records = &file_partition_response.records;
                    let mut file_batch_iterator =
                        FileBatchIterator::from_raw_slice(records.raw_slice());

                    // Input: FileBatch, Output: MemoryBatch post-filter
                    map.map(&mut file_batch_iterator, self.max_bytes as usize)
                        .map_err(|err| IoError::new(ErrorKind::Other, format!("map err {}", err)))?
                };

                self.send_processed_response(
                    file_partition_response,
                    next_offset,
                    batch,
                    smartstream_error,
                )
                .await
            }
            Some(SmartStream::Aggregate(aggregator)) => {
                info!("Creating Smart Aggregator");

                let records = &file_partition_response.records;
                let slice = records.raw_slice();
                let mut file_batch_iterator = FileBatchIterator::from_raw_slice(slice);

                let (batch, smartstream_error) = aggregator
                    .aggregate(&mut file_batch_iterator, self.max_bytes as usize)
                    .map_err(|err| {
                        IoError::new(ErrorKind::Other, format!("aggregate err: {}", err))
                    })?;

                self.send_processed_response(
                    file_partition_response,
                    next_offset,
                    batch,
                    smartstream_error,
                )
                .await
            }
            None => {
                // If no smartstream is provided, respond using raw file records
                debug!("No SmartStream, sending back entire log");

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

                Ok((read_end_offset.isolation(&self.isolation), true))
            }
        }
    }

    #[instrument(skip(self, file_partition_response, batch, smartstream_error))]
    async fn send_processed_response(
        &self,
        file_partition_response: FilePartitionResponse,
        mut next_offset: Offset,
        batch: Batch,
        smartstream_error: Option<SmartStreamRuntimeError>,
    ) -> Result<(Offset, bool), SocketError> {
        type DefaultPartitionResponse = FetchablePartitionResponse<RecordSet>;

        let error_code = match smartstream_error {
            Some(error) => ErrorCode::SmartStreamError(SmartStreamError::Runtime(error)),
            None => file_partition_response.error_code,
        };
        trace!(?error_code, "Smartstream error code output:");

        let has_error = !matches!(error_code, ErrorCode::None);
        let has_records = !batch.records().is_empty();

        if !has_records && !has_error {
            debug!(next_offset, "No records to send back, skipping");
            return Ok((next_offset, false));
        }

        if has_records {
            trace!(?batch, "Smartstream batch:");
            next_offset = batch.get_last_offset() + 1;
        }

        debug!(
            next_offset,
            records = batch.records().len(),
            "sending back to consumer"
        );
        let records = RecordSet::default().add(batch);
        let partition_response = DefaultPartitionResponse {
            partition_index: self.replica.partition,
            error_code,
            high_watermark: file_partition_response.high_watermark,
            log_start_offset: file_partition_response.log_start_offset,
            records,
            next_filter_offset: next_offset,
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

        trace!("Sending SmartStream response: {:#?}", response_msg);

        let mut inner_sink = self.sink.lock().await;
        inner_sink
            .send_response(&response_msg, self.header.api_version())
            .await?;

        Ok((next_offset, true))
    }
}

pub mod publishers {

    use std::{
        collections::HashMap,
        sync::{Arc, atomic::AtomicU32},
    };
    use std::sync::atomic::Ordering::SeqCst;
    use std::fmt::Debug;

    use async_lock::Mutex;
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

#[cfg(test)]
mod test {

    use std::{
        path::{Path, PathBuf},
        time::Duration,
        env::temp_dir,
    };

    use fluvio_controlplane_metadata::partition::Replica;
    use flv_util::fixture::ensure_clean_dir;
    use futures_util::StreamExt;

    use fluvio_future::timer::sleep;
    use fluvio_socket::{FluvioSocket, MultiplexerSocket};
    use dataplane::{
        Isolation,
        fixture::BatchProducer,
        record::{RecordData, Record},
    };
    use dataplane::fixture::{create_batch, TEST_RECORD};
    use fluvio_spu_schema::server::update_offset::{UpdateOffsetsRequest, OffsetUpdate};
    use fluvio_spu_schema::server::stream_fetch::SmartStreamWasm;
    use crate::core::GlobalContext;
    use crate::config::SpuConfig;
    use crate::replication::leader::LeaderReplicaState;
    use crate::services::create_public_server;
    use super::*;
    use fluvio_spu_schema::server::stream_fetch::SmartStreamPayload;
    use dataplane::smartstream::SmartStreamType;

    #[fluvio_future::test(ignore)]
    async fn test_stream_fetch() {
        let test_path = temp_dir().join("test_stream_fetch");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12000";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        // perform for two versions
        for version in 10..11 {
            let topic = format!("test{}", version);
            let test = Replica::new((topic.clone(), 0), 5001, vec![5001]);
            let test_id = test.id.clone();
            let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
                .await
                .expect("replica");
            ctx.leaders_state().insert(test_id, replica.clone());

            let stream_request = DefaultStreamFetchRequest {
                topic: topic.clone(),
                partition: 0,
                fetch_offset: 0,
                isolation: Isolation::ReadUncommitted,
                max_bytes: 1000,
                ..Default::default()
            };

            let mut stream = client_socket
                .create_stream(RequestMessage::new_request(stream_request), version)
                .await
                .expect("create stream");

            let mut records = RecordSet::default().add(create_batch());
            // write records, base offset = 0 since we are starting from 0
            replica
                .write_record_set(&mut records, ctx.follower_notifier())
                .await
                .expect("write");

            let response = stream.next().await.expect("first").expect("response");
            debug!("response: {:#?}", response);
            let stream_id = response.stream_id;
            {
                debug!("received first message");
                assert_eq!(response.topic, topic);

                let partition = &response.partition;
                assert_eq!(partition.error_code, ErrorCode::None);
                assert_eq!(partition.high_watermark, 2);
                assert_eq!(partition.next_offset_for_fetch(), Some(2)); // shoule be same as HW

                assert_eq!(partition.records.batches.len(), 1);
                let batch = &partition.records.batches[0];
                assert_eq!(batch.base_offset, 0);
                assert_eq!(batch.get_last_offset(), 1);
                assert_eq!(batch.records().len(), 2);
                assert_eq!(batch.records()[0].value().as_ref(), TEST_RECORD);
                assert_eq!(batch.records()[1].value().as_ref(), TEST_RECORD);
                assert_eq!(batch.records()[1].get_offset_delta(), 1);
            }

            drop(response);

            // consumer can send back to same offset to read back again
            debug!("send back offset ack to SPU");
            client_socket
                .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
                    offsets: vec![OffsetUpdate {
                        offset: 1,
                        session_id: stream_id,
                    }],
                }))
                .await
                .expect("send offset");

            let response = stream.next().await.expect("2nd").expect("response");
            {
                debug!("received 2nd message");
                assert_eq!(response.topic, topic);
                let partition = &response.partition;
                assert_eq!(partition.error_code, ErrorCode::None);
                assert_eq!(partition.high_watermark, 2);
                assert_eq!(partition.next_offset_for_fetch(), Some(2)); // shoule be same as HW

                // we got whole batch rather than individual batches
                assert_eq!(partition.records.batches.len(), 1);
                let batch = &partition.records.batches[0];
                assert_eq!(batch.base_offset, 0);
                assert_eq!(batch.get_last_offset(), 1);
                assert_eq!(batch.records().len(), 2);
                assert_eq!(batch.records()[0].value().as_ref(), TEST_RECORD);
                assert_eq!(batch.records()[1].value().as_ref(), TEST_RECORD);
            }

            drop(response);

            // send back that consume has processed all current bacthes
            client_socket
                .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
                    offsets: vec![OffsetUpdate {
                        offset: 2,
                        session_id: stream_id,
                    }],
                }))
                .await
                .expect("send offset");

            debug!("writing 2nd batch");
            // base offset should be 2
            replica
                .write_record_set(&mut records, ctx.follower_notifier())
                .await
                .expect("write");
            assert_eq!(replica.hw(), 4);

            let response = stream.next().await.expect("first").expect("response");
            debug!("received 3nd response");
            assert_eq!(response.stream_id, stream_id);
            assert_eq!(response.topic, topic);

            {
                let partition = &response.partition;
                assert_eq!(partition.error_code, ErrorCode::None);
                assert_eq!(partition.high_watermark, 4);

                assert_eq!(partition.next_offset_for_fetch(), Some(4));
                assert_eq!(partition.records.batches.len(), 1);
                let batch = &partition.records.batches[0];
                assert_eq!(batch.base_offset, 2);
                assert_eq!(batch.get_last_offset(), 3);
                assert_eq!(batch.records().len(), 2);
                assert_eq!(batch.records()[0].value().as_ref(), TEST_RECORD);
                assert_eq!(batch.records()[1].value().as_ref(), TEST_RECORD);
            }
        }

        server_end_event.notify();
        debug!("terminated controller");
    }

    fn read_filter_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
        let path = filter_path.as_ref();
        std::fs::read(path).unwrap_or_else(|_| panic!("Unable to read file {}", path.display()))
    }

    fn load_wasm_module(module_name: &str) -> Vec<u8> {
        let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
        let wasm_path = PathBuf::from(spu_dir)
            .parent()
            .expect("parent")
            .join(format!(
                "fluvio-smartstream/examples/target/wasm32-unknown-unknown/debug/{}.wasm",
                module_name
            ));
        read_filter_from_path(wasm_path)
    }

    #[fluvio_future::test(ignore)]
    async fn test_stream_fetch_filter() {
        let test_path = temp_dir().join("test_stream_fetch_filter");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12001";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "testfilter";

        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        let wasm = load_wasm_module("fluvio_wasm_filter");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Filter,
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        // 1 out of 2 are filtered
        let mut records = create_filter_records(2);
        //debug!("records: {:#?}", records);
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        debug!("first filter fetch");
        let response = stream.next().await.expect("first").expect("response");
        //debug!("respose: {:#?}", response);
        let stream_id = response.stream_id;
        {
            debug!("received first message");
            assert_eq!(response.topic, topic);

            let partition = &response.partition;
            assert_eq!(partition.error_code, ErrorCode::None);
            assert_eq!(partition.high_watermark, 2);
            assert_eq!(partition.next_offset_for_fetch(), Some(2)); // shoule be same as HW

            assert_eq!(partition.records.batches.len(), 1);
            let batch = &partition.records.batches[0];
            assert_eq!(batch.base_offset, 0);
            assert_eq!(batch.records().len(), 1);
            assert_eq!(
                batch.records()[0].value().as_ref(),
                "a".repeat(100).as_bytes()
            );
            assert_eq!(batch.records()[0].get_offset_delta(), 1);
        }

        drop(response);

        // firt write 2 non filterable records
        let mut records = RecordSet::default().add(create_batch());
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        // another 1 of 3, here base offset should be = 4
        let mut records = create_filter_records(3);
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        // create another 4, base should be 4 + 3 = 7 and total 10 records
        let mut records = create_filter_records(3);
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");
        assert_eq!(replica.hw(), 10);

        debug!("2nd filter batch, hw=10");
        // consumer can send back to same offset to read back again
        debug!("send back offset ack to SPU");
        client_socket
            .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
                offsets: vec![OffsetUpdate {
                    offset: 2,
                    session_id: stream_id,
                }],
            }))
            .await
            .expect("send offset");

        let response = stream.next().await.expect("2nd").expect("response");
        {
            debug!("received 2nd message");
            assert_eq!(response.topic, topic);
            let partition = &response.partition;
            assert_eq!(partition.error_code, ErrorCode::None);
            assert_eq!(partition.high_watermark, 10);
            assert_eq!(partition.next_offset_for_fetch(), Some(10)); // shoule be same as HW

            // we got whole batch rather than individual batches
            assert_eq!(partition.records.batches.len(), 1);
            let batch = &partition.records.batches[0];
            assert_eq!(batch.base_offset, 4); // first base offset where we had filtered records
            assert_eq!(batch.records().len(), 2);
            assert_eq!(
                batch.records()[0].value().as_ref(),
                "a".repeat(100).as_bytes()
            );
        }

        drop(response);

        server_end_event.notify();
        debug!("terminated controller");
    }

    #[fluvio_future::test(ignore)]
    async fn test_stream_fetch_filter_individual() {
        let test_path = temp_dir().join("test_stream_fetch_filter_individual");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12002";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        let topic = "testfilter";
        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        let wasm = load_wasm_module("fluvio_wasm_filter_odd");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Filter,
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        // First, open the consumer stream
        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        let mut records: RecordSet = BatchProducer::builder()
            .records(1u16)
            .record_generator(Arc::new(|_, _| Record::new("1")))
            .build()
            .expect("batch")
            .records();
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        tokio::select! {
            _ = stream.next() => panic!("Should not receive response here"),
            _ = fluvio_future::timer::sleep(std::time::Duration::from_millis(1000)) => (),
        }

        let mut records: RecordSet = BatchProducer::builder()
            .records(1u16)
            .record_generator(Arc::new(|_, _| Record::new("2")))
            .build()
            .expect("batch")
            .records();
        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        let response = stream.next().await.expect("first").expect("response");
        let records = response.partition.records.batches[0].records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref(), "2".as_bytes());

        match response.partition.error_code {
            ErrorCode::None => (),
            _ => panic!("Should not have gotten an error"),
        }

        drop(response);

        server_end_event.notify();
        debug!("terminated controller");
    }

    #[fluvio_future::test(ignore)]
    async fn test_stream_filter_error_fetch() {
        let test_path = temp_dir().join("test_stream_filter_error_fetch");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12003";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "test_filter_error";

        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        let wasm = load_wasm_module("fluvio_wasm_filter_odd");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Filter,
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
            let value = if record_index < 10 {
                record_index.to_string()
            } else {
                "ten".to_string()
            };

            Record::new(value)
        }

        let mut records: RecordSet = BatchProducer::builder()
            .records(11u16)
            .record_generator(Arc::new(generate_record))
            .build()
            .expect("batch")
            .records();

        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        debug!("first filter fetch");
        let response = stream.next().await.expect("first").expect("response");

        assert_eq!(response.partition.records.batches.len(), 1);
        let records = response.partition.records.batches[0].records();
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].value.as_ref(), "0".as_bytes());
        assert_eq!(records[1].value.as_ref(), "2".as_bytes());
        assert_eq!(records[2].value.as_ref(), "4".as_bytes());
        assert_eq!(records[3].value.as_ref(), "6".as_bytes());
        assert_eq!(records[4].value.as_ref(), "8".as_bytes());

        match &response.partition.error_code {
            ErrorCode::SmartStreamError(SmartStreamError::Runtime(error)) => {
                assert_eq!(error.offset, 10);
                assert!(error.record_key.is_none());
                assert_eq!(error.record_value.as_ref(), "ten".as_bytes());
                assert_eq!(error.kind, SmartStreamType::Filter);
                let rendered = format!("{}", error);
                assert_eq!(rendered, "Oops something went wrong\n\nCaused by:\n   0: Failed to parse int\n   1: invalid digit found in string\n\nSmartStream Info: \n    Type: Filter\n    Offset: 10\n    Key: NULL\n    Value: ten");
            }
            _ => panic!("should have gotten error code"),
        }

        drop(response);

        server_end_event.notify();
        debug!("terminated controller");
    }

    fn generate_record(record_index: usize, _producer: &BatchProducer) -> Record {
        let msg = match record_index {
            0 => "b".repeat(100),
            1 => "a".repeat(100),
            _ => "z".repeat(100),
        };

        Record::new(RecordData::from(msg))
    }

    /// create records that can be filtered
    fn create_filter_records(records: u16) -> RecordSet {
        BatchProducer::builder()
            .records(records)
            .record_generator(Arc::new(generate_record))
            .build()
            .expect("batch")
            .records()
    }

    /// test filter with max bytes
    #[fluvio_future::test(ignore)]
    async fn test_stream_filter_max() {
        let test_path = temp_dir().join("test_stream_filter_max");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12005";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "testfilter";

        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        // write 2 batches each with 10 records
        //debug!("records: {:#?}", records);
        replica
            .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
            .await
            .expect("write"); // 1000 bytes
        replica
            .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
            .await
            .expect("write"); // 2000 bytes totals
        replica
            .write_record_set(&mut create_filter_records(10), ctx.follower_notifier())
            .await
            .expect("write"); // 3000 bytes total
                              // now total of 300 filter records bytes (min), but last filter record is greater than max

        let wasm = load_wasm_module("fluvio_wasm_filter");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Filter,
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 250,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        let response = stream.next().await.expect("first").expect("response");
        debug!("respose: {:#?}", response);

        // received partial because we exceed max bytes
        let stream_id = response.stream_id;
        {
            debug!("received first message");
            assert_eq!(response.topic, topic);

            let partition = &response.partition;
            assert_eq!(partition.error_code, ErrorCode::None);
            assert_eq!(partition.high_watermark, 30);
            assert_eq!(partition.next_offset_for_fetch(), Some(20)); // shoule be same as HW

            assert_eq!(partition.records.batches.len(), 1);
            let batch = &partition.records.batches[0];
            assert_eq!(batch.base_offset, 0);
            assert_eq!(batch.records().len(), 2);
            assert_eq!(
                batch.records()[0].value().as_ref(),
                "a".repeat(100).as_bytes()
            );
        }

        drop(response);

        // consumer can send back to same offset to read back again
        debug!("send back offset ack to SPU");
        client_socket
            .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
                offsets: vec![OffsetUpdate {
                    offset: 20,
                    session_id: stream_id,
                }],
            }))
            .await
            .expect("send offset");

        let response = stream.next().await.expect("2nd").expect("response");
        {
            debug!("received 2nd message");
            assert_eq!(response.topic, topic);
            let partition = &response.partition;
            assert_eq!(partition.error_code, ErrorCode::None);
            assert_eq!(partition.high_watermark, 30);
            assert_eq!(partition.next_offset_for_fetch(), Some(30)); // shoule be same as HW

            // we got whole batch rather than individual batches
            assert_eq!(partition.records.batches.len(), 1);
            let batch = &partition.records.batches[0];
            assert_eq!(batch.base_offset, 20);
            assert_eq!(batch.records().len(), 1);
            assert_eq!(
                batch.records()[0].value().as_ref(),
                "a".repeat(100).as_bytes()
            );
        }

        drop(response);

        server_end_event.notify();
    }

    #[fluvio_future::test(ignore)]
    async fn test_stream_fetch_map_error() {
        let test_path = temp_dir().join("test_stream_fetch_map_error");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12006";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "test_map_error";
        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        let wasm = load_wasm_module("fluvio_wasm_map_double");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Map,
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        let mut records: RecordSet = BatchProducer::builder()
            .records(10u16)
            .record_generator(Arc::new(|i, _| {
                if i < 9 {
                    Record::new(i.to_string())
                } else {
                    Record::new("nine".to_string())
                }
            }))
            .build()
            .expect("batch")
            .records();

        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        debug!("first map fetch");
        let response = stream.next().await.expect("first").expect("response");

        assert_eq!(response.partition.records.batches.len(), 1);
        let records = response.partition.records.batches[0].records();
        assert_eq!(records.len(), 9);
        assert_eq!(records[0].value.as_ref(), "0".as_bytes());
        assert_eq!(records[1].value.as_ref(), "2".as_bytes());
        assert_eq!(records[2].value.as_ref(), "4".as_bytes());
        assert_eq!(records[3].value.as_ref(), "6".as_bytes());
        assert_eq!(records[4].value.as_ref(), "8".as_bytes());
        assert_eq!(records[5].value.as_ref(), "10".as_bytes());
        assert_eq!(records[6].value.as_ref(), "12".as_bytes());
        assert_eq!(records[7].value.as_ref(), "14".as_bytes());
        assert_eq!(records[8].value.as_ref(), "16".as_bytes());

        match &response.partition.error_code {
            ErrorCode::SmartStreamError(SmartStreamError::Runtime(error)) => {
                assert_eq!(error.offset, 9);
                assert_eq!(error.kind, SmartStreamType::Map);
                assert_eq!(error.record_value.as_ref(), "nine".as_bytes());
            }
            _ => panic!("should get runtime error"),
        }

        drop(response);

        server_end_event.notify();
        debug!("terminated controller");
    }

    #[fluvio_future::test(ignore)]
    async fn test_stream_aggregate_fetch() {
        let test_path = temp_dir().join("aggregate_stream_fetch");
        ensure_clean_dir(&test_path);

        let addr = "127.0.0.1:12007";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = test_path;
        let ctx = GlobalContext::new_shared_context(spu_config);
        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            MultiplexerSocket::new(FluvioSocket::connect(addr).await.expect("connect"));

        let topic = "testaggregate";
        let test = Replica::new((topic.to_owned(), 0), 5001, vec![5001]);
        let test_id = test.id.clone();
        let replica = LeaderReplicaState::create(test, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        // Providing an accumulator causes SPU to run aggregator
        let wasm = load_wasm_module("fluvio_wasm_aggregate");
        let wasm_payload = SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(wasm),
            kind: SmartStreamKind::Aggregate {
                accumulator: Vec::from("789".repeat(100)),
            },
        };

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module: Vec::new(),
            wasm_payload: Some(wasm_payload),
            ..Default::default()
        };

        // Aggregate 10 records
        // These records look like:
        //
        // 000000000000000000... x100
        // 111111111111111111... x100
        // 222222222222222222... x100
        let mut records = BatchProducer::builder()
            .records(10u16)
            .record_generator(Arc::new(|i, _| Record::new(i.to_string().repeat(100))))
            .build()
            .expect("batch")
            .records();
        debug!("records: {:#?}", records);

        let mut stream = client_socket
            .create_stream(RequestMessage::new_request(stream_request), 11)
            .await
            .expect("create stream");

        replica
            .write_record_set(&mut records, ctx.follower_notifier())
            .await
            .expect("write");

        debug!("first aggregate fetch");
        let response = stream.next().await.expect("first").expect("response");
        let stream_id = response.stream_id;

        {
            debug!("received first message");
            assert_eq!(response.topic, topic);

            let partition = &response.partition;
            assert_eq!(partition.error_code, ErrorCode::None);
            assert_eq!(partition.high_watermark, 10);
            assert_eq!(partition.next_offset_for_fetch(), Some(10)); // shoule be same as HW

            assert_eq!(partition.records.batches.len(), 1);
            let batch = &partition.records.batches[0];
            assert_eq!(batch.base_offset, 0);
            assert_eq!(batch.records().len(), 10);

            let mut accumulator = "789".repeat(100);
            for i in 0..10 {
                accumulator.push_str(&i.to_string().repeat(100));
                assert_eq!(batch.records()[i].value().as_ref(), accumulator.as_bytes());
                assert_eq!(batch.records()[i].get_offset_delta(), i as i64);
            }
        }

        // consumer can send back to same offset to read back again
        debug!("send back offset ack to SPU");
        client_socket
            .send_and_receive(RequestMessage::new_request(UpdateOffsetsRequest {
                offsets: vec![OffsetUpdate {
                    offset: 20,
                    session_id: stream_id,
                }],
            }))
            .await
            .expect("send offset");

        server_end_event.notify();
    }
}
