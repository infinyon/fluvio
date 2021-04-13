use std::sync::Arc;
use std::time::{Instant};
use std::io::ErrorKind;
use std::io::Error as IoError;

use tracing::{debug, trace, error};
use tracing::instrument;
use futures_util::io::{AsyncRead, AsyncWrite};
use tokio::select;

use fluvio_types::event::{SimpleEvent, offsets::OffsetPublisher};
use fluvio_future::zero_copy::ZeroCopyWrite;
use fluvio_future::task::spawn;
use fluvio_socket::{InnerFlvSink, InnerExclusiveFlvSink, FlvSocketError};
use dataplane::{
    ErrorCode,
    api::{RequestMessage, RequestHeader},
    fetch::FetchablePartitionResponse,
    record::RecordSet,
};
use dataplane::{Offset, Isolation, ReplicaKey};
use dataplane::fetch::FilePartitionResponse;
use fluvio_spu_schema::server::stream_fetch::{
    FileStreamFetchRequest, DefaultStreamFetchRequest, StreamFetchResponse,
};
use fluvio_types::event::offsets::OffsetChangeListener;

use crate::core::DefaultSharedGlobalContext;
use crate::replication::leader::SharedFileLeaderState;
use crate::smart_stream::filter::{SmartStreamModule, SmartStreamEngine};
use publishers::INIT_OFFSET;

/// Fetch records as stream
pub struct StreamFetchHandler<S> {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    max_bytes: u32,
    max_fetch_bytes: u32,
    header: RequestHeader,
    sink: InnerExclusiveFlvSink<S>,
    end_event: Arc<SimpleEvent>,
    consumer_offset_listener: OffsetChangeListener,
    leader_state: SharedFileLeaderState,
    stream_id: u32,
    sm_engine: SmartStreamEngine,
    sm_bytes: Vec<u8>,
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
        let sm_bytes = msg.wasm_module;

        if let Some(leader_state) = ctx.leaders_state().get(&replica) {
            let (stream_id, offset_publisher) =
                ctx.stream_publishers().create_new_publisher().await;
            let offset_listener = offset_publisher.change_listner();

            debug!(

                sink = sink.id(),
                %replica,
                current_offset,
                max_bytes,
                sm_bytes = sm_bytes.len(),
                "start stream fetch"
            );

            // if we are filtered we should scan all batches instead of just limit to max bytes
            let max_fetch_bytes = if sm_bytes.is_empty() {
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
                sm_engine: SmartStreamEngine::new(),
                sm_bytes,
                max_fetch_bytes,
            };

            spawn(async move { handler.process(current_offset).await });
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
        // initialize smart stream module here instead of beginning because WASM module is not thread safe
        // and can't be send across Send
        let module = if !self.sm_bytes.is_empty() {
            Some(
                self.sm_engine
                    .create_module_from_binary(&self.sm_bytes)
                    .map_err(|err| -> FlvSocketError {
                        FlvSocketError::IoError(IoError::new(
                            ErrorKind::Other,
                            format!("module loading error {}", err),
                        ))
                    })?,
            )
        } else {
            None
        };

        let (mut last_read_offset, consumer_wait) = self
            .send_back_records(starting_offset, module.as_ref())
            .await?;

        let mut leader_offset_receiver = self.leader_state.offset_listener(&self.isolation);

        let mut counter: i32 = 0;
        let mut consumer_offset: Option<Offset> = if !consumer_wait {
            // since we don't need to wait for consumer, can move consumer to same offset as last read
            Some(last_read_offset)
        } else {
            None // offset for consumer
        };

        loop {
            counter += 1;

            debug!(counter, ?consumer_offset, last_read_offset, "waiting");

            select! {

                _ = self.end_event.listen() => {
                    debug!("end event has been received, terminating");
                    break;
                },


                changed_consumer_offset = self.consumer_offset_listener.listen() => {


                    if changed_consumer_offset != INIT_OFFSET  {

                        // consume hasn't read all offsets, need to send back gaps
                        if changed_consumer_offset < last_read_offset {
                            debug!(
                                changed_consumer_offset,
                                last_read_offset,
                                "need send back"
                            );
                            let (offset,wait) = self.send_back_records(changed_consumer_offset,module.as_ref()).await?;
                            last_read_offset = offset;
                            if wait {
                                consumer_offset = None;
                                debug!(
                                    last_read_offset,
                                    "wait for consumer"
                                );
                            } else {
                                consumer_offset = Some(last_read_offset);   // no need wait for consumer, skip it
                            }

                        } else {
                            debug!(
                                changed_consumer_offset,
                                last_read_offset,
                                "consume caught up"
                            );
                            consumer_offset = Some(changed_consumer_offset);
                        }
                    }
                },

                // received new offset from leader
                leader_offset_update = leader_offset_receiver.listen() => {

                    debug!(leader_offset_update);

                    if let Some(last_consumer_offset) = consumer_offset {
                        // we know what consumer offset is
                        if leader_offset_update > last_consumer_offset {
                            debug!(leader_offset_update,
                                consumer_offset = last_consumer_offset,
                                "reading offset event");
                            let (offset,wait) = self.send_back_records(last_consumer_offset,module.as_ref()).await?;
                            last_read_offset = offset;
                            if wait {
                                consumer_offset = None;
                                debug!(
                                    last_read_offset,
                                    "wait for consumer"
                                );
                            } else {
                                consumer_offset = Some(last_read_offset);   // no need wait for consumer, skip it
                            }


                        } else {
                            debug!(ignored_update_offset = leader_offset_update);
                            last_read_offset = leader_offset_update;
                        }
                    } else {
                        // we don't know consumer offset, so we delay
                        debug!(delay_consumer_offset = leader_offset_update);
                        last_read_offset = leader_offset_update;

                    }

                },
            }
        }

        debug!("done with stream fetch loop exiting");
        drop(module);

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
        skip(self,module_option),
        fields(stream_id = self.stream_id)
    )]
    async fn send_back_records(
        &mut self,
        offset: Offset,
        module_option: Option<&SmartStreamModule>,
    ) -> Result<(Offset, bool), FlvSocketError> {
        let now = Instant::now();
        let mut file_partition_response = FilePartitionResponse {
            partition_index: self.replica.partition,
            ..Default::default()
        };

        let (hw, leo) = self
            .leader_state
            .read_records(
                offset,
                self.max_fetch_bytes,
                self.isolation.clone(),
                &mut file_partition_response,
            )
            .await;

        debug!(
            hw = hw,
            leo = leo,
            slice_start = file_partition_response.records.position(),
            slice_end = file_partition_response.records.len(),
            read_records_ms = %now.elapsed().as_millis()
        );

        let mut next_offset = match self.isolation {
            Isolation::ReadCommitted => hw,
            Isolation::ReadUncommitted => leo,
        };

        if file_partition_response.records.len() > 0 {
            if let Some(module) = module_option {
                type DefaultPartitionResponse = FetchablePartitionResponse<RecordSet>;

                debug!("creating smart filter");
                let filter_batch = {
                    let filter = module.create_filter().map_err(|err| {
                        IoError::new(ErrorKind::Other, format!("creating filter {}", err))
                    })?;

                    let records = &file_partition_response.records;

                    filter
                        .filter(records.raw_slice(), self.max_bytes as usize)
                        .map_err(|err| {
                            IoError::new(ErrorKind::Other, format!("filter err {}", err))
                        })?
                };

                let consumer_wait = if !filter_batch.records().is_empty() {
                    trace!("filter batch: {:#?}", filter_batch);

                    next_offset = filter_batch.get_last_offset() + 1;

                    debug!(
                        next_offset,
                        records = filter_batch.records().len(),
                        "sending back to consumer"
                    );
                    let records = RecordSet::default().add(filter_batch);
                    let filter_partition_response = DefaultPartitionResponse {
                        partition_index: self.replica.partition,
                        error_code: file_partition_response.error_code,
                        high_watermark: file_partition_response.high_watermark,
                        log_start_offset: file_partition_response.log_start_offset,
                        records,
                        next_filter_offset: next_offset,
                        // we mark last offset in the response that we should sync up
                        ..Default::default()
                    };

                    let filter_response = StreamFetchResponse {
                        topic: self.replica.topic.clone(),
                        stream_id: self.stream_id,
                        partition: filter_partition_response,
                    };

                    let filter_response_msg =
                        RequestMessage::<DefaultStreamFetchRequest>::response_with_header(
                            &self.header,
                            filter_response,
                        );

                    trace!("sending back filter respone: {:#?}", filter_response_msg);

                    let mut inner_sink = self.sink.lock().await;
                    inner_sink
                        .send_response(&filter_response_msg, self.header.api_version())
                        .await?;
                    true
                } else {
                    debug!(next_offset, "filter, no records send back, skipping");
                    false
                };

                Ok((next_offset, consumer_wait))
            } else {
                debug!("no filter, sending back entire");

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

                let next_offset = match self.isolation {
                    Isolation::ReadCommitted => hw,
                    Isolation::ReadUncommitted => leo,
                };

                Ok((next_offset, true))
            }
        } else {
            debug!("empty records, skipping");

            let next_offset = match self.isolation {
                Isolation::ReadCommitted => hw,
                Isolation::ReadUncommitted => leo,
            };

            Ok((next_offset, false))
        }
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

#[cfg(test)]
mod test {

    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

    use fluvio_controlplane_metadata::partition::Replica;
    use flv_util::fixture::ensure_clean_dir;
    use futures_util::StreamExt;

    use fluvio_future::{test_async};
    use fluvio_future::timer::sleep;
    use fluvio_socket::{FlvSocket, DefaultMultiplexerSocket};
    use dataplane::{
        Isolation,
        fixture::BatchProducer,
        record::{DefaultAsyncBuffer, DefaultRecord},
    };
    use dataplane::fixture::{create_batch, TEST_RECORD};
    use fluvio_spu_schema::server::update_offset::{UpdateOffsetsRequest, OffsetUpdate};

    use crate::core::GlobalContext;
    use crate::config::SpuConfig;
    use crate::replication::leader::LeaderReplicaState;
    use crate::services::create_public_server;
    use super::*;

    #[test_async]
    async fn test_stream_fetch() -> Result<(), ()> {
        let test_path = "/tmp/stream_test";
        ensure_clean_dir(test_path);

        let addr = "127.0.0.1:12000";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = PathBuf::from(test_path);
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            DefaultMultiplexerSocket::new(FlvSocket::connect(addr).await.expect("connect"));

        // perform for two versions
        for version in 10..11 {
            let topic = format!("test{}", version);
            let test = Replica::new((topic.clone(), 0), 5001, vec![]);
            let test_id = test.id.clone();
            let (replica, _) = LeaderReplicaState::create(test, ctx.config())
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
            replica.write_record_set(&mut records).await.expect("write");

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
            replica.write_record_set(&mut records).await.expect("write");
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

        Ok(())
    }

    fn read_filter_from_path(filter_path: impl AsRef<Path>) -> Vec<u8> {
        std::fs::read(filter_path).expect("Unable to read file")
    }

    fn load_wasm_module(module_name: &str) -> Vec<u8> {
        let spu_dir = std::env::var("CARGO_MANIFEST_DIR").expect("target");
        let mut wasm_path = PathBuf::from(spu_dir)
            .parent()
            .expect("p")
            .parent()
            .expect("p")
            .to_owned();
        wasm_path.push("smart_filter");
        wasm_path.push("target");
        wasm_path.push("wasm32-unknown-unknown");
        wasm_path.push("release");
        wasm_path.push(format!("{}.wasm", module_name));
        debug!(?wasm_path);
        read_filter_from_path(wasm_path)
    }

    #[test_async]
    async fn test_stream_filter_fetch() -> Result<(), ()> {
        let test_path = "/tmp/filter_stream_fetch";
        ensure_clean_dir(test_path);

        let addr = "127.0.0.1:12001";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = PathBuf::from(test_path);
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            DefaultMultiplexerSocket::new(FlvSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "testfilter";

        let test = Replica::new((topic.to_owned(), 0), 5001, vec![]);
        let test_id = test.id.clone();
        let (replica, _) = LeaderReplicaState::create(test, ctx.config())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        let wasm_module = load_wasm_module("fluvio_filter_test");

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 10000,
            wasm_module,
            ..Default::default()
        };

        // 1 out of 2 are filtered
        let mut records = create_filter_records(2);
        //debug!("records: {:#?}", records);
        replica.write_record_set(&mut records).await.expect("write");

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
        replica.write_record_set(&mut records).await.expect("write");

        // another 1 of 3, here base offset should be = 4
        let mut records = create_filter_records(3);
        replica.write_record_set(&mut records).await.expect("write");

        // create another 4, base should be 4 + 3 = 7 and total 10 records
        let mut records = create_filter_records(3);
        replica.write_record_set(&mut records).await.expect("write");
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

        Ok(())
    }

    fn generate_record(record_index: usize, _producer: &BatchProducer) -> DefaultRecord {
        let msg = match record_index {
            0 => "b".repeat(100),
            1 => "a".repeat(100),
            _ => "z".repeat(100),
        };

        DefaultRecord {
            value: DefaultAsyncBuffer::new(msg.into_bytes()),
            ..Default::default()
        }
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
    #[test_async]
    async fn test_stream_filter_max() -> Result<(), ()> {
        let test_path = "/tmp/filter_stream_max";
        ensure_clean_dir(test_path);

        let addr = "127.0.0.1:12002";
        let mut spu_config = SpuConfig::default();
        spu_config.log.base_dir = PathBuf::from(test_path);
        let ctx = GlobalContext::new_shared_context(spu_config);

        let server_end_event = create_public_server(addr.to_owned(), ctx.clone()).run();

        // wait for stream controller async to start
        sleep(Duration::from_millis(100)).await;

        let client_socket =
            DefaultMultiplexerSocket::new(FlvSocket::connect(addr).await.expect("connect"));

        // perform for two versions

        let topic = "testfilter";

        let test = Replica::new((topic.to_owned(), 0), 5001, vec![]);
        let test_id = test.id.clone();
        let (replica, _) = LeaderReplicaState::create(test, ctx.config())
            .await
            .expect("replica");
        ctx.leaders_state().insert(test_id, replica.clone());

        // write 2 batches each with 10 records
        //debug!("records: {:#?}", records);
        replica
            .write_record_set(&mut create_filter_records(10))
            .await
            .expect("write"); // 1000 bytes
        replica
            .write_record_set(&mut create_filter_records(10))
            .await
            .expect("write"); // 2000 bytes totals
        replica
            .write_record_set(&mut create_filter_records(10))
            .await
            .expect("write"); // 3000 bytes total
                              // now total of 300 filter records bytes (min), but last filter record is greater than max

        let wasm_module = load_wasm_module("fluvio_filter_test");

        let stream_request = DefaultStreamFetchRequest {
            topic: topic.to_owned(),
            partition: 0,
            fetch_offset: 0,
            isolation: Isolation::ReadUncommitted,
            max_bytes: 250,
            wasm_module,
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
        debug!("terminated controller");

        Ok(())
    }
}
