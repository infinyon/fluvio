use std::sync::Arc;
use std::collections::HashMap;
use tracing::instrument;

mod partitioning;

use fluvio_types::{SpuId, PartitionId};
use dataplane::ReplicaKey;
use dataplane::produce::DefaultProduceRequest;
use dataplane::produce::DefaultPartitionRequest;
use dataplane::produce::DefaultTopicRequest;
use dataplane::batch::{Batch, MemoryRecords};
use dataplane::record::Record;
pub use dataplane::record::{RecordKey, RecordData};

use crate::FluvioError;
use crate::spu::SpuPool;
use crate::sync::StoreContext;
use crate::metadata::partition::PartitionSpec;
use crate::producer::partitioning::{Partitioner, SiphashRoundRobinPartitioner, PartitionerConfig};
use fluvio_spu_schema::server::stream_fetch::{SmartStreamPayload, SmartStreamWasm, SmartStreamKind};

/// An interface for producing events to a particular topic
///
/// A `TopicProducer` allows you to send events to the specific
/// topic it was initialized for. Once you have a `TopicProducer`,
/// you can send events to the topic, choosing which partition
/// each event should be delivered to.
pub struct TopicProducer {
    topic: String,
    pool: Arc<SpuPool>,
    partitioner: Box<dyn Partitioner + Send + Sync>,
    pub(crate) wasm_module: Option<SmartStreamPayload>,
}

#[cfg(feature = "smartengine")]
impl TopicProducer {
    /// Adds a SmartStream filter to this TopicProducer
    pub fn wasm_filter<T: Into<Vec<u8>>>(
        mut self,
        filter: T,
        params: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.wasm_module = Some(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(filter.into()),
            kind: SmartStreamKind::Filter,
            params: params.into(),
        });
        self
    }

    /// Adds a SmartStream map to this TopicProducer
    pub fn wasm_map<T: Into<Vec<u8>>>(
        mut self,
        map: T,
        params: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.wasm_module = Some(SmartStreamPayload {
            wasm: SmartStreamWasm::Raw(map.into()),
            kind: SmartStreamKind::Map,
            params: params.into(),
        });
        self
    }
}

impl TopicProducer {
    pub(crate) fn new(topic: String, pool: Arc<SpuPool>) -> Self {
        let partitioner = Box::new(SiphashRoundRobinPartitioner::new());
        Self {
            topic,
            pool,
            partitioner,
            wasm_module: None,
        }
    }

    /// Sends a key/value record to this producer's Topic.
    ///
    /// The partition that the record will be sent to is derived from the Key.
    ///
    /// # Example
    ///
    /// ```
    /// # use fluvio::{TopicProducer, FluvioError};
    /// # async fn example(producer: &TopicProducer) -> Result<(), FluvioError> {
    /// producer.send("Key", "Value").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[instrument(
        skip(self, key, value),
        fields(topic = %self.topic),
    )]
    pub async fn send<K, V>(&self, key: K, value: V) -> Result<(), FluvioError>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
    {
        let record_key = key.into();
        let record_value = value.into();
        self.send_all(Some((record_key, record_value))).await?;
        Ok(())
    }

    #[instrument(
        skip(self, records),
        fields(topic = %self.topic),
    )]
    pub async fn send_all<K, V, I>(&self, records: I) -> Result<(), FluvioError>
    where
        K: Into<RecordKey>,
        V: Into<RecordData>,
        I: IntoIterator<Item = (K, V)>,
    {
        let topics = self.pool.metadata.topics();
        let topic_spec = topics
            .lookup_by_key(&self.topic)
            .await?
            .ok_or_else(|| FluvioError::TopicNotFound(self.topic.to_string()))?
            .spec;
        let partition_count = topic_spec.partitions();
        let partition_config = PartitionerConfig { partition_count };

        let mut entries = records
            .into_iter()
            .map::<(RecordKey, RecordData), _>(|(k, v)| (k.into(), v.into()))
            .map(Record::from)
            .collect::<Vec<Record>>();

        cfg_if::cfg_if! {
            if #[cfg(feature = "smartengine")] {
                if let Some(ref smart_payload) = self.wasm_module {

                    use fluvio_smartengine::{SmartEngine, SmartStream};
                    use dataplane::smartstream::{
                        SmartStreamExtraParams,
                        SmartStreamInput,
                    };
                    let engine = SmartEngine::default();
                    let smart_module = engine
                        .create_module_from_binary(
                            &smart_payload
                            .wasm
                            .get_raw()
                            .expect("failed to get raw wasm"),
                        )
                        .expect("Failed to get module from raw wasm");

                    let mut smartstream: Box<dyn SmartStream> = match &smart_payload.kind {
                        SmartStreamKind::Filter => Box::new(
                            smart_module
                            .create_filter(&engine, SmartStreamExtraParams::default())
                            .expect("Failed to create filter"),
                        ),
                        SmartStreamKind::Map => Box::new(
                            smart_module
                            .create_map(&engine, SmartStreamExtraParams::default())
                            .expect("Failed to create map"),
                        ),
                        SmartStreamKind::ArrayMap => Box::new(
                            smart_module
                            .create_array_map(&engine, SmartStreamExtraParams::default())
                            .expect("Failed to create array map"),
                        ),
                        SmartStreamKind::Aggregate { accumulator: _ } => {
                            todo!("Aggregate not implemented yet")
                        }
                    };
                    let output = smartstream.process(SmartStreamInput::from_records(entries).expect("Failed to build smartstream input")).expect("Failed to process smartstream");
                    entries = output.successes;
                }
            }
        }

        // Calculate the partition for each entry
        // Use a block scope to ensure we drop the partitioner lock
        let records_by_partition = {
            let mut iter = Vec::new();
            for record in entries {
                let key = record.key.as_ref().map(|k| k.as_ref());
                let value = record.value.as_ref();
                let partition = self.partitioner.partition(&partition_config, key, value);
                iter.push((partition, record));
            }
            iter
        };

        // Group all of the records by the partitions they belong to, then
        // group all of the partitions by the SpuId that leads that partition
        let partitions_by_spu = group_by_spu(
            &self.topic,
            self.pool.metadata.partitions(),
            records_by_partition,
        )
        .await?;

        // Create one request per SPU leader
        let requests = assemble_requests(&self.topic, partitions_by_spu);

        for (leader, request) in requests {
            let spu_client = self.pool.create_serial_socket_from_leader(leader).await?;
            spu_client.send_receive(request).await?;
        }

        Ok(())
    }
}

async fn group_by_spu(
    topic: &str,
    partitions: &StoreContext<PartitionSpec>,
    records_by_partition: Vec<(PartitionId, Record)>,
) -> Result<HashMap<SpuId, HashMap<PartitionId, MemoryRecords>>, FluvioError> {
    let mut map: HashMap<SpuId, HashMap<i32, MemoryRecords>> = HashMap::new();
    for (partition, record) in records_by_partition {
        let replica_key = ReplicaKey::new(topic, partition);
        let partition_spec = partitions
            .lookup_by_key(&replica_key)
            .await?
            .ok_or_else(|| FluvioError::PartitionNotFound(topic.to_string(), partition))?
            .spec;
        let leader = partition_spec.leader;
        map.entry(leader)
            .or_insert_with(HashMap::new)
            .entry(partition)
            .or_insert_with(Vec::new)
            .push(record);
    }

    Ok(map)
}

fn assemble_requests(
    topic: &str,
    partitions_by_spu: HashMap<SpuId, HashMap<PartitionId, MemoryRecords>>,
) -> Vec<(SpuId, DefaultProduceRequest)> {
    let mut requests: Vec<(SpuId, DefaultProduceRequest)> =
        Vec::with_capacity(partitions_by_spu.len());

    for (leader, partitions) in partitions_by_spu {
        let mut request = DefaultProduceRequest::default();

        let mut topic_request = DefaultTopicRequest {
            name: topic.to_string(),
            ..Default::default()
        };

        for (partition, records) in partitions {
            let mut partition_request = DefaultPartitionRequest {
                partition_index: partition,
                ..Default::default()
            };
            partition_request.records.batches.push(Batch::from(records));
            topic_request.partitions.push(partition_request);
        }

        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);
        requests.push((leader, request));
    }

    requests
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::store::MetadataStoreObject;

    #[fluvio_future::test]
    async fn test_group_by_spu() {
        let partitions = StoreContext::new();
        let partition_specs: Vec<_> = vec![
            (ReplicaKey::new("TOPIC", 0), PartitionSpec::new(0, vec![])), // Partition 0
            (ReplicaKey::new("TOPIC", 1), PartitionSpec::new(0, vec![])), // Partition 1
            (ReplicaKey::new("TOPIC", 2), PartitionSpec::new(1, vec![])), // Partition 2
        ]
        .into_iter()
        .map(|(key, spec)| MetadataStoreObject::with_spec(key, spec))
        .collect();
        partitions.store().sync_all(partition_specs).await;
        let records_by_partition = vec![
            (0, Record::new("A")),
            (1, Record::new("B")),
            (2, Record::new("C")),
            (0, Record::new("D")),
            (1, Record::new("E")),
            (2, Record::new("F")),
        ];

        let grouped = group_by_spu("TOPIC", &partitions, records_by_partition)
            .await
            .unwrap();

        // SPU 0 should have partitions 0 and 1, but not 2
        let spu_0 = grouped.get(&0).unwrap();
        let partition_0 = spu_0.get(&0).unwrap();

        assert!(
            partition_0
                .iter()
                .any(|record| record.value.as_ref() == b"A"),
            "SPU 0/Partition 0 should contain record A",
        );
        assert!(
            partition_0
                .iter()
                .any(|record| record.value.as_ref() == b"D"),
            "SPU 0/Partition 0 should contain record D",
        );

        let partition_1 = spu_0.get(&1).unwrap();
        assert!(
            partition_1
                .iter()
                .any(|record| record.value.as_ref() == b"B"),
            "SPU 0/Partition 1 should contain record B",
        );
        assert!(
            partition_1
                .iter()
                .any(|record| record.value.as_ref() == b"E"),
            "SPU 0/Partition 1 should contain record E",
        );

        assert!(
            spu_0.get(&2).is_none(),
            "SPU 0 should not contain Partition 2",
        );

        let spu_1 = grouped.get(&1).unwrap();
        let partition_2 = spu_1.get(&2).unwrap();

        assert!(
            spu_1.get(&0).is_none(),
            "SPU 1 should not contain Partition 0",
        );

        assert!(
            spu_1.get(&1).is_none(),
            "SPU 1 should not contain Partition 1",
        );

        assert!(
            partition_2
                .iter()
                .any(|record| record.value.as_ref() == b"C"),
            "SPU 1/Partition 2 should contain record C",
        );

        assert!(
            partition_2
                .iter()
                .any(|record| record.value.as_ref() == b"F"),
            "SPU 1/Partition 2 should contain record F",
        );
    }

    #[test]
    fn test_assemble_requests() {
        let partitions_by_spu = {
            let mut pbs = HashMap::new();

            let spu_0 = {
                let mut s0_partitions = HashMap::new();
                let partition_0_records = vec![Record::new("A"), Record::new("B")];
                let partition_1_records = vec![Record::new("C"), Record::new("D")];
                s0_partitions.insert(0, partition_0_records);
                s0_partitions.insert(1, partition_1_records);
                s0_partitions
            };

            let spu_1 = {
                let mut s1_partitions = HashMap::new();
                let partition_2_records = vec![Record::new("E"), Record::new("F")];
                let partition_3_records = vec![Record::new("G"), Record::new("H")];
                s1_partitions.insert(2, partition_2_records);
                s1_partitions.insert(3, partition_3_records);
                s1_partitions
            };
            pbs.insert(0, spu_0);
            pbs.insert(1, spu_1);
            pbs
        };

        let requests = assemble_requests("TOPIC", partitions_by_spu);
        assert_eq!(requests.len(), 2);

        // SPU 0
        {
            let (spu0, request) = requests.iter().find(|(spu, _)| *spu == 0).unwrap();
            assert_eq!(*spu0, 0);
            assert_eq!(request.topics.len(), 1);
            let topic_request = request.topics.get(0).unwrap();
            assert_eq!(topic_request.name, "TOPIC");
            assert_eq!(topic_request.partitions.len(), 2);

            let partition_0_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 0)
                .unwrap();
            assert_eq!(partition_0_request.records.batches.len(), 1);
            let batch = partition_0_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_0_0 = batch.records().get(0).unwrap();
            assert_eq!(record_0_0.value.as_ref(), b"A");
            let record_0_1 = batch.records().get(1).unwrap();
            assert_eq!(record_0_1.value.as_ref(), b"B");

            let partition_1_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 1)
                .unwrap();
            assert_eq!(partition_1_request.records.batches.len(), 1);
            let batch = partition_1_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_1_0 = batch.records().get(0).unwrap();
            assert_eq!(record_1_0.value.as_ref(), b"C");
            let record_1_1 = batch.records().get(1).unwrap();
            assert_eq!(record_1_1.value.as_ref(), b"D");
        }

        // SPU 1
        {
            let (spu1, request) = requests.iter().find(|(spu, _)| *spu == 1).unwrap();
            assert_eq!(*spu1, 1);
            assert_eq!(request.topics.len(), 1);
            let topic_request = request.topics.get(0).unwrap();
            assert_eq!(topic_request.name, "TOPIC");
            assert_eq!(topic_request.partitions.len(), 2);

            let partition_0_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 2)
                .unwrap();
            assert_eq!(partition_0_request.records.batches.len(), 1);
            let batch = partition_0_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_0_0 = batch.records().get(0).unwrap();
            assert_eq!(record_0_0.value.as_ref(), b"E");
            let record_0_1 = batch.records().get(1).unwrap();
            assert_eq!(record_0_1.value.as_ref(), b"F");

            let partition_1_request = topic_request
                .partitions
                .iter()
                .find(|p| p.partition_index == 3)
                .unwrap();
            assert_eq!(partition_1_request.records.batches.len(), 1);
            let batch = partition_1_request.records.batches.get(0).unwrap();
            assert_eq!(batch.records().len(), 2);
            let record_1_0 = batch.records().get(0).unwrap();
            assert_eq!(record_1_0.value.as_ref(), b"G");
            let record_1_1 = batch.records().get(1).unwrap();
            assert_eq!(record_1_1.value.as_ref(), b"H");
        }
    }
}
