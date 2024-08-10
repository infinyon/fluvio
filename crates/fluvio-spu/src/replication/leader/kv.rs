use std::{sync::Arc, task::Poll, pin::Pin};

use anyhow::Result;
use fluvio_kv_storage::{Log, LogBasedKVStorage};
use fluvio_protocol::{
    record::{RecordSet, RawRecords, Batch, Record},
    Version,
};
use fluvio_storage::{
    ReplicaStorage,
    iterators::{FileBatchIterator, FileRecordIterator, RecordItem},
};
use futures_util::{Stream, ready, Future, FutureExt};
use tracing::debug;

use super::{LeaderReplicaState, FollowerNotifier};

const RECORDS_SERIALIZATION_VERSION: Version = 0;

pub type LeaderKVStorage<K, V, S> = LogBasedKVStorage<K, V, LeaderReplicaLog<S>>;

/// [`Log`] implementation that enables using [`fluvio_kv_storage::KVStorage`] on top of [`LeaderReplicaState`].
/// Basically, it implements a logic for storing KV operations on the topic, and reading them from
/// the end of the topic.
#[derive(Debug)]
pub struct LeaderReplicaLog<S> {
    replica: LeaderReplicaState<S>,
    follower_notifier: Arc<FollowerNotifier>,
}

impl<S> LeaderReplicaLog<S> {
    pub fn new(replica: LeaderReplicaState<S>, follower_notifier: Arc<FollowerNotifier>) -> Self {
        Self {
            replica,
            follower_notifier,
        }
    }
}

impl<S: ReplicaStorage + Send + Sync + 'static> Log for LeaderReplicaLog<S> {
    async fn read_from_end(&self) -> Result<impl futures_util::Stream<Item = Result<Vec<u8>>>> {
        Ok(ReplicaRevStream::new(self.replica.clone()))
    }

    async fn append_batch(&mut self, entries: Vec<Vec<u8>>) -> Result<()> {
        let count = entries.len();
        let mut records = RecordSet::<RawRecords>::default();
        let mut batch = Batch::new();
        for entry in entries {
            batch.add_record(Record::new(entry));
        }
        records = records.add(batch.try_into()?);
        self.replica
            .write_record_set(&mut records, &self.follower_notifier)
            .await?;
        debug!(count, "append batch");
        Ok(())
    }
}

struct ReplicaRevStream<S: ReplicaStorage + Send + Sync> {
    last_seen_offset: i64,
    replica: LeaderReplicaState<S>,
    inner_iter_fut: InnerFutureState,
}

impl<S: ReplicaStorage + Send + Sync> ReplicaRevStream<S> {
    fn new(replica: LeaderReplicaState<S>) -> Self {
        Self {
            last_seen_offset: replica.hw(),
            replica,
            inner_iter_fut: InnerFutureState::None,
        }
    }

    async fn read_batch(
        replica: LeaderReplicaState<S>,
        last_seen_offset: i64,
    ) -> Result<Vec<RecordItem>> {
        let Some(next) = last_seen_offset.checked_sub(1) else {
            return Ok(Vec::new());
        };
        if next.is_negative() {
            return Ok(Vec::new());
        }
        let slice = replica
            .read_records(next, u32::MAX, Default::default())
            .await?;
        if let Some(file) = slice.file_slice {
            let batch_it = FileBatchIterator::from_raw_slice(file).take(1);
            let record_it = FileRecordIterator::new(batch_it, RECORDS_SERIALIZATION_VERSION);
            Ok(record_it.collect::<Result<Vec<_>, _>>()?)
        } else {
            Ok(Vec::new())
        }
    }
}

impl<S: ReplicaStorage + Send + Sync + 'static> Stream for ReplicaRevStream<S> {
    type Item = Result<Vec<u8>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let next = match &mut self.inner_iter_fut {
            InnerFutureState::Resolved(ref mut records) => records.pop(),

            InnerFutureState::Pending(fut) => match ready!(fut.poll_unpin(cx)) {
                Ok(records) if records.is_empty() => return Poll::Ready(None),
                Ok(mut records) => {
                    let next = records.pop();
                    self.inner_iter_fut = InnerFutureState::Resolved(records);
                    next
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            },
            InnerFutureState::None => {
                let replica = self.replica.clone();
                let last_read_offset = self.last_seen_offset;
                let fut = Box::pin(Self::read_batch(replica, last_read_offset));
                self.inner_iter_fut = InnerFutureState::Pending(fut);
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        };
        match next {
            Some(record) => {
                self.last_seen_offset = record.offset;
                Poll::Ready(Some(Ok(record.record.into_value().into_vec())))
            }
            None => {
                self.inner_iter_fut = InnerFutureState::None;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

enum InnerFutureState {
    None,
    Resolved(Vec<RecordItem>),
    Pending(Pin<Box<dyn Future<Output = Result<Vec<RecordItem>>> + Send + 'static>>),
}

#[cfg(test)]
mod tests {
    use std::{env::temp_dir, path::Path};

    use fluvio_controlplane::replica::Replica;
    use fluvio_kv_storage::KVStorage;
    use fluvio_protocol::record::ReplicaKey;
    use fluvio_storage::{config::ReplicaConfig, FileReplica};
    use fluvio_types::PartitionId;
    use flv_util::fixture::ensure_clean_dir;
    use futures_util::StreamExt;

    use crate::{
        config::ReplicationConfig, control_plane::StatusLrsMessageSink,
        storage::SharableReplicaStorage,
    };

    use super::*;

    #[fluvio_future::test]
    async fn test_replica_kv() {
        //given
        let leader = create_replica("test_replica_kv").await;
        let notifier = FollowerNotifier::shared();
        let mut kv = LeaderKVStorage::<String, String, _>::new(LeaderReplicaLog::new(
            leader.clone(),
            notifier.clone(),
        ));

        //when
        kv.put("key1", "value1").await.unwrap();
        kv.put("key2", "value2").await.unwrap();
        kv.put("key3", "value3").await.unwrap();

        kv.delete(&"key2".to_string()).await.unwrap();

        drop(kv);

        let mut kv = LeaderKVStorage::<String, String, _>::new(LeaderReplicaLog::new(
            leader.clone(),
            notifier,
        ));
        kv.sync_from_log().await.expect("synced");

        //then

        assert_eq!(
            kv.get(&"key1".to_string()).await.expect("read key1"),
            Some("value1".to_string())
        );

        assert_eq!(kv.get(&"key2".to_string()).await.expect("read key1"), None);

        assert_eq!(
            kv.get(&"key3".to_string()).await.expect("read key1"),
            Some("value3".to_string())
        );

        leader.remove().await.expect("removed");
    }

    #[fluvio_future::test]
    async fn test_log() {
        //given
        let leader = create_replica("test_log").await;
        let notifier = FollowerNotifier::shared();

        //when
        let mut log = LeaderReplicaLog::new(leader.clone(), notifier);
        log.append_batch(vec![b"1".to_vec()]).await.unwrap();
        log.append_batch(vec![b"2".to_vec(), b"3".to_vec()])
            .await
            .unwrap();
        log.append_batch(vec![b"4".to_vec()]).await.unwrap();
        let output: Vec<Result<Vec<u8>>> = log.read_from_end().await.expect("read").collect().await;

        //then
        leader.remove().await.expect("removed");

        assert_eq!(output.len(), 4);

        let output = output
            .into_iter()
            .map(|r| r.map(|v| String::from_utf8_lossy(&v).to_string()))
            .collect::<std::result::Result<Vec<String>, _>>()
            .expect("repacked");

        assert_eq!(output, &["4", "3", "2", "1"]);
    }

    #[fluvio_future::test]
    async fn test_stream_on_empty_replica() {
        //given
        let leader = create_replica("test_log_on_empty_replica").await;
        let stream = ReplicaRevStream::new(leader.clone());

        //when
        let output = stream.collect::<Vec<_>>().await;

        //then
        leader.remove().await.expect("removed");

        assert!(output.is_empty());
    }

    #[fluvio_future::test]
    async fn test_stream_on_replica_with_one_record() {
        //given
        let leader = create_replica("test_stream_on_replica_with_one_record").await;
        let notifier = FollowerNotifier::shared();

        //when
        leader
            .write_record_set(&mut create_raw_recordset(1), &notifier)
            .await
            .expect("write records");
        let stream = ReplicaRevStream::new(leader.clone());
        let output = stream.collect::<Vec<_>>().await;

        //then
        leader.remove().await.expect("removed");

        assert_eq!(output.len(), 1);
    }

    #[fluvio_future::test]
    async fn test_stream_on_replica_with_many_records_one_batch() {
        //given
        let leader = create_replica("test_stream_on_replica_with_many_records_one_batch").await;
        let notifier = FollowerNotifier::shared();

        //when
        leader
            .write_record_set(&mut create_raw_recordset(5), &notifier)
            .await
            .expect("write records");
        let stream = ReplicaRevStream::new(leader.clone());
        let output = stream.collect::<Vec<_>>().await;

        //then
        leader.remove().await.expect("removed");

        assert_eq!(output.len(), 5);

        let output = output
            .into_iter()
            .map(|r| r.map(|v| String::from_utf8_lossy(&v).to_string()))
            .collect::<std::result::Result<Vec<String>, _>>()
            .expect("repacked");

        assert_eq!(output, &["4", "3", "2", "1", "0"]);
    }

    #[fluvio_future::test]
    async fn test_stream_on_replica_with_many_records_many_batches() {
        //given
        let leader = create_replica("test_stream_on_replica_with_many_records_many_batches").await;
        let notifier = FollowerNotifier::shared();

        //when
        leader
            .write_record_set(&mut create_raw_recordset(5), &notifier)
            .await
            .expect("write records");
        leader
            .write_record_set(&mut create_raw_recordset(3), &notifier)
            .await
            .expect("write records");

        let stream = ReplicaRevStream::new(leader.clone());
        let output = stream.collect::<Vec<_>>().await;

        //then
        leader.remove().await.expect("removed");

        assert_eq!(output.len(), 8);

        let output = output
            .into_iter()
            .map(|r| r.map(|v| String::from_utf8_lossy(&v).to_string()))
            .collect::<std::result::Result<Vec<String>, _>>()
            .expect("repacked");

        assert_eq!(output, &["2", "1", "0", "4", "3", "2", "1", "0"]);
    }

    async fn create_replica(dir: impl AsRef<Path>) -> LeaderReplicaState<FileReplica> {
        let base_dir = temp_dir().join(dir);
        ensure_clean_dir(&base_dir);
        let config = ReplicaConfig {
            base_dir,
            ..Default::default()
        };
        let replica_id = ReplicaKey::new("topic", PartitionId::default());
        let replication_config = ReplicationConfig::default();
        let replica = Replica::new(replica_id.clone(), 5000, vec![5000]);
        let status_update = StatusLrsMessageSink::shared();

        let storage = SharableReplicaStorage::create(replica_id, config)
            .await
            .expect("storage");
        LeaderReplicaState::new(replica, replication_config, status_update, storage).into_inner()
    }

    pub fn create_raw_recordset(num: u16) -> RecordSet<RawRecords> {
        let records = RecordSet::default();

        let mut batches = Batch::default();
        for i in 0..num {
            let mut record = Record::default();
            let bytes = format!("{i}");
            record.value = bytes.into();
            batches.add_record(record);
        }
        records.add(
            batches
                .try_into()
                .expect("converted from memory records to raw"),
        )
    }
}
