use std::sync::Arc;
use std::time::Duration;
use std::ops::Div;
use std::ops::Rem;

use tracing::{debug, info, instrument};

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_types::event::StickyEvent;

use crate::config::{SharedReplicaConfig, StorageConfig};
use crate::replica::ReplicaSize;
use crate::segments::SharedSegments;

/// Replica cleaner. This is a background task that periodically checks for expired segments and
/// removes them. It also enforces max partition size by removing first segments if replica size is
/// exceeded. In the future, this may be done by a central cleaner pool instead of per a replica.
#[derive(Debug)]
pub(crate) struct Cleaner {
    config: Arc<StorageConfig>,
    replica_config: Arc<SharedReplicaConfig>,
    segments: Arc<SharedSegments>,
    replica_size: Arc<ReplicaSize>,
    end_event: Arc<StickyEvent>,
}

impl Cleaner {
    pub(crate) fn start_new(
        config: Arc<StorageConfig>,
        replica_config: Arc<SharedReplicaConfig>,
        segments: Arc<SharedSegments>,
        replica_size: Arc<ReplicaSize>,
    ) -> Arc<Self> {
        let end_event = StickyEvent::shared();
        let cleaner = Arc::new(Cleaner {
            config,
            replica_config,
            segments,
            replica_size,
            end_event,
        });

        let cleaner_ref = cleaner.clone();
        spawn(async move {
            cleaner_ref.clean().await;
        });
        cleaner
    }

    pub(crate) fn shutdown(&self) {
        self.end_event.notify();
    }

    #[instrument(skip(self))]
    async fn clean(&self) {
        use tokio::select;

        let sleep_period = Duration::from_millis(self.config.cleaning_interval_ms as u64);

        loop {
            debug!(ms = self.config.cleaning_interval_ms, "sleeping");

            if self.end_event.is_set() {
                info!("clear is terminated");
                break;
            }

            select! {
                _ = self.end_event.listen() => {
                    info!("cleaner end event received");
                    break;
                },
                _ = sleep(sleep_period) => {
                    self.enforce_size().await;
                    self.enforce_ttl().await;
                }
            }
        }

        info!("cleaner end");
    }

    #[instrument(skip(self))]
    async fn enforce_size(&self) {
        let replica_size = self.replica_size.get();
        let max_partition_size = self.replica_config.max_partition_size.get();
        let excess = replica_size.saturating_sub(max_partition_size);
        if excess > 0 {
            let segment_size = self.replica_config.segment_max_bytes.get() as u64;
            let mut count_to_remove = excess.div(segment_size);
            if excess.rem(segment_size) > 0 {
                count_to_remove += 1;
            }
            debug!(
                replica_size,
                max_partition_size,
                segments_to_remove = count_to_remove,
                "replica size exceeded max partition size"
            );
            let segments_to_remove = self
                .segments
                .read()
                .await
                .find_first(count_to_remove as usize);
            self.segments.remove_segments(&segments_to_remove).await;

            let read = self.segments.read().await;
            self.replica_size.store_prev(read.occupied_memory());
        }
    }

    #[instrument(skip(self))]
    async fn enforce_ttl(&self) {
        let retention_secs =
            Duration::from_secs(self.replica_config.retention_seconds.get() as u64);
        let read = self.segments.read().await;
        let expired_segments = read.find_expired_segments(&retention_secs);
        let total = read.len();
        drop(read);
        debug!(
            seconds = retention_secs.as_secs(),
            total = total,
            expired = expired_segments.len(),
            "expired segments"
        );
        if !expired_segments.is_empty() {
            self.segments.remove_segments(&expired_segments).await;
            let read = self.segments.read().await;
            self.replica_size.store_prev(read.occupied_memory());
        }
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::ops::AddAssign;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;

    use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_new_dir;
    use fluvio_protocol::fixture::create_batch;
    use fluvio_protocol::record::Offset;

    use crate::config::SharedReplicaConfig;
    use crate::segment::MutableSegment;
    use crate::segment::ReadSegment;
    use crate::replica::ReplicaSize;
    use crate::config::{ReplicaConfig, StorageConfig};
    use fluvio_types::event::StickyEvent;

    use crate::segments::{SegmentList, SharedSegments};

    use crate::cleaner::Cleaner;

    #[fluvio_future::test]
    async fn test_enforce_size_delete_one() {
        //given
        let mut config = default_option();
        config.max_partition_size = 150;
        config.segment_max_bytes = 80;
        let segments = shared_segments("cleaner-enforce-size-one", 2, config.clone()).await;
        let replica_size = Arc::new(ReplicaSize::default());
        replica_size.store_prev(151); //exceeds max_partition_size, need to remove 1 segment
        let cleaner = test_cleaner(config, segments.clone(), replica_size.clone());
        assert_eq!(segments.read().await.find_first(10), vec![100, 600]);

        //when
        cleaner.enforce_size().await;

        //then
        let read = segments.read().await;
        assert_eq!(read.find_first(10), vec![600]);
        assert_eq!(read.occupied_memory(), replica_size.get());
    }

    #[fluvio_future::test]
    async fn test_enforce_size_delete_many() {
        //given
        let mut config = default_option();
        config.max_partition_size = 150;
        config.segment_max_bytes = 80;
        let segments = shared_segments("cleaner-enforce-size-many", 3, config.clone()).await;
        let replica_size = Arc::new(ReplicaSize::default());
        replica_size.store_prev(150 + 80 + 1); //exceeds max_partition_size, need to remove 2 segments
        let cleaner = test_cleaner(config, segments.clone(), replica_size.clone());
        assert_eq!(segments.read().await.find_first(10), vec![100, 600, 1200]);

        //when
        cleaner.enforce_size().await;

        //then
        let read = segments.read().await;
        assert_eq!(read.find_first(10), vec![1200]);
        assert_eq!(read.occupied_memory(), replica_size.get());
    }

    #[fluvio_future::test]
    async fn test_enforce_ttl() {
        //given
        let mut config = default_option();
        config.retention_seconds = 1;
        let segments = shared_segments("cleaner-enforce-ttl", 2, config.clone()).await;
        let replica_size = Arc::new(ReplicaSize::default());
        replica_size.store_prev(segments.read().await.occupied_memory());
        let cleaner = test_cleaner(config, segments.clone(), replica_size.clone());
        assert_eq!(segments.read().await.find_first(10), vec![100, 600]);

        //when
        sleep(Duration::from_millis(1400)).await;
        cleaner.enforce_ttl().await;

        //then
        let read = segments.read().await;
        assert!(read.find_first(10).is_empty());
        assert_eq!(read.occupied_memory(), replica_size.get());
    }

    async fn shared_segments(
        path: &str,
        count: usize,
        mut config: ReplicaConfig,
    ) -> Arc<SharedSegments> {
        let rep_dir = temp_dir().join(path);
        ensure_new_dir(&rep_dir).expect("new");

        config.base_dir = rep_dir;
        let option = config.shared();

        let slist = SharedSegments::from(SegmentList::new());

        let mut start = 100;
        let mut end = 600;
        for _ in 0..count {
            slist
                .add_segment(
                    create_segment(option.clone(), start, end)
                        .await
                        .expect("create"),
                )
                .await;
            start = end;
            end.add_assign(600);
        }
        slist
    }

    async fn create_segment(
        option: Arc<SharedReplicaConfig>,
        start: Offset,
        end_offset: Offset,
    ) -> Result<ReadSegment> {
        let mut mut_segment = MutableSegment::create(start, option).await?;
        mut_segment.append_batch(&mut create_batch()).await?;
        mut_segment.set_end_offset(end_offset);
        mut_segment.convert_to_segment().await
    }

    fn default_option() -> ReplicaConfig {
        ReplicaConfig {
            segment_max_bytes: 100,
            index_max_bytes: 1000,
            index_max_interval_bytes: 0,
            max_partition_size: 400,
            ..Default::default()
        }
    }

    fn test_cleaner(
        replica_config: ReplicaConfig,
        segments: Arc<SharedSegments>,
        replica_size: Arc<ReplicaSize>,
    ) -> Cleaner {
        Cleaner {
            config: Arc::new(
                StorageConfig::builder()
                    .cleaning_interval_ms(u16::MAX)
                    .build()
                    .expect("config built"),
            ),
            replica_config: replica_config.shared(),
            segments,
            replica_size,
            end_event: StickyEvent::shared(),
        }
    }
}
