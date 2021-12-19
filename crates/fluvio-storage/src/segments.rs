use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, trace, error, instrument};

use dataplane::ErrorCode;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_types::event::StickyEvent;

use dataplane::Offset;

use crate::config::SharedReplicaConfig;
use crate::segment::ReadSegment;
use crate::StorageError;
use crate::util::log_path_get_offset;
use cleaner::Cleaner;

pub use self::cleaner::CleanerConfig;

const MEM_ORDER: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

/// Thread safe Segment list
///
#[derive(Debug)]
pub(crate) struct SharedSegments {
    inner: Arc<RwLock<SegmentList>>,
    min_offset: AtomicI64,
    config: Arc<SharedReplicaConfig>,
    end_event: Arc<StickyEvent>,
}

impl SharedSegments {
    fn from(
        list: SegmentList,
        config: Arc<SharedReplicaConfig>,
        clean_config: CleanerConfig,
    ) -> Arc<Self> {
        let min = list.min_offset;
        let end_event = StickyEvent::shared();
        let shared_segments = Arc::new(Self {
            inner: Arc::new(RwLock::new(list)),
            min_offset: AtomicI64::new(min),
            config,
            end_event: end_event.clone(),
        });

        Cleaner::start(clean_config, shared_segments.clone(), end_event);
        shared_segments
    }

    pub async fn from_dir(
        option: Arc<SharedReplicaConfig>,
    ) -> Result<(Arc<SharedSegments>, Option<Offset>), StorageError> {
        let clear_config = CleanerConfig::builder().build().map_err(|err| {
            StorageError::Other(format!("failed to build cleaner config: {}", err))
        })?;

        Self::from_dir_with_config(option, clear_config).await
    }

    pub async fn from_dir_with_config(
        option: Arc<SharedReplicaConfig>,
        clean_config: CleanerConfig,
    ) -> Result<(Arc<SharedSegments>, Option<Offset>), StorageError> {
        let dirs = option.base_dir.read_dir()?;
        debug!("reading segments at: {:#?}", dirs);
        let files: Vec<_> = dirs.filter_map(|entry| entry.ok()).collect();
        let mut offsets: Vec<Offset> = vec![];
        for entry in files {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    let path = entry.path();
                    trace!("scanning file: {:#?}", path);

                    if path.extension() == Some(OsStr::new("log")) {
                        if let Ok(offset) = log_path_get_offset(&path) {
                            trace!("detected valid log: {}", offset);
                            offsets.push(offset);
                            /*
                            match Segment::open(offset,option).await {
                                Ok(segment) => segments.add_segment(segment),
                                Err(err) => error!("error opening segment: {:#?}",err)
                            }
                            } else {
                                debug!("not log, skipping: {:#?}",path);
                            */
                        }
                    }
                }
            }
        }

        offsets.sort_unstable();

        let last_offset = offsets.pop();
        let mut segments = SegmentList::new();

        for offset in offsets {
            // for now, set end offset same as base, this will be reset when validation occurs
            match ReadSegment::open_unknown(offset, option.clone()).await {
                Ok(segment) => {
                    let min_offset = segments.add_segment(segment);
                    debug!(min_offset, "adding segment");
                }
                Err(err) => {
                    error!("error opening segment: {:#?}", err);
                    return Err(err);
                }
            }
        }

        let shared_segments = SharedSegments::from(segments, option, clean_config);

        Ok((shared_segments, last_offset))
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, SegmentList> {
        self.inner.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, SegmentList> {
        self.inner.write().await
    }

    pub fn min_offset(&self) -> Offset {
        self.min_offset.load(MEM_ORDER)
    }

    pub async fn add_segment(&self, segment: ReadSegment) {
        let mut writer = self.write().await;
        let min_offset = writer.add_segment(segment);
        self.min_offset.store(min_offset, MEM_ORDER);
    }

    #[instrument(skip(self))]
    async fn remove_segment(&self, base_offset: Offset) {
        let mut write = self.write().await;

        if let Some((old_segment, min_offset)) = write.remove_segment(&base_offset) {
            self.min_offset.store(min_offset, MEM_ORDER);
            if let Err(err) = old_segment.remove().await {
                error!("failed to remove segment: {:#?}", err);
            }
        }
    }

    /// find slice in the segments
    /// if not found, return OutOfRange error
    pub async fn find_slice(
        &self,
        start_offset: Offset,
        max_offset: Option<Offset>,
    ) -> Result<AsyncFileSlice, ErrorCode> {
        let reader = self.read().await;
        if let Some((_offset, segment)) = reader.find_segment(start_offset) {
            if let Some(slice) = segment.records_slice(start_offset, max_offset).await? {
                Ok(slice)
            } else {
                Err(ErrorCode::OffsetOutOfRange)
            }
        } else {
            Err(ErrorCode::OffsetOutOfRange)
        }
    }

    /// perform any clean up such as shutdown of cleaner
    pub fn clean(&self) {
        self.end_event.notify();
    }
}

#[derive(Debug)]
pub struct SegmentList {
    segments: BTreeMap<Offset, ReadSegment>, // max base offset of all segments
    min_offset: Offset,
    max_offset: Offset,
}

impl SegmentList {
    pub fn new() -> Self {
        SegmentList {
            segments: BTreeMap::new(),
            max_offset: 0,
            min_offset: -1,
        }
    }

    // load segments

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    #[instrument(skip(self, segment))]
    fn add_segment(&mut self, segment: ReadSegment) -> Offset {
        debug!(
            base_offset = segment.get_base_offset(),
            end_offset = segment.get_end_offset(),
            "inserting"
        );
        self.segments.insert(segment.get_base_offset(), segment);
        self.update_min_max();
        self.min_offset
    }

    fn update_min_max(&mut self) {
        let mut max_offset = 0;
        let mut min_offset = -1;
        self.segments.values().for_each(|segment| {
            let base_offset = segment.get_base_offset();
            let end_offset = segment.get_end_offset();
            if end_offset > max_offset {
                max_offset = end_offset;
            }
            if min_offset < 0 || base_offset < min_offset {
                min_offset = base_offset;
            }
        });
        self.max_offset = max_offset;
        self.min_offset = min_offset;
    }

    /// remove segment and return min offset
    fn remove_segment(&mut self, offset: &Offset) -> Option<(ReadSegment, Offset)> {
        if let Some(segment) = self.segments.remove(offset) {
            self.update_min_max();
            Some((segment, self.min_offset))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn get_segment(&self, offset: Offset) -> Option<&ReadSegment> {
        self.segments.get(&offset)
    }

    pub fn find_segment(&self, offset: Offset) -> Option<(&Offset, &ReadSegment)> {
        if offset < self.min_offset {
            None
        } else if offset == self.min_offset {
            (&self.segments)
                .range((Included(offset), Included(offset)))
                .next_back()
        } else if offset >= self.max_offset {
            None
        } else {
            let range = (
                Included(offset - self.max_offset + self.min_offset + 1),
                Included(offset),
            );
            //  println!("range: {:?}", range);
            (&self.segments).range(range).next_back()
        }
    }

    fn find_expired_segments(&self, expired_duration: &Duration) -> Vec<Offset> {
        self.segments
            .iter()
            .filter_map(|(base_offset, segment)| {
                if segment.is_expired(expired_duration) {
                    Some(*base_offset)
                } else {
                    None
                }
            })
            .collect()
    }
}

mod cleaner {

    use std::time::Duration;

    use derive_builder::Builder;
    use tracing::{debug, info};

    use fluvio_future::task::spawn;
    use fluvio_future::timer::sleep;

    use super::{SharedSegments, StickyEvent};
    use super::{Arc, instrument};

    #[derive(Builder, Debug)]
    pub struct CleanerConfig {
        #[builder(default = "10000")] // 10 seconds
        cleaning_interval_ms: u16,
    }

    impl CleanerConfig {
        pub fn builder() -> CleanerConfigBuilder {
            CleanerConfigBuilder::default()
        }
    }

    /// Replica cleaner.  This is a background task that periodically checks for expired segments and
    /// removes them.  In the future, this may be done by a central cleaner pool instead of per a replica.
    pub(crate) struct Cleaner {
        config: CleanerConfig,
        segments: Arc<SharedSegments>,
    }

    impl Cleaner {
        pub(crate) fn start(
            config: CleanerConfig,
            segments: Arc<SharedSegments>,
            end_event: Arc<StickyEvent>,
        ) {
            let cleaner = Cleaner { config, segments };

            spawn(async move {
                cleaner.clean(end_event).await;
            });
        }

        #[instrument(skip(self, end_event))]
        async fn clean(&self, end_event: Arc<StickyEvent>) {
            use tokio::select;

            let sleep_period = Duration::from_millis(self.config.cleaning_interval_ms as u64);

            loop {
                debug!(ms = self.config.cleaning_interval_ms, "sleeping");

                if end_event.is_set() {
                    info!("clear is terminated");
                    break;
                }

                select! {
                    _ = end_event.listen() => {
                        info!("cleaner end event received");
                        break;
                    },
                    _ = sleep(sleep_period) => {


                        let retention_secs = Duration::from_secs(self.segments.config.retention_seconds.get() as u64);
                        let read = self.segments.read().await;
                        let expired_segments =
                            read.find_expired_segments(&retention_secs);
                        debug!(seconds = retention_secs.as_secs(),expired = expired_segments.len(), "found expired segments");
                        drop(read);
                        if !expired_segments.is_empty() {
                            for base_offset in expired_segments {
                                info!(base_offset, "removing segment");
                                self.segments.remove_segment(base_offset).await;
                            }
                        }
                    }
                }
            }

            info!("cleaner end");
        }
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use tracing::debug;

    use fluvio_future::timer::sleep;
    use flv_util::fixture::ensure_new_dir;
    use dataplane::fixture::create_batch;
    use dataplane::Offset;

    use crate::StorageError;
    use crate::config::SharedReplicaConfig;
    use crate::segment::MutableSegment;
    use crate::segment::ReadSegment;
    use crate::config::ReplicaConfig;

    use super::{SegmentList, SharedSegments, CleanerConfig};

    // create fake segment, this doesn't create a segment with all data, it just fill with a min data but with a valid end offset
    async fn create_segment(
        option: Arc<SharedReplicaConfig>,
        start: Offset,
        end_offset: Offset,
    ) -> Result<ReadSegment, StorageError> {
        let mut mut_segment = MutableSegment::create(start, option).await?;
        mut_segment.write_batch(&mut create_batch()).await?;
        mut_segment.set_end_offset(end_offset); // only used for testing
        let segment = mut_segment.convert_to_segment().await?;
        Ok(segment)
    }

    fn default_option(base_dir: PathBuf) -> ReplicaConfig {
        ReplicaConfig {
            segment_max_bytes: 100,
            base_dir,
            index_max_bytes: 1000,
            index_max_interval_bytes: 0,
            ..Default::default()
        }
    }

    #[fluvio_future::test]
    async fn test_segment_empty() {
        let rep_dir = temp_dir().join("segmentlist-read-empty");
        ensure_new_dir(&rep_dir).expect("new");
        let option = default_option(rep_dir).shared();

        let (segments, last_segment) = SharedSegments::from_dir(option).await.expect("from");

        let read = segments.read().await;
        assert_eq!(read.len(), 0); // 0,500,2000
        assert!(last_segment.is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_single_base_zero() {
        let rep_dir = temp_dir().join("segmentlist-single-zero");
        ensure_new_dir(&rep_dir).expect("new");
        let mut list = SegmentList::new();

        let option = default_option(rep_dir).shared();

        list.add_segment(create_segment(option, 0, 500).await.expect("create"));
        println!("segments: {:#?}", list);
        assert_eq!(list.min_offset, 0);
        assert_eq!(list.max_offset, 500);
        assert!(list.find_segment(-1).is_none());
        assert!(list.find_segment(0).is_some());
        assert!(list.find_segment(1).is_some());
        assert!(list.find_segment(499).is_some());
        assert!(list.find_segment(500).is_none());
        assert!(list.find_segment(501).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_single_base_some() {
        let rep_dir = temp_dir().join("segmentlist-single-some");
        ensure_new_dir(&rep_dir).expect("new");
        let mut list = SegmentList::new();

        let option = default_option(rep_dir).shared();

        list.add_segment(create_segment(option, 100, 500).await.expect("create"));
        println!("segments: {:#?}", list);

        assert!(list.find_segment(50).is_none());
        assert!(list.find_segment(99).is_none());
        assert!(list.find_segment(100).is_some());
        assert!(list.find_segment(499).is_some());
        assert!(list.find_segment(500).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_many_zero() {
        let rep_dir = temp_dir().join("segmentlist-many-zero");
        ensure_new_dir(&rep_dir).expect("new");
        let mut list = SegmentList::new();

        let option = default_option(rep_dir).shared();

        list.add_segment(
            create_segment(option.clone(), 0, 500)
                .await
                .expect("create"),
        );
        list.add_segment(
            create_segment(option.clone(), 500, 2000)
                .await
                .expect("create"),
        );
        list.add_segment(
            create_segment(option.clone(), 2000, 3000)
                .await
                .expect("create"),
        );
        list.add_segment(
            create_segment(option.clone(), 3000, 4000)
                .await
                .expect("create"),
        );

        println!("segments: {:#?}", list);

        assert_eq!(list.min_offset, 0);
        assert_eq!(list.max_offset, 4000);

        assert_eq!(list.len(), 4); // 0,500,2000
        assert_eq!(list.find_segment(0).expect("segment").0, &0);
        assert_eq!(list.find_segment(1).expect("segment").0, &0);
        assert_eq!(list.find_segment(499).expect("segment").0, &0);
        assert_eq!(list.find_segment(500).expect("segment").0, &500);
        assert_eq!(list.find_segment(1500).expect("segment").0, &500); // belong to 2nd segment
        assert_eq!(list.find_segment(3000).expect("segment").0, &3000);
        assert!(list.find_segment(4000).is_none());
        assert!(list.find_segment(4001).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_many_some() {
        let rep_dir = temp_dir().join("segmentlist-many-some");
        ensure_new_dir(&rep_dir).expect("new");
        let mut list = SegmentList::new();

        let option = default_option(rep_dir).shared();

        list.add_segment(
            create_segment(option.clone(), 100, 600)
                .await
                .expect("create"),
        );
        list.add_segment(
            create_segment(option.clone(), 600, 4000)
                .await
                .expect("create"),
        );
        list.add_segment(
            create_segment(option.clone(), 4000, 9000)
                .await
                .expect("create"),
        );

        println!("segments: {:#?}", list);

        assert_eq!(list.min_offset, 100);
        assert_eq!(list.max_offset, 9000);

        assert_eq!(list.len(), 3);
        assert!(list.find_segment(0).is_none());
        assert!(list.find_segment(99).is_none());
        assert_eq!(list.find_segment(100).expect("segment").0, &100);
        assert_eq!(list.find_segment(599).expect("segment").0, &100);
        assert_eq!(list.find_segment(600).expect("segment").0, &600);
        assert_eq!(list.find_segment(900).expect("segment").0, &600); // belong to 2nd segment
        assert_eq!(list.find_segment(8000).expect("segment").0, &4000);
        assert!(list.find_segment(9000).is_none());
        assert!(list.find_segment(10000).is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_delete() {
        let rep_dir = temp_dir().join("segmentlist-delete");
        ensure_new_dir(&rep_dir).expect("new");

        let mut config = default_option(rep_dir);
        config.retention_seconds = 1;
        let option = config.shared();

        let clear_config = CleanerConfig::builder()
            .cleaning_interval_ms(200)
            .build()
            .expect("build");

        let slist = SharedSegments::from(SegmentList::new(), option.clone(), clear_config);

        slist
            .add_segment(
                create_segment(option.clone(), 100, 600)
                    .await
                    .expect("create"),
            )
            .await;

        let read = slist.read().await;
        assert_eq!(read.len(), 1);
        assert_eq!(read.find_segment(100).expect("segment").0, &100);
        assert_eq!(slist.min_offset(), 100);
        drop(read);

        sleep(Duration::from_millis(700)).await; // previous segment should not expire
        slist
            .add_segment(
                create_segment(option.clone(), 600, 4000)
                    .await
                    .expect("create"),
            )
            .await;

        let read = slist.read().await;
        assert_eq!(read.len(), 2);
        assert_eq!(read.find_segment(100).expect("segment").0, &100);
        assert_eq!(read.find_segment(900).expect("segment").0, &600); // belong to 2nd segment
        drop(read);

        sleep(Duration::from_millis(1000)).await; // enough time for cleaer should have deleted the first segment
        let read = slist.read().await;
        assert_eq!(read.len(), 1);
        assert_eq!(slist.min_offset(), 600); // first segment should be deleted
        debug!("droppping segment");
        drop(read);
        assert_eq!(Arc::strong_count(&slist), 2); // cleaner is pointing to shared segment
        slist.clean(); // perform shutdown of cleaner
        sleep(Duration::from_millis(200)).await;
        assert_eq!(Arc::strong_count(&slist), 1); // cleaner is gone...
        debug!("test done");
    }
}
