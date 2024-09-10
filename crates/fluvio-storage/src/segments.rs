use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ffi::OsStr;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::time::Duration;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{debug, trace, error, instrument, info};
use anyhow::Result;

use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::record::Size64;
use fluvio_protocol::record::Offset;
use fluvio_future::file_slice::AsyncFileSlice;

use crate::config::SharedReplicaConfig;
use crate::segment::ReadSegment;
use crate::util::log_path_get_offset;

const MEM_ORDER: std::sync::atomic::Ordering = std::sync::atomic::Ordering::SeqCst;

/// Thread safe Segment list
///
#[derive(Debug)]
pub(crate) struct SharedSegments {
    inner: Arc<RwLock<SegmentList>>,
    min_offset: AtomicI64,
}

impl SharedSegments {
    pub(crate) fn from(list: SegmentList) -> Arc<Self> {
        let min = list.min_offset;
        Arc::new(Self {
            inner: Arc::new(RwLock::new(list)),
            min_offset: AtomicI64::new(min),
        })
    }

    pub async fn from_dir(
        option: Arc<SharedReplicaConfig>,
    ) -> Result<(Arc<SharedSegments>, Option<Offset>)> {
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

        let shared_segments = SharedSegments::from(segments);

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
    pub(crate) async fn remove_segments(&self, base_offsets: &[Offset]) {
        for offset in base_offsets {
            info!(offset, "removing segment");
            self.remove_segment(offset).await;
        }
    }

    /// find slice in the segments
    /// if not found, return OutOfRange error
    pub async fn find_slice(
        &self,
        start_offset: Offset,
        max_offset: Option<Offset>,
    ) -> Result<Option<AsyncFileSlice>, ErrorCode> {
        let reader = self.read().await;
        if let Some((_offset, segment)) = reader.find_segment(start_offset) {
            if let Some(slice) = segment.records_slice(start_offset, max_offset).await? {
                Ok(Some(slice))
            } else {
                Err(ErrorCode::Other(format!(
                    "slice not found in start_offset: {start_offset}, segment: {segment:#?} "
                )))
            }
        } else {
            Ok(None)
        }
    }

    #[instrument(skip(self))]
    async fn remove_segment(&self, base_offset: &Offset) {
        let mut write = self.write().await;

        if let Some((old_segment, min_offset)) = write.remove_segment(base_offset) {
            drop(write);
            self.min_offset.store(min_offset, MEM_ORDER);
            if let Err(err) = old_segment.remove().await {
                error!("failed to remove segment: {:#?}", err);
            }
        }
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
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn occupied_memory(&self) -> Size64 {
        self.segments
            .values()
            .map(ReadSegment::occupied_memory)
            .sum()
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
    #[cfg(feature = "fixture")]
    pub fn get_segment(&self, offset: Offset) -> Option<&ReadSegment> {
        self.segments.get(&offset)
    }

    pub fn find_segment(&self, offset: Offset) -> Option<(&Offset, &ReadSegment)> {
        if offset < self.min_offset {
            None
        } else if offset == self.min_offset {
            self.segments
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
            self.segments.range(range).next_back()
        }
    }

    pub(crate) fn find_expired_segments(&self, expired_duration: &Duration) -> Vec<Offset> {
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

    #[instrument(skip(self))]
    pub(crate) fn find_first(&self, count: usize) -> Vec<Offset> {
        self.segments.keys().take(count).copied().collect()
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;
    use std::sync::Arc;

    use anyhow::Result;

    use flv_util::fixture::ensure_new_dir;
    use fluvio_protocol::fixture::create_batch;
    use fluvio_protocol::record::Offset;

    use crate::config::SharedReplicaConfig;
    use crate::segment::MutableSegment;
    use crate::segment::ReadSegment;
    use crate::config::ReplicaConfig;

    use super::{SegmentList, SharedSegments};

    // create fake segment, this doesn't create a segment with all data, it just fill with a min data but with a valid end offset
    async fn create_segment(
        option: Arc<SharedReplicaConfig>,
        start: Offset,
        end_offset: Offset,
    ) -> Result<ReadSegment> {
        let mut mut_segment = MutableSegment::create(start, option).await?;
        mut_segment.append_batch(&mut create_batch()).await?;
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
        println!("segments: {list:#?}");
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
        println!("segments: {list:#?}");

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

        println!("segments: {list:#?}");

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

        println!("segments: {list:#?}");

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
    async fn test_find_and_remove_many() {
        //given
        let rep_dir = temp_dir().join("segmentlist-remove-many");
        ensure_new_dir(&rep_dir).expect("new");
        let option = default_option(rep_dir).shared();
        let (segments, _) = SharedSegments::from_dir(option.clone())
            .await
            .expect("from");
        segments
            .add_segment(
                create_segment(option.clone(), 100, 600)
                    .await
                    .expect("create"),
            )
            .await;

        segments
            .add_segment(
                create_segment(option.clone(), 600, 4000)
                    .await
                    .expect("create"),
            )
            .await;

        segments
            .add_segment(
                create_segment(option.clone(), 4000, 9000)
                    .await
                    .expect("create"),
            )
            .await;

        let list = segments.read().await;

        assert!(list.find_first(0).is_empty());
        assert_eq!(list.find_first(1), vec![100]);
        assert_eq!(list.find_first(2), vec![100, 600]);
        assert_eq!(list.find_first(10), vec![100, 600, 4000]);

        //when
        let offsets = list.find_first(10);
        drop(list);
        segments.remove_segments(&offsets).await;

        //then
        assert!(segments.read().await.find_first(10).is_empty());
    }
}
