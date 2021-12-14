use std::cmp::max;
use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Bound::Included;
use std::ffi::OsStr;

use tracing::debug;
use tracing::trace;
use tracing::error;

use dataplane::Offset;

use crate::segment::ReadSegment;
use crate::StorageError;
use crate::config::ConfigOption;
use crate::util::log_path_get_offset;

#[derive(Debug)]
pub(crate) struct SegmentList {
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
    pub async fn from_dir(
        option: &ConfigOption,
    ) -> Result<(SegmentList, Option<Offset>), StorageError> {
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
        let mut segments = Self::new();

        for offset in offsets {
            // for now, set end offset same as base, this will be reset when validation occurs
            match ReadSegment::open_unknown(offset, option).await {
                Ok(segment) => segments.add_segment(segment),
                Err(err) => error!("error opening segment: {:#?}", err),
            }
        }

        Ok((segments, last_offset))
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn min_offset(&self) -> Offset {
        self.min_offset
    }

    pub fn add_segment(&mut self, segment: ReadSegment) {
        debug!(
            base_offset = segment.get_base_offset(),
            end_offset = segment.get_end_offset(),
            "inserting"
        );
        self.max_offset = max(self.max_offset, segment.get_end_offset());
        self.min_offset = if self.min_offset < 0 {
            segment.get_base_offset()
        } else {
            min(self.min_offset, segment.get_base_offset())
        };
        self.segments.insert(segment.get_base_offset(), segment);
    }

    #[allow(dead_code)]
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
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;

    use flv_util::fixture::ensure_new_dir;
    use dataplane::fixture::create_batch;
    use dataplane::Offset;

    use crate::StorageError;
    use crate::segment::MutableSegment;
    use crate::segment::ReadSegment;
    use crate::config::ConfigOption;

    use super::SegmentList;

    // create fake segment, this doesn't create a segment with all data, it just fill with a min data but with a valid end offset
    async fn create_segment(
        option: &ConfigOption,
        start: Offset,
        end_offset: Offset,
    ) -> Result<ReadSegment, StorageError> {
        let mut mut_segment = MutableSegment::create(start, option).await?;
        mut_segment.write_batch(&mut create_batch()).await?;
        mut_segment.set_end_offset(end_offset); // only used for testing
        let segment = mut_segment.convert_to_segment().await?;
        Ok(segment)
    }

    fn default_option(base_dir: PathBuf) -> ConfigOption {
        ConfigOption {
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
        let option = default_option(rep_dir);

        let (segments, last_segment) = SegmentList::from_dir(&option).await.expect("from");

        assert_eq!(segments.len(), 0); // 0,500,2000
        assert!(last_segment.is_none());
    }

    #[fluvio_future::test]
    async fn test_segment_single_base_zero() {
        let rep_dir = temp_dir().join("segmentlist-single-zero");
        ensure_new_dir(&rep_dir).expect("new");
        let mut list = SegmentList::new();

        let option = default_option(rep_dir);

        list.add_segment(create_segment(&option, 0, 500).await.expect("create"));
        println!("segments: {:#?}", list);

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

        let option = default_option(rep_dir);

        list.add_segment(create_segment(&option, 100, 500).await.expect("create"));
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

        let option = default_option(rep_dir);

        list.add_segment(create_segment(&option, 0, 500).await.expect("create"));
        list.add_segment(create_segment(&option, 500, 2000).await.expect("create"));
        list.add_segment(create_segment(&option, 2000, 3000).await.expect("create"));
        list.add_segment(create_segment(&option, 3000, 4000).await.expect("create"));

        println!("segments: {:#?}", list);

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

        let option = default_option(rep_dir);

        list.add_segment(create_segment(&option, 100, 600).await.expect("create"));
        list.add_segment(create_segment(&option, 600, 4000).await.expect("create"));
        list.add_segment(create_segment(&option, 4000, 9000).await.expect("create"));

        println!("segments: {:#?}", list);

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
}
