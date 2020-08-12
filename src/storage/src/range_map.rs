use std::cmp::max;
use std::cmp::min;
use std::collections::BTreeMap;
use std::ops::Bound::Excluded;
use std::ops::Bound::Included;
use std::ffi::OsStr;

use tracing::debug;
use tracing::trace;
use tracing::error;

use kf_protocol::api::Offset;

use crate::segment::ReadSegment;
use crate::StorageError;
use crate::ConfigOption;
use crate::util::log_path_get_offset;

#[derive(Debug)]
pub(crate) struct SegmentList {
    segments: BTreeMap<Offset, ReadSegment>,
    max_base_offset: Offset, // maximum number of offset for all segments
    min_base_offset: Offset,
}

impl SegmentList {
    pub fn new() -> Self {
        SegmentList {
            segments: BTreeMap::new(),
            max_base_offset: 0,
            min_base_offset: -1,
        }
    }

    // load segments
    pub async fn from_dir(
        option: &ConfigOption,
    ) -> Result<(SegmentList, Option<Offset>), StorageError> {
        let dirs = option.base_dir.read_dir()?;
        debug!("reading segments at: {:#?}", dirs);
        let files: Vec<_> = dirs.into_iter().filter_map(|entry| entry.ok()).collect();
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

        offsets.sort();

        let last_offset = offsets.pop();
        let mut segments = Self::new();

        for offset in offsets {
            match ReadSegment::open_for_read(offset, option).await {
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

    #[allow(dead_code)]
    pub fn max_offset(&self) -> Offset {
        self.max_base_offset
    }

    pub fn min_offset(&self) -> Offset {
        self.min_base_offset
    }

    pub fn add_segment(&mut self, segment: ReadSegment) {
        let base_offset = segment.get_base_offset();
        debug!("inserting segment base: {}", base_offset);
        self.max_base_offset = max(self.max_base_offset, base_offset);
        self.min_base_offset = if self.min_base_offset < 0 {
            base_offset
        } else {
            min(self.min_base_offset, base_offset)
        };
        &self.segments.insert(segment.get_base_offset(), segment);
    }

    #[allow(dead_code)]
    pub fn get_segment(&self, offset: Offset) -> Option<&ReadSegment> {
        self.segments.get(&offset)
    }

    pub fn find_segment(&self, offset: Offset) -> Option<(&Offset, &ReadSegment)> {
        (&self.segments)
            .range((Excluded(offset - self.max_base_offset), Included(offset)))
            .next_back()
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::path::PathBuf;

    use flv_future_aio::test_async;
    use kf_protocol::api::Offset;
    use flv_util::fixture::ensure_new_dir;

    use super::SegmentList;
    use crate::StorageError;
    use crate::segment::MutableSegment;
    use crate::segment::ReadSegment;
    use crate::ConfigOption;
    use crate::fixture::create_batch;

    const TEST_SEGMENT_DIR: &str = "segmentlist-test";

    async fn create_segment(
        option: &ConfigOption,
        start: Offset,
        _offsets: Offset,
    ) -> Result<ReadSegment, StorageError> {
        let mut mut_segment = MutableSegment::create(start, option).await?;
        mut_segment.send(create_batch()).await?;
        //        mut_segment.set_end_offset(offsets);
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

    #[test_async]
    async fn test_find_segment() -> Result<(), StorageError> {
        let rep_dir = temp_dir().join(TEST_SEGMENT_DIR);
        ensure_new_dir(&rep_dir)?;
        let mut list = SegmentList::new();

        let option = default_option(rep_dir);

        list.add_segment(create_segment(&option, 0, 500).await?);
        list.add_segment(create_segment(&option, 500, 2000).await?);
        list.add_segment(create_segment(&option, 2000, 1000).await?);
        list.add_segment(create_segment(&option, 3000, 2000).await?);

        let index = list.find_segment(1500);

        assert!(index.is_some());
        let (pos, _) = index.unwrap();
        assert_eq!(*pos, 500);

        Ok(())
    }

    const TEST_READ_DIR: &str = "segmentlist-read-many";

    #[test_async]
    async fn test_segment_read_many() -> Result<(), StorageError> {
        let rep_dir = temp_dir().join(TEST_READ_DIR);
        ensure_new_dir(&rep_dir)?;
        let option = default_option(rep_dir);

        create_segment(&option, 10, 500).await?;
        create_segment(&option, 500, 2000).await?;
        create_segment(&option, 2000, 1000).await?;
        create_segment(&option, 3000, 2000).await?;

        let (segments, last_offset_res) = SegmentList::from_dir(&option).await?;

        assert_eq!(segments.len(), 3); // 0,500,2000
        assert_eq!(segments.max_offset(), 2000);
        assert_eq!(segments.min_offset(), 10);
        let segment1 = segments.get_segment(10).expect("should have segment at 0 ");
        assert_eq!(segment1.get_base_offset(), 10);
        let last_offset = last_offset_res.expect("last segment should be there");
        assert_eq!(last_offset, 3000);
        let segment2 = segments
            .get_segment(500)
            .expect("should have segment at 500");
        assert_eq!(segment2.get_base_offset(), 500);

        Ok(())
    }

    const TEST_EMPTY_DIR: &str = "segmentlist-read-empty";

    #[test_async]
    async fn test_segment_read_empty() -> Result<(), StorageError> {
        let rep_dir = temp_dir().join(TEST_EMPTY_DIR);
        ensure_new_dir(&rep_dir)?;
        let option = default_option(rep_dir);

        let (segments, last_segment) = SegmentList::from_dir(&option).await?;

        assert_eq!(segments.len(), 0); // 0,500,2000
        assert!(last_segment.is_none());
        Ok(())
    }
}
