use kf_protocol_api::PartitionOffset;

use crate::offset::KfListOffsetResponse;
use crate::offset::ListOffsetPartitionResponse;

impl KfListOffsetResponse {

    pub fn find_partition(self,topic: &str,partition: i32) -> Option<ListOffsetPartitionResponse> {

        for topic_res in self.topics {
            if topic_res.name == topic {
                for partition_res in topic_res.partitions {
                    if partition_res.partition_index == partition {
                        return Some(partition_res);
                    }
                }
            }
        }

        None

    }

}

impl PartitionOffset for ListOffsetPartitionResponse {

    fn last_stable_offset(&self) -> i64 {
        self.offset
    }


    // we don't have start offset yet for kafka
    fn start_offset(&self) -> i64 {
        self.offset
    }
}
