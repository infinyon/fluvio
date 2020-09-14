use std::fmt::Debug;

use kf_protocol::Decoder;
use kf_protocol::Encoder;

use kf_protocol_api::RecordSet;

use crate::produce::{KfProduceResponse, KfProduceRequest};
use crate::produce::TopicProduceData;
use crate::produce::{PartitionProduceData, PartitionProduceResponse};

pub type DefaultKfProduceRequest = KfProduceRequest<RecordSet>;
pub type DefaultKfTopicRequest = TopicProduceData<RecordSet>;
pub type DefaultKfPartitionRequest = PartitionProduceData<RecordSet>;

// -----------------------------------
// Implementation - KfProduceRequest
// -----------------------------------

impl <R>KfProduceRequest<R> where R: Encoder + Decoder + Debug {

    /// Find partition in request
    pub fn find_partition_request(&self, topic: &str, partition: i32) -> Option<&PartitionProduceData<R>> {
        if let Some(request) = self.topics.iter().find(|request| request.name == topic) {
             request.partitions.iter().find( |part_request| part_request.partition_index == partition)
        } else {
            None
        }
    }
}

// -----------------------------------
// Implementation - KfProduceResponse
// -----------------------------------

impl KfProduceResponse {

    /// Find partition in Response
    pub fn find_partition_response(&self, topic: &str, partition: i32) -> Option<&PartitionProduceResponse> {

        if let Some(response) = self.responses.iter().find(|response| response.name == topic) {
             response.partitions.iter().find( |part_response| part_response.partition_index == partition)
        } else {
            None
        }
    }
}
