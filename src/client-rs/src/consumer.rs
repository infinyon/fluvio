use kf_protocol::api::*;
use kf_protocol::message::fetch::FetchablePartitionResponse;


use crate::client::*;
use crate::ClientError;
use crate::params::*;

pub struct Consumer {
    #[allow(unused)]
    serial: SerialClient,
    topic: String,
    partition: i32
}

impl Consumer  {

    pub fn new(serial: SerialClient,topic: &str,partition: i32) -> Self {
        Self {
            serial,
            topic: topic.to_owned(),
            partition
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub async fn fetch_logs_once(
        &mut self,
        _offset_option: FetchOffset,
        _option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, ClientError> {

        todo!()
    }
}