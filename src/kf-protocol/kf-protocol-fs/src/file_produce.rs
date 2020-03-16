use std::io::Error as IoError;

use log::trace;

use flv_future_aio::bytes::BytesMut;
use kf_protocol_core::Encoder;
use kf_protocol_core::Version;
use kf_protocol_message::produce::KfProduceRequest;
use kf_protocol_message::produce::TopicProduceData;
use kf_protocol_message::produce::PartitionProduceData;

use crate::FileWrite;
use crate::StoreValue;
use super::file_fetch::KfFileRecordSet;

pub type FileProduceRequest = KfProduceRequest<KfFileRecordSet>;
pub type FileTopicRequest = TopicProduceData<KfFileRecordSet>;
pub type FilePartitionRequest = PartitionProduceData<KfFileRecordSet>;

impl FileWrite for FileProduceRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding produce request");
        self.transactional_id.encode(src, version)?;
        self.acks.encode(src, version)?;
        self.timeout_ms.encode(src, version)?;
        self.topics.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FileTopicRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding produce topic request");
        self.name.encode(src, version)?;
        self.partitions.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FilePartitionRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding for partition request");
        self.partition_index.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}
