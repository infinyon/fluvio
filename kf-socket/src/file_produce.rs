use std::io::Error as IoError;

use log::trace;

use future_aio::BytesMut;
use kf_protocol::Encoder;
use kf_protocol::Version;
use kf_protocol::message::produce::KfProduceRequest;
use kf_protocol::message::produce::TopicProduceData;
use kf_protocol::message::produce::PartitionProduceData;


use crate::FileWrite;
use crate::StoreValue;
use super::file_fetch::KfFileRecordSet;


pub type FileProduceRequest = KfProduceRequest<KfFileRecordSet>;
pub type FileTopicRequest = TopicProduceData<KfFileRecordSet>;
pub type FilePartitionRequest = PartitionProduceData<KfFileRecordSet>;



impl FileWrite for FileProduceRequest {

      fn file_encode<'a: 'b,'b>(&'a self, src: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError> {
        trace!("file encoding produce request");
        self.transactional_id.encode(src,version)?;
        self.acks.encode(src,version)?;
        self.timeout_ms.encode(src,version)?;
        self.topics.file_encode(src,data,version)?;
        Ok(())
    }
}


impl FileWrite for FileTopicRequest {

     fn file_encode<'a: 'b,'b>(&'a self, src: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError>  {
        trace!("file encoding produce topic request");
        self.name.encode(src,version)?;
        self.partitions.file_encode(src,data,version)?;
        Ok(())
    }
}


impl FileWrite for FilePartitionRequest {

    fn file_encode<'a: 'b,'b>(&'a self, src: &mut BytesMut, data: &'b mut Vec<StoreValue<'a>>,version: Version) -> Result<(), IoError>  {
        trace!("file encoding for partition request");
        self.partition_index.encode(src,version)?;
        self.records.file_encode(src,data,version)?;
        Ok(())
    }   
}


