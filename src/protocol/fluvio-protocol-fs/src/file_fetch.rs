use std::io::Error as IoError;
use std::fmt;

use log::trace;

use flv_future_aio::fs::AsyncFileSlice;
use flv_future_aio::bytes::BufMut;
use flv_future_aio::bytes::BytesMut;
use kf_protocol_core::Version;
use kf_protocol_core::Encoder;
use kf_protocol_core::Decoder;
use kf_protocol_core::bytes::Buf;
use kf_protocol_message::fetch::KfFetchResponse;
use kf_protocol_message::fetch::KfFetchRequest;
use kf_protocol_message::fetch::FetchableTopicResponse;
use kf_protocol_message::fetch::FetchablePartitionResponse;

use crate::StoreValue;
use crate::FileWrite;

pub type FileFetchResponse = KfFetchResponse<KfFileRecordSet>;
pub type FileTopicResponse = FetchableTopicResponse<KfFileRecordSet>;
pub type FilePartitionResponse = FetchablePartitionResponse<KfFileRecordSet>;

#[derive(Default, Debug)]
pub struct KfFileRecordSet(AsyncFileSlice);

pub type KfFileFetchRequest = KfFetchRequest<KfFileRecordSet>;

impl fmt::Display for KfFileRecordSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "pos: {} len: {}", self.position(), self.len())
    }
}

impl KfFileRecordSet {
    pub fn position(&self) -> u64 {
        self.0.position()
    }

    pub fn len(&self) -> usize {
        self.0.len() as usize
    }

    pub fn raw_slice(&self) -> AsyncFileSlice {
        self.0.clone()
    }
}

impl From<AsyncFileSlice> for KfFileRecordSet {
    fn from(slice: AsyncFileSlice) -> Self {
        Self(slice)
    }
}

impl Encoder for KfFileRecordSet {
    fn write_size(&self, _version: Version) -> usize {
        self.len() + 4 // include header
    }

    fn encode<T>(&self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: BufMut,
    {
        unimplemented!("file slice cannot be encoded in the ButMut")
    }
}

impl Decoder for KfFileRecordSet {
    fn decode<T>(&mut self, _src: &mut T, _version: Version) -> Result<(), IoError>
    where
        T: Buf,
    {
        unimplemented!("file slice cannot be decoded in the ButMut")
    }
}

impl FileWrite for KfFileRecordSet {
    fn file_encode(
        &self,
        dest: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        let len: i32 = self.len() as i32;
        trace!("KfFileRecordSet encoding file slice len: {}", len);
        len.encode(dest, version)?;
        let bytes = dest.split_to(dest.len()).freeze();
        data.push(StoreValue::Bytes(bytes));
        data.push(StoreValue::FileSlice(self.raw_slice()));
        Ok(())
    }
}

impl FileWrite for FileFetchResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding FileFetchResponse");
        trace!("encoding throttle_time_ms {}", self.throttle_time_ms);
        self.throttle_time_ms.encode(src, version)?;
        trace!("encoding error code {:#?}", self.error_code);
        self.error_code.encode(src, version)?;
        trace!("encoding session code {}", self.session_id);
        self.session_id.encode(src, version)?;
        trace!("encoding topics len: {}", self.topics.len());
        self.topics.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FileTopicResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch topic response");
        self.name.encode(src, version)?;
        self.partitions.file_encode(src, data, version)?;
        Ok(())
    }
}

impl FileWrite for FilePartitionResponse {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch partition response");
        self.partition_index.encode(src, version)?;
        self.error_code.encode(src, version)?;
        self.high_watermark.encode(src, version)?;
        self.last_stable_offset.encode(src, version)?;
        self.log_start_offset.encode(src, version)?;
        self.aborted.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}
