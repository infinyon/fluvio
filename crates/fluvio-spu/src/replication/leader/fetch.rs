use std::fmt::Debug;
use std::marker::PhantomData;


use dataplane::core::{Encoder, Decoder};
use dataplane::api::Request;
use dataplane::fetch::FetchablePartitionResponse;
use dataplane::record::RecordSet;
use dataplane::Isolation;

pub type DefaultStreamFetchResponse = StreamFetchResponse<RecordSet>;

pub type DefaultStreamFetchRequest = StreamFetchRequest<RecordSet>;

use super::LeaderPeerApiEnum;

/// Based on SPU client schema
#[derive(Decoder, Encoder, Default, Debug)]
pub struct StreamFetchRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub partition: i32,
    pub fetch_offset: i64,
    pub max_bytes: i32,
    pub isolation: Isolation,
    pub data: PhantomData<R>,
}

impl<R> Request for StreamFetchRequest<R>
where
    R: Debug + Decoder + Encoder,
{
    const API_KEY: u16 = LeaderPeerApiEnum::StreamFetch as u16;
    const DEFAULT_API_VERSION: i16 = 0;
    type Response = StreamFetchResponse<R>;
}

#[derive(Encoder, Decoder, Default, Debug)]
pub struct StreamFetchResponse<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub topic: String,
    pub stream_id: u32,
    pub partition: FetchablePartitionResponse<R>,
}

mod file {

    use std::io::Error as IoError;

    use tracing::trace;
    use bytes::BytesMut;

    use dataplane::core::Version;
    use dataplane::store::StoreValue;
    use dataplane::record::FileRecordSet;
    use dataplane::store::FileWrite;

    pub type FileStreamFetchRequest = StreamFetchRequest<FileRecordSet>;

    use super::*;

    impl FileWrite for StreamFetchResponse<FileRecordSet> {
        fn file_encode(
            &self,
            src: &mut BytesMut,
            data: &mut Vec<StoreValue>,
            version: Version,
        ) -> Result<(), IoError> {
            trace!("file encoding FlvContinuousFetchResponse");
            trace!("topic {}", self.topic);
            self.topic.encode(src, version)?;
            self.stream_id.encode(src, version)?;
            self.partition.file_encode(src, data, version)?;
            Ok(())
        }
    }
}
