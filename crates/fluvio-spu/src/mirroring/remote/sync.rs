use std::fmt::Debug;
use std::fmt::Display;
use std::fmt;
use std::io::Error as IoError;

use bytes::BytesMut;
use tracing::trace;

use fluvio_protocol::store::StoreValue;
use fluvio_protocol::store::FileWrite;
use fluvio_protocol::{Encoder, Decoder, Version};
use fluvio_protocol::record::RecordSet;
use fluvio_protocol::api::Request;
use fluvio_protocol::record::RawRecords;
use fluvio_spu_schema::file::FileRecordSet;

use crate::mirroring::COMMON_MIRROR_VERSION;

use super::api_key::MirrorRemoteApiEnum;

pub type RemoteFilePartitionSyncRequest = MirrorPartitionSyncRequest<FileRecordSet>;
pub type DefaultRemotePartitionSyncRequest = MirrorPartitionSyncRequest<RecordSet<RawRecords>>;

#[derive(Encoder, Decoder, Default, Debug)]
pub struct MirrorPartitionSyncRequest<R>
where
    R: Encoder + Decoder + Default + Debug,
{
    pub hw: i64,
    pub leo: i64,
    pub records: R,
}

impl<R> fmt::Display for MirrorPartitionSyncRequest<R>
where
    R: Encoder + Decoder + Debug + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MirrorRemotePartitionRequest")
    }
}

// Request trait
// Note that DEFAULT_API_VERSION is 7 which is required in order to map all fields for file encoding
// TODO: come up with unify encoding
impl<R> Request for MirrorPartitionSyncRequest<R>
where
    R: Encoder + Decoder + Debug,
{
    const API_KEY: u16 = MirrorRemoteApiEnum::SyncRecords as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = MirrorPartitionSyncResponse;
}

#[derive(Default, Encoder, Decoder, Debug)]
pub struct MirrorPartitionSyncResponse {}

impl FileWrite for RemoteFilePartitionSyncRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch partition response");
        self.hw.encode(src, version)?;
        self.leo.encode(src, version)?;
        self.records.file_encode(src, data, version)?;
        Ok(())
    }
}
