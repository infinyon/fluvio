use std::fmt::Debug;
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

use crate::mirroring::remote::sync::MirrorPartitionSyncRequest;
use crate::mirroring::remote::sync::MirrorPartitionSyncResponse;
use crate::mirroring::COMMON_MIRROR_VERSION;

use super::api_key::MirrorHomeApiEnum;

pub type HomeFilePartitionSyncRequest = MirrorPartitionSyncRequestWrapper<FileRecordSet>;
pub type DefaultHomePartitionSyncRequest = MirrorPartitionSyncRequestWrapper<RecordSet<RawRecords>>;

#[derive(Encoder, Decoder, Default, Debug)]
pub(crate) struct MirrorPartitionSyncRequestWrapper<R: Encoder + Decoder + Debug>(
    MirrorPartitionSyncRequest<R>,
);

impl<R> From<MirrorPartitionSyncRequest<R>> for MirrorPartitionSyncRequestWrapper<R>
where
    R: Encoder + Decoder + Debug,
{
    fn from(request: MirrorPartitionSyncRequest<R>) -> Self {
        Self(request)
    }
}

impl<R> Request for MirrorPartitionSyncRequestWrapper<R>
where
    R: Encoder + Decoder + Debug,
{
    const API_KEY: u16 = MirrorHomeApiEnum::SyncRecords as u16;
    const DEFAULT_API_VERSION: i16 = COMMON_MIRROR_VERSION;
    type Response = MirrorPartitionSyncResponse;
}

impl<R> MirrorPartitionSyncRequestWrapper<R>
where
    R: Encoder + Decoder + Debug,
{
    pub fn inner(self) -> MirrorPartitionSyncRequest<R> {
        self.0
    }
}

impl FileWrite for HomeFilePartitionSyncRequest {
    fn file_encode(
        &self,
        src: &mut BytesMut,
        data: &mut Vec<StoreValue>,
        version: Version,
    ) -> Result<(), IoError> {
        trace!("file encoding fetch partition response");
        self.0.file_encode(src, data, version)?;
        Ok(())
    }
}
