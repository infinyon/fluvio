mod api;
mod kf_api;
mod request;
mod response;
mod error;
mod batch;
mod record;
mod common;
mod group_protocol_metadata;
mod group_assigment;
mod flv_errors;

pub type Offset = i64;
pub type Size = u32;

pub use self::api::KfApiKey;
pub use self::api::KfRequestMessage;
pub use self::api::Request;
pub use self::api::RequestHeader;
pub use self::kf_api::AllKfApiKey;
pub use self::request::RequestMessage;

pub use self::response::ResponseMessage;

pub use self::batch::Batch;
pub use self::batch::BatchRecords;
pub use self::batch::DefaultBatch;
pub use self::batch::DefaultBatchRecords;
pub use self::record::DefaultRecord;
pub use self::record::RecordSet;
pub use self::record::Record;
pub use self::record::RecordHeader;
pub use self::batch::BATCH_HEADER_SIZE;
pub use self::batch::BATCH_PREAMBLE_SIZE;
pub use self::group_protocol_metadata::ProtocolMetadata;
pub use self::group_protocol_metadata::Metadata;
pub use self::group_assigment::GroupAssignment;
pub use self::group_assigment::Assignment;
pub use self::common::*;

pub use self::error::ErrorCode;
pub use self::flv_errors::FlvErrorCode;

pub const MAX_BYTES: i32 = 52428800;

#[macro_export]
macro_rules! api_decode {
    ($api:ident,$req:ident,$src:expr,$header:expr) => {{
        use kf_protocol::Decoder;
        let request = $req::decode_from($src, $header.api_version())?;
        Ok($api::$req(RequestMessage::new($header, request)))
    }};
}

/// Offset information about Partition
pub trait PartitionOffset {

    /// last offset that was committed
    fn last_stable_offset(&self) -> i64;

    // beginning offset for the partition
    fn start_offset(&self) -> i64;


}