mod apis;
mod create_topics;
mod delete_topics;
mod custom_spu_register;
mod custom_spu_unregister;
mod fetch_spus;
mod create_spu_groups;
mod delete_spu_groups;
mod fetch_spu_groups;
mod fetch_topics;
mod topic_composition;
pub mod versions;
mod request;
mod common;
mod replica;
mod update_all_metadata;

pub use apis::ScServerApiKey;
pub use request::ScServerRequest;

pub use common::flv_response_message::FlvResponseMessage;

pub mod spu {
    pub use super::custom_spu_register::*;
    pub use super::custom_spu_unregister::*;
    pub use super::fetch_spus::*;
    pub use super::create_spu_groups::*;
    pub use super::delete_spu_groups::*;
    pub use super::fetch_spu_groups::*;
    pub use super::common::flv_spus::*;
}

pub mod topic {
    pub use super::create_topics::*;
    pub use super::delete_topics::*;
    pub use super::fetch_topics::*;
    pub use super::topic_composition::*;

    pub use flv_metadata::topic::TopicSpec as FlvTopicSpecMetadata;
    pub use flv_metadata::topic::PartitionMap as FlvTopicPartitionMap;
    pub use flv_metadata::topic::TopicResolution as FlvTopicResolution;
}

/// Error from api call
#[derive(Debug)]
pub enum ApiError {
    Code(kf_protocol::api::FlvErrorCode, Option<String>),
    NoResourceFounded(String),
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Code(code, msg_opt) => {
                if let Some(msg) = msg_opt {
                    write!(f, "{:#?} {}", code, msg)
                } else {
                    write!(f, "{:#?}", code)
                }
            }
            Self::NoResourceFounded(msg) => write!(f, "no resource founded {}", msg),
        }
    }
}
