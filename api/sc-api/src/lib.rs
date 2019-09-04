mod api_key;
mod flv_create_topics;
mod flv_delete_topics;
mod flv_create_custom_spus;
mod flv_delete_custom_spus;
mod flv_fetch_spus;
mod flv_create_spu_groups;
mod flv_delete_spu_groups;
mod flv_fetch_spu_groups;
mod flv_fetch_topics;
mod flv_topic_composition;
mod api_versions;
mod public_api;
mod common;

pub use api_key::ScApiKey;
pub use public_api::PublicRequest;

pub use crate::common::flv_response_message::FlvResponseMessage;

pub mod apis {
    pub use crate::api_key::*;
}

pub mod versions {
    pub use crate::api_versions::*;
}

pub mod errors {
    pub use kf_protocol::api::FlvErrorCode;
}

pub mod spu {
    pub use crate::flv_create_custom_spus::*;
    pub use crate::flv_delete_custom_spus::*;
    pub use crate::flv_fetch_spus::*;

    pub use crate::flv_create_spu_groups::*;
    pub use crate::flv_delete_spu_groups::*;
    pub use crate::flv_fetch_spu_groups::*;

    pub use crate::common::flv_spus::*;
}

pub mod topic {
    pub use crate::flv_create_topics::*;
    pub use crate::flv_delete_topics::*;
    pub use crate::flv_fetch_topics::*;
    pub use crate::flv_topic_composition::*;

    pub use metadata::topic::TopicSpec as FlvTopicSpecMetadata;
    pub use metadata::topic::PartitionMap as FlvTopicPartitionMap;
    pub use metadata::topic::TopicResolution as FlvTopicResolution;
}
