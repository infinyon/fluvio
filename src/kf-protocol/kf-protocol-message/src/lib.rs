mod kf_code_gen;
mod kf_handler;

pub mod fetch_handler;
pub mod produce_handler;

pub type KfApiVersions = Vec<kf_code_gen::api_versions::ApiVersionsResponseKey>;

pub mod api_versions {
   pub use crate::kf_code_gen::api_versions::*;
}

pub mod topic {
   pub use crate::kf_code_gen::create_topics::*;
   pub use crate::kf_code_gen::delete_topics::*;
}

pub mod metadata {
   pub use crate::kf_code_gen::metadata::*;
   pub use crate::kf_code_gen::update_metadata::*;
}

pub mod produce {
   pub use crate::kf_code_gen::produce::*;
   pub use crate::produce_handler::DefaultKfProduceRequest;
   pub use crate::produce_handler::DefaultKfTopicRequest;
   pub use crate::produce_handler::DefaultKfPartitionRequest;
}

pub mod fetch {
   pub use crate::kf_code_gen::fetch::*;
   pub use crate::fetch_handler::DefaultKfFetchResponse;
   pub use crate::fetch_handler::DefaultKfFetchRequest;
}

pub mod group {
   pub use crate::kf_code_gen::find_coordinator::*;
   pub use crate::kf_code_gen::join_group::*;
   pub use crate::kf_code_gen::sync_group::*;
   pub use crate::kf_code_gen::leave_group::*;
   pub use crate::kf_code_gen::delete_groups::*;
   pub use crate::kf_code_gen::list_groups::*;
   pub use crate::kf_code_gen::describe_groups::*;
   pub use crate::kf_code_gen::heartbeat::*;
}

pub mod offset {
   pub use crate::kf_code_gen::list_offset::*;
   pub use crate::kf_code_gen::offset_fetch::*;
}

pub mod isr {
   pub use crate::kf_code_gen::leader_and_isr::*;
}
