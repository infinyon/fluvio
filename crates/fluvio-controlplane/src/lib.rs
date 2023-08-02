pub mod sc_api;
pub mod spu_api;
pub mod replica;
pub mod message;
pub mod requests;
pub mod remote_cluster;
pub mod upstream_cluster;

pub use alias::*;
mod alias {
    use fluvio_controlplane_metadata::{store::MetadataStoreObject, partition::PartitionSpec};

    pub type PartitionMetadata<C> = MetadataStoreObject<PartitionSpec, C>;
}

pub const CONSUMER_STORAGE_TOPIC: &str = "consumer-offset";
