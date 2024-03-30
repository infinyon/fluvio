pub mod spu;
pub mod topic;
pub mod partition;
pub mod spg;
pub mod smartmodule;
pub mod tableformat;
pub mod message;
pub mod remote_cluster;
pub mod upstream_cluster;

pub use fluvio_stream_model::core;

pub mod store {
    pub use fluvio_stream_model::store::*;
}

#[cfg(feature = "k8")]
pub use fluvio_stream_model::k8_types;

pub mod extended {

    use super::core::Spec;

    #[derive(Debug, Clone, PartialEq, Hash, Eq)]
    #[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
    pub enum ObjectType {
        Spu,
        CustomSpu,
        SpuGroup,
        Topic,
        Partition,
        ManagedConnector,
        SmartModule,
        TableFormat,
        DerivedStream,
    }

    pub trait SpecExt: Spec {
        const OBJECT_TYPE: ObjectType;
    }
}
