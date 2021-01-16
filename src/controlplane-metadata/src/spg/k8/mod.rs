//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!
mod spec;

pub use self::spec::*;

mod ext {
    use crate::k8_types::Status as K8Status;
    use crate::spg::SpuGroupStatus;

    /// implement k8 status for spu group status because they are same
    impl K8Status for SpuGroupStatus {}
}
