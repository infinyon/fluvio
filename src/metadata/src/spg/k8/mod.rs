//!
//! # Cluster
//!
//! Interface to the Cluster metadata in K8 key value store
//!
mod spec;


pub use self::spec::*;


mod ext {
    use k8_obj_metadata::Status as K8Status;
    use crate::spg::SpuGroupStatus;


    /// implement k8 status for spu group status because they are same
    impl K8Status for SpuGroupStatus{}


}
