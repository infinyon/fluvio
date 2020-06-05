mod metadata;
mod world_store;

pub mod common;
pub mod partitions;
pub mod spus;
pub mod topics;

pub use self::metadata::{LocalStores, ShareLocalStores};
pub use self::world_store::WSUpdateService;
pub use self::world_store::WSChangeChannel;
pub use self::world_store::WSChangeDispatcher;

use std::io::Error as IoError;

use k8_metadata::metadata::Spec as K8Spec;
use k8_metadata::metadata::K8Obj;

use crate::core::common::KVObject;

pub trait Spec: Default + Clone {
    const LABEL: &'static str;

    type Status: Status;
    type K8Spec: K8Spec;
    type Owner: Spec;

    type Key: Ord + Clone + ToString;

    // convert kubernetes objects into KV value
    fn convert_from_k8(k8_obj: K8Obj<Self::K8Spec>) -> Result<KVObject<Self>, IoError>;
}

pub trait Status: Default + Clone {}
