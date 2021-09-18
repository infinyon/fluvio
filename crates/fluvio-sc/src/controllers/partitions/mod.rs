mod controller;
mod reducer;

pub use self::controller::*;
pub use common::*;

mod common {

    use fluvio_controlplane_metadata::{partition::PartitionSpec, store::k8::K8MetaItem};
    use crate::stores::actions::WSAction;

    pub type PartitionWSAction<C = K8MetaItem> = WSAction<PartitionSpec, C>;
}
