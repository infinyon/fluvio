mod controller;
mod reducer;

pub use self::controller::*;
pub use common::*;

mod common {

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use crate::stores::actions::WSAction;

    pub type PartitionWSAction = WSAction<PartitionSpec>;
}
