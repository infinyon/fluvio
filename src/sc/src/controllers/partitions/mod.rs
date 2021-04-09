mod controller;
mod reducer;

pub use self::controller::*;
pub use common::*;

mod common {

    use fluvio_controlplane_metadata::partition::PartitionSpec;
    use crate::stores::actions::WSAction;

    #[allow(clippy::upper_case_acronyms)]
    pub type PartitionWSAction = WSAction<PartitionSpec>;
}
