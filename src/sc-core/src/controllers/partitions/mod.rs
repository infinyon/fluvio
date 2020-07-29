mod controller;
mod reducer;

pub use self::controller::*;
pub use common::*;

mod common {

    use flv_metadata_cluster::partition::PartitionSpec;
    use crate::metadata::WSAction;

    pub type PartitionWSAction = WSAction<PartitionSpec>;
}
