pub use fluvio_controlplane_metadata::partition::*;

mod convert {

    use crate::{AdminSpec};
    use super::*;

    impl AdminSpec for PartitionSpec {}
}
