pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{AdminSpec, DeletableAdminSpec, CreatableAdminSpec};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {}

    impl CreatableAdminSpec for SpuGroupSpec {}

    impl DeletableAdminSpec for SpuGroupSpec {
        type DeleteKey = String;
    }
}
