pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{AdminSpec, CreatableAdminSpec, DeletableAdminSpec, UpdatableAdminSpec};
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {}

    impl CreatableAdminSpec for SpuGroupSpec {}

    impl DeletableAdminSpec for SpuGroupSpec {
        type DeleteKey = String;
    }

    impl UpdatableAdminSpec for SpuGroupSpec {
        type UpdateKey = String;
        type UpdateAction = String;
    }
}
