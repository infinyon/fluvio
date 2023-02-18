pub use fluvio_controlplane_metadata::spg::*;

mod convert {

    use crate::{
        AdminSpec, CreatableAdminSpec, DeletableAdminSpec,
    };
    use super::SpuGroupSpec;

    impl AdminSpec for SpuGroupSpec {}

    impl CreatableAdminSpec for SpuGroupSpec {
        const CREATE_TYPE: u8 = 2;
    }

    impl DeletableAdminSpec for SpuGroupSpec {
        type DeleteKey = String;
    }

}
