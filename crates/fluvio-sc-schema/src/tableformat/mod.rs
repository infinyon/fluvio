pub use fluvio_controlplane_metadata::tableformat::*;

mod convert {

    use crate::{CreatableAdminSpec, DeletableAdminSpec};

    use crate::{
        AdminSpec
    };
    use super::TableFormatSpec;

    impl AdminSpec for TableFormatSpec {}

    impl CreatableAdminSpec for TableFormatSpec {
        const CREATE_TYPE: u8 = 5;
    }

    impl DeletableAdminSpec for TableFormatSpec {
        type DeleteKey = String;
    }

}
