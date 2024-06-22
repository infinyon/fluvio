pub use fluvio_controlplane_metadata::tableformat::*;

mod convert {

    use crate::{CreatableAdminSpec, DeletableAdminSpec, UpdatableAdminSpec};

    use crate::AdminSpec;
    use super::TableFormatSpec;

    impl AdminSpec for TableFormatSpec {}

    impl CreatableAdminSpec for TableFormatSpec {}

    impl DeletableAdminSpec for TableFormatSpec {
        type DeleteKey = String;
    }

    impl UpdatableAdminSpec for TableFormatSpec {
        type UpdateKey = String;
        type UpdateAction = String;
    }
}
