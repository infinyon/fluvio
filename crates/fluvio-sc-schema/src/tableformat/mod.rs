pub use fluvio_controlplane_metadata::tableformat::*;

mod convert {

    use crate::{DeletableAdminSpec, CreatableAdminSpec};

    use crate::AdminSpec;
    use super::TableFormatSpec;

    impl AdminSpec for TableFormatSpec {}

    impl CreatableAdminSpec for TableFormatSpec {}

    impl DeletableAdminSpec for TableFormatSpec {
        type DeleteKey = String;
    }
}
