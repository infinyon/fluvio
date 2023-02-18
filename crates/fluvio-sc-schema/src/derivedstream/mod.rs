pub use fluvio_controlplane_metadata::derivedstream::*;

mod convert {

    use crate::AdminSpec;
    use crate::CreatableAdminSpec;
    use crate::DeletableAdminSpec;

    use super::DerivedStreamSpec;

    impl AdminSpec for DerivedStreamSpec {}

    impl CreatableAdminSpec for DerivedStreamSpec {
        const CREATE_TYPE: u8 = 10;
    }

    impl DeletableAdminSpec for DerivedStreamSpec {
        type DeleteKey = String;
    }

}
