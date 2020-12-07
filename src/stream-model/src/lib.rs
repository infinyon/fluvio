pub mod epoch;
pub mod core;
pub mod store;

#[cfg(feature = "k8")]
pub mod k8 {
    pub use k8_obj_app as app;
    pub use k8_obj_app::core;
    pub use k8_obj_app::core::metadata;
}

#[cfg(test)]
pub(crate) mod test_fixture {

    use crate::core::{Spec, Status};
    use crate::store::{DefaultMetadataObject};
    use crate::epoch::DualEpochMap;

    // define test spec and status
    #[derive(Debug, Default, Clone, PartialEq)]
    pub struct TestSpec {
        pub replica: u16,
    }

    impl Spec for TestSpec {
        const LABEL: &'static str = "Test";
        type IndexKey = String;
        type Owner = Self;
        type Status = TestStatus;
    }

    #[derive(Debug, Default, Clone, PartialEq)]
    pub struct TestStatus {
        pub up: bool,
    }

    impl Status for TestStatus {}

    pub type DefaultTest = DefaultMetadataObject<TestSpec>;

    pub type TestEpochMap = DualEpochMap<String, DefaultTest>;
}
