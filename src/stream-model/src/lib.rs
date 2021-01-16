pub mod epoch;
pub mod core;
pub mod store;

#[cfg(feature = "k8")]
pub use k8_types;

#[cfg(test)]
pub(crate) mod test_fixture {

    use crate::core::{Spec, Status, MetadataItem, MetadataContext};
    use crate::store::MetadataStoreObject;
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

    pub type DefaultTest = MetadataStoreObject<TestSpec, TestMeta>;

    pub type TestEpochMap = DualEpochMap<String, DefaultTest>;

    #[derive(Debug, Default, PartialEq, Clone)]
    pub struct TestMeta {
        pub rev: u32,
        pub comment: String,
    }

    impl MetadataItem for TestMeta {
        type UId = u32;

        fn uid(&self) -> &Self::UId {
            &self.rev
        }

        fn is_newer(&self, another: &Self) -> bool {
            self.rev > another.rev
        }
    }

    impl TestMeta {
        pub fn new(rev: u32) -> Self {
            Self {
                rev,
                comment: "new".to_owned(),
            }
        }
    }

    impl From<u32> for MetadataContext<TestMeta> {
        fn from(val: u32) -> MetadataContext<TestMeta> {
            TestMeta::new(val).into()
        }
    }
}
