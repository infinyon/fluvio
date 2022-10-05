pub mod epoch;
pub mod core;
pub mod store;

// re-export k8-types crate
#[cfg(feature = "k8")]
pub use k8_types;

#[cfg(feature = "fixture")]
pub mod fixture {

    use crate::core::{Spec, Status, MetadataItem, MetadataContext, MetadataRevExtension};
    use crate::store::MetadataStoreObject;
    use crate::epoch::DualEpochMap;

    // define test spec and status
    #[derive(Debug, Default, Clone, Eq, PartialEq)]
    pub struct TestSpec {
        pub replica: u16,
    }

    impl Spec for TestSpec {
        const LABEL: &'static str = "Test";
        type IndexKey = String;
        type Owner = Self;
        type Status = TestStatus;
    }

    #[derive(Debug, Default, Clone, Eq, PartialEq)]
    pub struct TestStatus {
        pub up: bool,
    }

    impl Status for TestStatus {}

    pub type DefaultTest = MetadataStoreObject<TestSpec, TestMeta>;

    pub type TestEpochMap = DualEpochMap<String, DefaultTest>;

    #[derive(Debug, Default, Eq, PartialEq, Clone)]
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
            self.rev >= another.rev
        }
    }

    impl MetadataRevExtension for TestMeta {
        fn next_rev(&self) -> Self {
            Self {
                rev: self.rev + 1,
                comment: self.comment.clone(),
            }
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
