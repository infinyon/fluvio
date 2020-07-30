mod spec;
mod status;
pub mod store;

pub use spec::*;
pub use status::*;

#[cfg(feature = "k8")]
mod k8;
#[cfg(feature = "k8")]
pub use k8::*;

mod convert {

    use crate::core::*;

    use super::*;

    impl Spec for SpuGroupSpec {
        const LABEL: &'static str = "SpuGroup";

        type Status = SpuGroupStatus;

        type Owner = Self;
        type IndexKey = String;
    }

    impl Removable for SpuGroupSpec {
        type DeleteKey = String;
    }

    impl Creatable for SpuGroupSpec {}

    impl Status for SpuGroupStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use super::SpuGroupSpec;
        use super::K8SpuGroupSpec;
        use crate::store::k8::K8ExtendedSpec;

        impl K8ExtendedSpec for SpuGroupSpec {
            type K8Spec = K8SpuGroupSpec;
            type K8Status = Self::Status;
        }
    }
}
