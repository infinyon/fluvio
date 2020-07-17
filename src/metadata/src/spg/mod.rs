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

    impl Creatable for SpuGroupSpec{}

    impl Status for SpuGroupStatus {}
}
