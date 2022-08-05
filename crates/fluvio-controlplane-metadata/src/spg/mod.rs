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

    use crate::core::{Spec, Status, Removable, Creatable};
    use crate::extended::{ObjectType, SpecExt};
    use super::*;

    impl Spec for SpuGroupSpec {
        const LABEL: &'static str = "SpuGroup";

        type Status = SpuGroupStatus;

        type Owner = Self;
        type IndexKey = String;
    }

    impl SpecExt for SpuGroupSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::SpuGroup;
    }

    impl Removable for SpuGroupSpec {
        type DeleteKey = String;
    }

    impl Creatable for SpuGroupSpec {}

    impl Status for SpuGroupStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::SpuGroupSpec;
        use super::K8SpuGroupSpec;

        impl K8ExtendedSpec for SpuGroupSpec {
            type K8Spec = K8SpuGroupSpec;
            type K8Status = Self::Status;

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
                multi_namespace_context: bool,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj, multi_namespace_context)
            }
        }
    }
}
