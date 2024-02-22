mod spec;
mod status;

pub use spec::*;
pub use status::*;

#[cfg(feature = "k8")]
mod k8;

mod convert {

    use crate::core::{Spec, Status, Removable, Creatable};
    use crate::extended::{ObjectType, SpecExt};
    use super::*;

    impl Spec for TableFormatSpec {
        const LABEL: &'static str = "TableFormat";

        type Status = TableFormatStatus;

        type Owner = Self;
        type IndexKey = String;
    }

    impl SpecExt for TableFormatSpec {
        const OBJECT_TYPE: ObjectType = ObjectType::TableFormat;
    }

    impl Removable for TableFormatSpec {
        type DeleteKey = String;
    }

    impl Creatable for TableFormatSpec {}

    impl Status for TableFormatStatus {}

    #[cfg(feature = "k8")]
    mod extended {

        use crate::store::k8::K8ExtendedSpec;
        use crate::store::k8::K8ConvertError;
        use crate::store::k8::K8MetaItem;
        use crate::store::MetadataStoreObject;
        use crate::k8_types::K8Obj;
        use crate::store::k8::default_convert_from_k8;

        use super::TableFormatSpec;

        impl K8ExtendedSpec for TableFormatSpec {
            type K8Spec = Self;

            fn convert_from_k8(
                k8_obj: K8Obj<Self::K8Spec>,
                multi_namespace_context: bool,
            ) -> Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
            {
                default_convert_from_k8(k8_obj, multi_namespace_context)
            }

            fn convert_status_from_k8(status: Self::Status) -> Self::Status {
                status
            }

            fn into_k8(self) -> Self::K8Spec {
                self
            }
        }
    }
}
